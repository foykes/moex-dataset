# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import datetime, pandas as pd, requests, csv, sys, time, os, json, math, re
import random, socket, errno, calendar, hashlib, uuid
from io import StringIO
from contextvars import ContextVar
from contextlib import contextmanager
from functools import wraps
from urllib3 import exceptions as http_errors
from urllib3._collections import HTTPHeaderDict


today = datetime.datetime.now()
df_full = pd.DataFrame()
exception_list = []
current_path = sys.path[0]


# %%
header = {'User-Agent': ''}
### Выгрузка header для запроса
json_path = current_path + '/settings/user_agents.json'
with open(json_path, 'r', encoding='utf-8') as f:
    headers_full = json.load(f)

header_first = str(headers_full['chrome'][0])
header['User-Agent'] = header_first

# %%
### Выгрузка конфига файлов
json_path = current_path + '/settings/datasets_config.json'

with open(json_path, 'r', encoding='utf-8') as f:
    config = json.load(f)

# %%
### Функция для получения следующего header
def get_next_header(current_header=None):
    global headers_full

    user_agents = []

    # Собираем все User-Agent из headers_full в один список
    # Структура headers_full:
    # {
    #     'chrome': [...],
    #     'edge': [...],
    #     'mozilla': [...],
    #     'opera': [...]
    # }
    for browser_name, agents_list in headers_full.items():
        for user_agent in agents_list:
            user_agents.append(user_agent)

    # Если по какой-то причине список пустой
    if len(user_agents) == 0:
        return current_header if current_header is not None else {}

    # Если текущий header не задан
    if current_header is None:
        current_header = {}

    current_user_agent = current_header.get('User-Agent')

    # Если текущего User-Agent нет в списке — начинаем с первого
    if current_user_agent not in user_agents:
        next_user_agent = user_agents[0]
    else:
        current_index = user_agents.index(current_user_agent)
        next_index = (current_index + 1) % len(user_agents)
        next_user_agent = user_agents[next_index]

    # Сохраняем остальные ключи header, если они есть,
    # и меняем только User-Agent
    new_header = current_header.copy()
    new_header['User-Agent'] = next_user_agent

    return new_header


# %%
def _prepare_ticker_catalogue(source):
    """Отдельная рабочая копия: исходный каталог и его экспорт не меняются."""
    if not isinstance(source, pd.DataFrame) or any(
            list(source.columns).count(name) != 1 for name in ('TRADE_CODE', 'SUPERTYPE')):
        raise ValueError('A1_TICKER_CATALOGUE_STRUCTURE')

    groups = {}
    for position, value in enumerate(source['TRADE_CODE']):
        if isinstance(value, str):
            key = value.strip()
            if key:
                groups.setdefault(key, []).append(position)
        elif pd.api.types.is_scalar(value) and pd.isna(value):
            continue
        else:
            # Не превращаем число/контейнер в новый строковый идентификатор.
            raise ValueError('A1_TICKER_CATALOGUE_STRUCTURE')

    positions = []
    for key, group in groups.items():
        raw_codes = {source.iloc[position]['TRADE_CODE'] for position in group}
        if len(raw_codes) > 1:
            # Проверяем всю новую strip-группу до выбора первой строки. Старые
            # exact-code дубли и произвольные различия NAME/CURRENCY не ремонтируем.
            routes = [source.iloc[position]['SUPERTYPE'] for position in group]
            if any(not isinstance(route, str) or not route.strip() for route in routes) or any(
                    route != routes[0] for route in routes[1:]):
                raise ValueError('A1_AMBIGUOUS_TICKER_ROUTING')
            for name in ('ISIN', 'INSTRUMENT_ID'):
                if list(source.columns).count(name) != 1:
                    continue
                hints = []
                for position in group:
                    value = source.iloc[position][name]
                    if not pd.api.types.is_scalar(value) or pd.isna(value):
                        continue
                    if isinstance(value, str) and not value.strip():
                        continue
                    hints.append(value)
                if hints and any(value != hints[0] for value in hints[1:]):
                    raise ValueError('A1_AMBIGUOUS_TICKER_IDENTITY')
        positions.append(group[0])

    if not positions:
        raise ValueError('A1_EMPTY_TICKER_CATALOGUE')
    processing = source.iloc[positions].copy(deep=True)
    processing['TRADE_CODE'] = list(groups)
    processing.reset_index(drop=True, inplace=True)
    return processing


def _a1_counts(received, accepted, instruments):
    return dict(rows_received=received, rows_accepted=accepted,
                rows_quarantined=None, pages=None, instruments=instruments,
                null_reasons={'rows_quarantined': 'NOT_ASSESSED', 'pages': 'NOT_MEASURED'})


def _a1_event(logging_context, level, event, fields):
    if logging_context is None:
        return
    try:
        import run_logging
        receipt = run_logging.emit_event(logging_context, level, event,
            'Сбор данных A1: ' + event, dict(stage='data_gathering', **fields))
        healthy = run_logging.check_logging_health(logging_context)['healthy']
        if not receipt['accepted'] or not healthy or level == 'ERROR' and not receipt['confirmed']:
            raise RuntimeError('LOGGING_INCOMPLETE')
    except BaseException:
        # Не цепляем вторичную ошибку с потенциальными секретами к новому сигналу.
        raise RuntimeError('LOGGING_INCOMPLETE') from None


def _a1_failure(logging_context, error, fields):
    if logging_context is None:
        return
    try:
        notes = getattr(error, '__notes__', [])
        if type(notes) is list and 'A1_LOGGING_FAILURE: LOGGING_INCOMPLETE' in notes:
            return
    except BaseException:
        pass
    try:
        import run_logging
        codes = {'A1_TICKER_CATALOGUE_STRUCTURE', 'A1_EMPTY_TICKER_CATALOGUE',
                 'A1_AMBIGUOUS_TICKER_ROUTING', 'A1_AMBIGUOUS_TICKER_IDENTITY',
                 'A1_STORED_TICKER_INCOMPATIBLE'}
        code = error.args[0] if (type(error) is ValueError and len(error.args) == 1 and isinstance(error.args[0], str)
                                 and error.args[0] in codes) else 'APPLICATION_ERROR'
        record = run_logging.make_error(logging_context, error, 'data_gathering', code=code)
        _a1_event(logging_context, 'ERROR', 'a1_failed',
                  dict(fields, outcome='FAILED', error=record))
    except BaseException:
        # Два независимых bounded сигнала; ни один fallback не заменяет primary.
        try:
            print('A1_LOGGING_FAILURE: LOGGING_INCOMPLETE', file=sys.stderr)
        except BaseException:
            pass
        try:
            BaseException.add_note(error, 'A1_LOGGING_FAILURE: LOGGING_INCOMPLETE')
        except BaseException:
            pass


def _check_stored_ticker_identity(dataset, keys):
    for value in dataset['ticker']:
        if isinstance(value, str) and value != value.strip() and value.strip() in keys:
            # Проверка всего read dataset до удаления RSI/30-day filter. Это отказ,
            # а не скрытая миграция старых свечей или historical mapping.
            raise ValueError('A1_STORED_TICKER_INCOMPATIBLE')


### Выгрузка датасета доступного на мосбирже
def moex_tickerlists (current_path, *, logging_context=None):
    operation_started = time.perf_counter()
    active_fields = dict(file='ticker_lists/moex_full.csv')
    _a1_event(logging_context, 'INFO', 'a1_moex_tickerlists_started', active_fields)
    try:
        CSV_URL = 'https://www.moex.com/ru/listing/securities-list-csv.aspx?type=1'
        global header

        with requests.Session() as s:
            download = s.get(CSV_URL, headers = header)

            decoded_content = download.content.decode('cp1251')

            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)


        if not my_list:
            raise ValueError('A1_EMPTY_TICKER_CATALOGUE')
        df_moex = pd.DataFrame(my_list)
        new_header = df_moex.iloc[0]
        df_moex = df_moex[1:]
        df_moex.columns = new_header

        # Guard до первого writer, но raw source rows/values/order/index сохраняются.
        all_stocks_ru = _prepare_ticker_catalogue(df_moex)
        _a1_event(logging_context, 'INFO', 'a1_catalogue_prepared',
            dict(file='ticker_lists/moex_full.csv',
                 counts=_a1_counts(len(df_moex), len(all_stocks_ru), len(all_stocks_ru))))

        print("Общее количество объектов на Мосбирже: {}".format(len(df_moex)))
        active_fields = dict(file='ticker_lists/moex_full.xlsx')
        df_moex.to_excel(("{}/datasets/ticker_lists/moex_full.xlsx").format(current_path))
        active_fields = dict(file='ticker_lists/moex_full.csv')
        df_moex.to_csv(("{}/datasets/ticker_lists/moex_full.csv").format(current_path))

        df_moex_stocks = df_moex[(df_moex['SUPERTYPE'] == "Акции")|(df_moex['SUPERTYPE'] == "Депозитарные расписки")]
        df_moex_stocks.reset_index(drop=True, inplace=True)

        ## moex_stocks_list['CURRENCY'] == '' это заблокированные акции

        print("Количество акций и депозитарных расписок: {}".format(len(df_moex_stocks)))
        # df_moex_stocks.to_excel(("{}/datasets/ticker_lists/moex_stocks.xlsx").format(current_path))
        active_fields = dict(file='ticker_lists/moex_stocks.csv')
        df_moex_stocks.to_csv(("{}/datasets/ticker_lists/moex_stocks.csv").format(current_path))

        _a1_event(logging_context, 'INFO', 'a1_catalogue_returned',
            dict(file='ticker_lists/moex_full.csv', outcome='RETURNED',
                 duration_ms=max(0, (time.perf_counter() - operation_started) * 1000),
                 counts=_a1_counts(len(df_moex), len(all_stocks_ru), len(all_stocks_ru))))
        return all_stocks_ru
    except BaseException as error:
        _a1_failure(logging_context, error,
            dict(active_fields, duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
        raise

# %%
### Функция запроса к API по тикеру, датам и нужному интервалу
def _a2_number(value):
    # Только предел безопасного отображения, отдельно от runtime policy.
    if type(value) not in (int, float) or not 0 <= value <= 1000000000:
        return 'UNKNOWN'
    if type(value) is float and not math.isfinite(value):
        return 'UNKNOWN'
    return str(value) if type(value) is int else format(value, '.6g')


def _a2_date(value):
    if type(value) is not str or len(value) != 10:
        return 'UNKNOWN'
    try:
        date = datetime.datetime.strptime(value, '%Y-%m-%d')
        return f'{date.year:04d}-{date.month:02d}-{date.day:02d}'
    except ValueError:
        return 'UNKNOWN'


def _a2_diagnostic(logging_context, level, event, message, fields, primary=None):
    try:
        if logging_context is None:
            # Без context оставляем bounded fallback, без DEBUG flooding.
            if level != 'DEBUG':
                print(message)
        else:
            # Штатный console writer и его уровни принадлежат F-LOG.
            import run_logging
            receipt = run_logging.emit_event(logging_context, level, event, message, fields)
            healthy = run_logging.check_logging_health(logging_context)['healthy']
            if not receipt['accepted'] or not healthy or level == 'ERROR' and not receipt['confirmed']:
                raise RuntimeError('LOGGING_INCOMPLETE')
    except BaseException:
        if primary is None:
            raise RuntimeError('LOGGING_INCOMPLETE') from None
        # Secondary никогда не заменяет primary и не запускает новый HTTP retry.
        try:
            print('A2_LOGGING_FAILURE: LOGGING_INCOMPLETE', file=sys.stderr)
        except BaseException:
            pass
        try:
            BaseException.add_note(primary, 'A2_LOGGING_FAILURE: LOGGING_INCOMPLETE')
        except BaseException:
            pass
        raise primary from None


def _a2_header_values(response, name):
    # Только исходные occurrences pinned urllib3; flattened headers не доказательство.
    try:
        raw_headers = getattr(getattr(response, 'raw', None), 'headers', None)
        if not isinstance(raw_headers, HTTPHeaderDict):
            return None
        values = raw_headers.getlist(name)
        return values if all(type(value) is str for value in values) else None
    except MemoryError:
        raise
    except Exception:
        return None


def _a2_retry_after(response, wall_time):
    values = _a2_header_values(response, 'Retry-After')
    if values is None or len(values) > 1:
        return None, 'RETRY_AFTER_AMBIGUOUS'
    if not values:
        return 0, 'RETRY_AFTER_ABSENT'
    value = values[0]
    if len(value) > 128:
        return None, 'RETRY_AFTER_OVERLONG'
    value = value.strip(' \t')
    if re.fullmatch(r'[0-9]+', value, re.ASCII):
        significant = value.lstrip('0') or '0'
        if len(significant) > 2 or int(significant) > 30:
            return None, 'RETRY_AFTER_TOO_LONG'
        return int(significant), 'RETRY_AFTER_VALID'
    # Сначала полная grammar даты: её одна запятая не является списком.
    formats = (
        (r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun), [0-9]{2} (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) [0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT', '%a, %d %b %Y %H:%M:%S GMT'),
        (r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), [0-9]{2}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} GMT', '%A, %d-%b-%y %H:%M:%S GMT'),
        (r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ( [1-9]|[0-9]{2}) [0-9]{2}:[0-9]{2}:[0-9]{2} [0-9]{4}', '%a %b %d %H:%M:%S %Y'),
    )
    for pattern, date_format in formats:
        if re.fullmatch(pattern, value, re.ASCII) is None:
            continue
        try:
            # Wire tokens английские независимо от LC_TIME вызывающего процесса.
            if '%y' in date_format:
                weekday, date_part, clock, _ = value.split(' ')
                day, month, year = date_part.split('-')
            elif '%a,' in date_format:
                weekday, day, month, year, clock, _ = value.split(' ')
            else:
                weekday, month, day, clock, year = value.split()
            month = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec').index(month) + 1
            hour, minute, second = (int(part) for part in clock.split(':'))
            year, day = int(year), int(day)
            if '%y' in date_format:
                now = time.gmtime(wall_time)
                year = now.tm_year // 100 * 100 + year
                candidate = (year, month, day, hour, minute, second)
                if candidate > (now.tm_year + 50, *now[1:6]):
                    year -= 100
            parsed = datetime.datetime(year, month, day, hour, minute, second)
            if weekday[:3] != ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')[parsed.weekday()]:
                raise ValueError('A2_HTTP_DATE_INVALID')
            delay = max(0, calendar.timegm(parsed.timetuple()) - wall_time)
        except (ValueError, OverflowError):
            return (None, 'RETRY_AFTER_AMBIGUOUS') if ',' in value else (0, 'RETRY_AFTER_INVALID')
        if delay > 30:
            return None, 'RETRY_AFTER_TOO_LONG'
        return delay, 'RETRY_AFTER_VALID'
    if ',' in value:
        return None, 'RETRY_AFTER_AMBIGUOUS'
    return 0, 'RETRY_AFTER_INVALID'


def _a2_request_policy(primary):
    # Ограниченный обход только typed transport wrappers; текст не классифицируем.
    pending, seen, errors = [(primary, 0)], set(), []
    incomplete = False
    while pending and len(seen) < 32:
        error, depth = pending.pop()
        if id(error) in seen or not isinstance(error, BaseException):
            continue
        if depth > 8:
            incomplete = True
            continue
        seen.add(id(error))
        errors.append(error)
        if isinstance(error, (requests.exceptions.RequestException, http_errors.HTTPError)):
            for item in (getattr(error, 'reason', None), error.__cause__):
                if isinstance(item, BaseException):
                    if len(pending) + len(seen) < 32:
                        pending.append((item, depth + 1))
                    else:
                        incomplete = True
            available = max(0, 32 - len(pending) - len(seen))
            if len(error.args) > available:
                incomplete = True
            for item in error.args[:available]:
                if isinstance(item, BaseException):
                    pending.append((item, depth + 1))
    if any(isinstance(error, (requests.exceptions.SSLError, http_errors.SSLError)) for error in errors):
        return 'A2_ISS_TLS_FAILED', 'SOURCE', False, 'TLS_FAILED'
    if isinstance(primary, requests.exceptions.TooManyRedirects):
        return 'A2_ISS_REDIRECT_REJECTED', 'SOURCE', False, 'REDIRECT_REJECTED'
    if isinstance(primary, MemoryError):
        return 'A2_ISS_MEMORY_ERROR', 'MEMORY_ERROR', False, 'REQUEST_FAILED'
    if any(isinstance(error, (requests.exceptions.ProxyError, requests.exceptions.InvalidURL,
            requests.exceptions.InvalidSchema, requests.exceptions.MissingSchema,
            requests.exceptions.URLRequired, requests.exceptions.InvalidHeader,
            http_errors.ProxyError, http_errors.LocationValueError)) for error in errors):
        return 'A2_ISS_REQUEST_ERROR', 'CONFIG', False, 'REQUEST_FAILED'
    if incomplete:
        return 'A2_ISS_REQUEST_ERROR', 'SOURCE', False, 'REQUEST_FAILED'
    if any(isinstance(error, requests.exceptions.Timeout) or
           isinstance(error, (http_errors.ConnectTimeoutError, http_errors.ReadTimeoutError))
           and not isinstance(error, http_errors.NewConnectionError) for error in errors):
        return 'A2_ISS_TIMEOUT', 'TIMEOUT', True, None
    for error in errors:
        if (isinstance(error, (ConnectionResetError, ConnectionAbortedError)) or
                isinstance(error, OSError) and error.errno in (errno.ECONNRESET, errno.ECONNABORTED) or
                isinstance(error, socket.gaierror) and error.errno == socket.EAI_AGAIN):
            return 'A2_ISS_REQUEST_ERROR', 'SOURCE', True, None
    category = 'SOURCE' if isinstance(primary, (requests.exceptions.RequestException,
                                               http_errors.HTTPError, OSError)) else 'UNKNOWN'
    return 'A2_ISS_REQUEST_ERROR', category, False, 'REQUEST_FAILED'


_A3_MAX_PAGES = 1000
_A3_MAX_BYTES = 64 * 1024 * 1024
_A3_EVIDENCE_BYTES = 16 * 1024 * 1024
_A3_METADATA_BYTES = 16 * 1024


def _a2_candle_body(response, checkpoint, resource=None):
    values = _a2_header_values(response, 'Content-Encoding')
    if (values is None or len(values) > 1 or values and
            (not values[0].isascii() or values[0].strip(' \t').lower() != 'identity')):
        raise ValueError('A2_ISS_CONTENT_ENCODING_UNSUPPORTED')
    content = bytearray()
    while True:
        checkpoint()
        amount = min(65536, 8 * 1024 * 1024 + 1 - len(content))
        if resource is not None:
            amount = min(amount, _A3_MAX_BYTES + 1 - resource['bytes'])
        part = response.raw.read(amount,
                                 decode_content=False)
        if type(part) is not bytes:
            raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
        if resource is not None:
            resource['bytes'] += len(part)
            if resource['bytes'] > _A3_MAX_BYTES:
                raise ValueError('A3_ISS_QUERY_BYTES_EXCEEDED')
        if len(content) + len(part) > 8 * 1024 * 1024:
            raise ValueError('A2_ISS_BODY_LIMIT_EXCEEDED')
        checkpoint()
        if not part:
            return bytes(content)
        _a3_charge('bytes', len(part))
        content.extend(part)


def _a2_candle_codec(response, content):
    # Закрытый consumer profile, а не утверждение обо всех вариантах ISS.
    content_type = response.headers.get('Content-Type')
    if type(content_type) is not str:
        raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
    parts = [part.strip(' \t') for part in content_type.split(';')]
    if parts[0].lower() != 'text/csv' or len(parts) > 2:
        raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
    codec = 'cp1251'
    if len(parts) == 2:
        parameter = re.fullmatch(r'charset[ \t]*=[ \t]*(?:"([A-Za-z0-9-]+)"|([A-Za-z0-9-]+))',
                                 parts[1], re.IGNORECASE | re.ASCII)
        if parameter is None:
            raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
        charset = (parameter.group(1) or parameter.group(2)).lower()
        codec = {'cp1251': 'cp1251', 'windows-1251': 'cp1251',
                 'utf8': 'utf-8', 'utf-8': 'utf-8'}.get(charset)
        if codec is None:
            raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
    if type(content) is not bytes:
        raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
    # BOM проверяем до decode: cp1251 иначе скроет его как обычные символы.
    bom = b'\xef\xbb\xbf'
    if bom in content and (codec != 'utf-8' or not content.startswith(bom)
                           or bom in content[len(bom):]):
        raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
    return codec


def _a2_candle_records(text, duplicate=None):
    # Только временное представление; возвращаемый pandas frame не нормализуем.
    if '\r' in text.replace('\r\n', ''):
        raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
    validation_text = text[1:] if text.startswith('\ufeff') else text
    records = [row for row in csv.reader(StringIO(validation_text, newline=''),
               delimiter=';', quotechar='"', doublequote=True, strict=True) if row]
    columns = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end']
    if len(records) < 2 or records[0] != ['candles'] or records[1] != columns:
        raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
    rows = records[2:]
    seen_begin = {}
    number = r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?'
    timestamp = r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
    for ordinal, row in enumerate(rows, 1):
        if len(row) != 8:
            raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
        for cell in row[:6]:
            value = cell.strip(' \t')
            if re.fullmatch(number, value) is None or not math.isfinite(float(value)):
                raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
        for cell in row[6:]:
            if re.fullmatch(timestamp, cell) is None:
                raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
            try:
                parsed = datetime.datetime.strptime(cell, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID') from None
            reconstructed = (f'{parsed.year:04d}-{parsed.month:02d}-{parsed.day:02d} '
                             f'{parsed.hour:02d}:{parsed.minute:02d}:{parsed.second:02d}')
            if reconstructed != cell:
                raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
        # Локальная request-bound проекция; полную source identity она не доказывает.
        if row[6] in seen_begin:
            primary = ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
            if duplicate is not None:
                # Diagnostic-only hook: existing parser refusal remains primary.
                try:
                    duplicate(primary, seen_begin[row[6]], ordinal, rows)
                except BaseException:
                    _a3_evidence_note(primary)
            raise primary
        seen_begin[row[6]] = ordinal
    return rows


def _a2_candle_frame(df, rows):
    columns = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end']
    if (df.columns.tolist() != columns or len(df) != len(rows)
            or not df.index.equals(pd.RangeIndex(len(rows)))):
        raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
    for position, column in enumerate(columns[6:], start=6):
        if df[column].tolist() != [row[position] for row in rows]:
            raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')
    # Empty CSV имеет object dtype; прежний zero-column return сохраняется ниже.
    if rows:
        for column in columns[:6]:
            if (not pd.api.types.is_numeric_dtype(df[column])
                    or not all(math.isfinite(value) for value in df[column])):
                raise ValueError('A2_ISS_RESPONSE_SCHEMA_INVALID')


def _a3_capture(content, page, offset, attempt):
    # До decode/parser; ошибка capture сама становится terminal primary.
    return dict(body=content, body_bytes=len(content), body_sha256=hashlib.sha256(content).hexdigest(),
                page_ordinal=page, start=offset, attempt=attempt, codec=None,
                admission_state='RAW_CAPTURED')


def _a3_evidence_note(primary):
    try:
        BaseException.add_note(primary, 'A3_EVIDENCE_FAILED_UNVERIFIED')
    except BaseException:
        pass


def _a3_metadata(value):
    if type(value) is str:
        if (len(value) <= 128 and not any(0xd800 <= ord(char) <= 0xdfff for char in value)
                and len(value.encode('utf-8')) <= 128):
            return value
        digest, size = hashlib.sha256(), 0
        for offset in range(0, len(value), 4096):
            part = value[offset:offset + 4096].encode('utf-8', errors='surrogatepass')
            size += len(part)
            digest.update(part)
        return dict(value=None, state='HASH_ONLY_OVERSIZE_OR_ENCODING', bytes=size,
                    sha256=digest.hexdigest(), encoding='UTF8_SURROGATEPASS')
    if value is None or type(value) in (int, bool):
        if value is None or type(value) is bool or value.bit_length() <= 64:
            return value
    return dict(value=None, state='UNSUPPORTED_METADATA_TYPE')


def _a3_evidence(primary, reason, request, pairs=(), logging_context=None):
    # Только bounded local exception attribute. Production writer отсутствует.
    if hasattr(primary, 'a3_evidence'):
        return
    try:
        from datetime import datetime as utc_datetime, timezone
        cells, provenance = [], []
        size = 0
        for capture, ordinal, row in pairs[:2]:
            if len(row) != 8 or any(type(cell) is not str for cell in row):
                raise ValueError('A3_EVIDENCE_CELLS_INVALID')
            size += sum(len(cell.encode('utf-8')) for cell in row)
            if size > _A3_EVIDENCE_BYTES:
                raise ValueError('A3_EVIDENCE_CELLS_LIMIT')
            cells.append(tuple(row))
            provenance.append({key: capture[key] for key in (
                'page_ordinal', 'start', 'attempt', 'body_bytes', 'body_sha256',
                'codec', 'admission_state')})
            provenance[-1]['data_record_ordinal'] = ordinal
        context = logging_context if type(logging_context) is dict else {}
        metadata = dict(format='RAW_CSV_CELLS_V1', reason=reason, state='CAPTURED',
            metadata_exact=True, omitted_fields=[], request={key: _a3_metadata(value)
                for key, value in request.items()}, engine='stock', endpoint='ISS_CANDLES',
            identity='ISS_CANDLES_BOARDLESS/stock/market/security/interval/raw_begin',
            board=None, board_reason='NOT_PRESENT_IN_REQUEST_A5_UNVERIFIED',
            source_identity='NOT_VERIFIED', availability='NOT_VERIFIED',
            columns=['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end'],
            row_cap=2, cell_bytes_cap=_A3_EVIDENCE_BYTES, cell_bytes=size,
            metadata_bytes_cap=_A3_METADATA_BYTES, rows=provenance,
            captured_utc=utc_datetime.now(timezone.utc).isoformat(),
            correlation={key: _a3_metadata(context.get(key))
                         for key in ('run_id', 'snapshot_id', 'dataset_id')},
            verified_sha=None, sha_reason='NOT_VERIFIED', persistence='NOT_REQUESTED')
        metadata['omitted_fields'] = [key for key, value in metadata['request'].items()
                                      if type(value) is dict]
        metadata['omitted_fields'] += ['correlation.' + key for key, value in metadata['correlation'].items()
                                        if type(value) is dict]
        metadata['metadata_exact'] = not metadata['omitted_fields']
        if len(json.dumps(metadata, ensure_ascii=False, sort_keys=True,
                          separators=(',', ':')).encode('utf-8')) > _A3_METADATA_BYTES:
            raise ValueError('A3_EVIDENCE_METADATA_LIMIT')
        primary.a3_evidence = dict(metadata=metadata, raw_cells=tuple(cells))
    except BaseException:
        _a3_evidence_note(primary)


def _a3_numeric_identity(value):
    if pd.api.types.is_integer(value) and not isinstance(value, bool):
        return int(value), 1, False
    if pd.api.types.is_float(value) and math.isfinite(value):
        native = float(value)
        return (*native.as_integer_ratio(), native == 0 and math.copysign(1, native) < 0)
    raise ValueError('A3_ISS_NUMERIC_ASSEMBLY_LOSS')


def _a3_assemble(frames, ticker):
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    _a3_numeric_union(frames, result)
    result['ticker'] = ticker
    return result


def _a3_numeric_union(frames, result):
    # Общий audit без переписывания ticker/index у dataset concat.
    for column in ('open', 'close', 'high', 'low', 'value', 'volume'):
        dtype = result[column].dtype
        if not pd.api.types.is_numeric_dtype(dtype):
            raise ValueError('A3_ISS_NUMERIC_ASSEMBLY_LOSS')
        source_dtypes = [frame[column].dtype for frame in frames if column in frame]
        if all(item == source_dtypes[0] for item in source_dtypes) and dtype != source_dtypes[0]:
            raise ValueError('A3_ISS_NUMERIC_ASSEMBLY_LOSS')
        offset = 0
        for frame in frames:
            if not len(frame):
                continue
            for before, after in zip(frame[column], result[column].iloc[offset:offset + len(frame)]):
                if _a3_numeric_identity(before) != _a3_numeric_identity(after):
                    raise ValueError('A3_ISS_NUMERIC_ASSEMBLY_LOSS')
            offset += len(frame)


def moex_query(ticker_in, ticker_type, end_date_mx, start_date_mx, interval, max_retries=5, retry_sleep_start=3, *, logging_context=None, retry_jitter=0):
    global header
    global headers_full
    # Определение типа market для корректного запроса
    if ticker_type in ['Инвестиционные паи', 'Депозитарные расписки', 'Акции', 'Ипотечные сертификаты участия']:
        market = "shares"
    elif ticker_type in ['Облигации', 'Еврооблигации']:
        market = "bonds"
    else:
        market = "shares"

    operation_started = time.perf_counter()
    attempt = 0
    status = body_bytes = None

    page_ordinal, offset = 1, 0
    total_attempts = retry_total = rows_received_total = pages_validated = accumulated = 0
    page_received, added = None, 0
    failed_emitted = False
    diagnostic_failed = False
    success_sleep_interrupted = False
    resource = {'bytes': 0}
    frames, captures, seen, payloads = [], [], {}, set()
    capture = None
    last_raw = None
    retry_after_note = None
    deadline_error = None

    def message(outcome, code='NO_ERROR', rotated=False):
        # Only fixed aliases/counters; raw bounds, body and exceptions stay local.
        return (f'Свечи {outcome}: {code}; p={page_ordinal}; start={offset}; '
                f'got={page_received}; add={added}; sum={accumulated}; '
                f'get={total_attempts}; retry={retry_total}; term={outcome}'
                + ('; HEADER_ROTATED' if rotated else '')
                + ('; ' + retry_after_note if retry_after_note is not None else ''))

    def correlation_fields(aggregate=False):
        fields = {}
        if type(logging_context) is dict:
            for key in ('snapshot_id', 'dataset_id', 'file', 'artifact_id', 'target_id'):
                value = logging_context.get(key)
                if type(value) is str:
                    fields[key] = value
            page = logging_context.get('page')
            if type(page) is str or type(page) is int and page >= 0:
                fields['page'] = page
        if 'page' not in fields:
            fields['page'] = None if aggregate else offset
        return fields

    def error_record(primary, code, category, final, aggregate):
        nullable = ('run_id', 'snapshot_id', 'dataset_id', 'instrument', 'interval',
                    'page', 'file', 'artifact_id', 'target_id', 'endpoint', 'attempt', 'exception_type')
        record = dict(category=category, code=code, stage='data_gathering',
                      retryable=not final, final=final, message='Отказ запроса свечей: ' + code)
        record.update({key: None for key in nullable})
        record.update(correlation_fields(aggregate))
        if type(logging_context) is dict and type(logging_context.get('run_id')) is str:
            record['run_id'] = logging_context['run_id']
        exception_type = 'Exception'
        for native in (requests.exceptions.SSLError, requests.exceptions.TooManyRedirects,
                       requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout,
                       requests.exceptions.Timeout, requests.exceptions.HTTPError,
                       requests.exceptions.ConnectionError, requests.exceptions.RequestException,
                       pd.errors.EmptyDataError, pd.errors.ParserError,
                       http_errors.ReadTimeoutError, http_errors.ConnectTimeoutError,
                       http_errors.ProtocolError, ConnectionResetError, ConnectionAbortedError,
                       socket.gaierror, MemoryError, OSError, UnicodeDecodeError, csv.Error, ValueError):
            if isinstance(primary, native):
                exception_type = native.__name__
                break
        record.update(instrument=ticker_in if type(ticker_in) is str else None,
                      interval=interval if type(interval) is int and interval > 0 else None,
                      endpoint='ISS_CANDLES', attempt=attempt, exception_type=exception_type)
        record['null_reasons'] = {key: ('AGGREGATE_QUERY' if key == 'page' and aggregate else 'NOT_AVAILABLE_OR_NOT_APPLICABLE')
                                  for key in nullable if record[key] is None}
        return record

    def emit(level, event, outcome, primary=None, code='NO_ERROR', category='SOURCE', final=False,
             received=None, invalid=False, rotated=False, pending_primary=None):
        nonlocal failed_emitted, diagnostic_failed
        aggregate = event in ('a2_query_started', 'a2_query_returned', 'a2_query_failed')
        if event == 'a2_query_failed':
            failed_emitted = True
        fields = dict(stage='data_gathering', fields={}, outcome=outcome,
                      instrument=ticker_in if type(ticker_in) is str else None,
                      interval=interval if type(interval) is int and interval > 0 else None,
                      duration_ms=max(0, (time.perf_counter() - operation_started) * 1000),
                      retries=None if invalid else dict(
                          attempt=total_attempts if aggregate else attempt,
                          limit=_A3_MAX_PAGES * max_retries if aggregate else max_retries),
                      error=None if primary is None else error_record(primary, code, category, final, aggregate))
        fields.update(correlation_fields(aggregate))
        if invalid:
            fields['null_reasons'] = {'retries': 'INVALID_CONFIGURATION'}
        if fields.get('page') is None:
            fields.setdefault('null_reasons', {})['page'] = 'AGGREGATE_QUERY'
        fields['counts'] = dict(rows_received=rows_received_total if aggregate else page_received,
            rows_accepted=None, rows_quarantined=None,
            pages=pages_validated if aggregate else int(received is not None), instruments=None,
            null_reasons={'rows_accepted': 'NOT_ASSESSED', 'rows_quarantined': 'NOT_ASSESSED',
                          'instruments': 'NOT_ASSESSED'})
        if not aggregate and page_received is None:
            fields['counts']['null_reasons']['rows_received'] = 'NOT_YET_VALIDATED'
        try:
            _a2_diagnostic(logging_context, level, event, message(outcome, code, rotated), fields,
                           primary if primary is not None else pending_primary)
        except BaseException:
            diagnostic_failed = True
            raise

    if (type(max_retries) is not int or not 1 <= max_retries <= 5 or
            type(retry_sleep_start) not in (int, float) or not 0 <= retry_sleep_start <= 30 or
            type(retry_jitter) not in (int, float) or not 0 <= retry_jitter <= 1):
        primary = ValueError('A2_RETRY_ARGUMENTS')
        emit('ERROR', 'a2_query_failed', 'CONFIG_REJECTED', primary,
             'A2_RETRY_ARGUMENTS', 'CONFIG', final=True, invalid=True)
        raise primary

    def remaining():
        return 180 - max(0, time.perf_counter() - operation_started)

    def checkpoint():
        nonlocal deadline_error
        if remaining() <= 0:
            deadline_error = requests.exceptions.Timeout('A2_ISS_DEADLINE_EXCEEDED')
            raise deadline_error
        _a3_checkpoint()

    def deadline(primary=None, code='A2_ISS_DEADLINE_EXCEEDED', category='TIMEOUT', available=None):
        if (remaining() if available is None else available) <= 0:
            if primary is None:
                primary = requests.exceptions.Timeout('A2_ISS_DEADLINE_EXCEEDED')
            emit('ERROR', 'a2_query_failed', 'DEADLINE_EXCEEDED', primary, code, category, final=True)
            raise primary
        try:
            _a3_checkpoint(primary)
        except BaseException as error:
            if primary is not None:
                emit('ERROR', 'a2_query_failed', 'DEADLINE_EXCEEDED', primary, code, category, final=True)
            raise error

    deadline()
    emit('INFO', 'a2_query_started', 'QUERY_STARTED')
    deadline()
    request = dict(market=market, security=f'{ticker_in}', interval=f'{interval}',
                   from_date=f'{end_date_mx}', till_date=f'{start_date_mx}',
                   request_id=uuid.uuid4().hex)
    query = (f'https://iss.moex.com/iss/engines/stock/markets/{market}/securities/'
             f"{request['security']}/candles.csv?from={request['from_date']}&till="
             f"{request['till_date']}&interval={request['interval']}")

    # Если global header почему-то не задан или пустой —
    # инициализируем первым User-Agent из headers_full
    if not isinstance(header, dict) or len(header) == 0:
        header = get_next_header(None)

    def local_duplicate(primary, first, current, rows):
        _a3_evidence(primary, 'LOCAL_DUPLICATE', request,
                     ((capture, first, rows[first - 1]), (capture, current, rows[current - 1])),
                     logging_context)

    def refuse_order(reason, pairs):
        primary = ValueError('A3_ISS_PAGE_ORDER_INVALID')
        try:
            _a3_evidence(primary, reason, request, pairs, logging_context)
        except BaseException:
            _a3_evidence_note(primary)
        raise primary

    def page():
        global header
        nonlocal attempt, status, body_bytes, retry_after_note, capture
        nonlocal total_attempts, retry_total, page_received, rows_received_total, pages_validated
        nonlocal success_sleep_interrupted
        primary = None
        while attempt < max_retries:
            deadline(primary, code if primary is not None else 'A2_ISS_DEADLINE_EXCEEDED',
                     category if primary is not None else 'TIMEOUT')
            status = body_bytes = None
            retry_after_note = None
            emit('DEBUG', 'a2_query_attempt_started', 'REQUEST_ADMISSION_STARTED', pending_primary=primary)
            deadline(primary, code if primary is not None else 'A2_ISS_DEADLINE_EXCEEDED',
                     category if primary is not None else 'TIMEOUT')
            request_headers = {key: value for key, value in header.items()
                               if type(key) is not str or key.lower() != 'accept-encoding'}
            request_headers['Accept-Encoding'] = 'identity'
            available = remaining()
            deadline(primary, code if primary is not None else 'A2_ISS_DEADLINE_EXCEEDED',
                     category if primary is not None else 'TIMEOUT', available=available)
            try:
                _a3_charge('gets', 1, primary)
                _a3_charge('retries', int(attempt > 0), primary)
            except BaseException:
                if primary is not None:
                    emit('ERROR', 'a2_query_failed', 'CAPACITY_EXCEEDED', primary, code, category, final=True)
                raise
            primary = None
            response = None
            retryable = False
            code, category, terminal = 'A2_ISS_REQUEST_ERROR', 'UNKNOWN', None
            phase = 'request'
            attempt += 1
            total_attempts += 1
            retry_total += int(attempt > 1)
            capture = None
            try:
                response = requests.get(query + f'&start={offset}', headers=request_headers,
                                        timeout=(min(5, available), min(30, available)),
                                        verify=True, allow_redirects=False, stream=True)
                status = response.status_code
                if 300 <= status <= 399:
                    raise requests.exceptions.TooManyRedirects('A2_ISS_REDIRECT_REJECTED', response=response)
                if status != 200:
                    raise requests.exceptions.HTTPError('A2_ISS_HTTP_STATUS', response=response)
                checkpoint()
                content = _a2_candle_body(response, checkpoint, resource)
                body_bytes = len(content)
                state = _A3_RANGE_STATE.get()
                if state is not None:
                    # До decode/CSV/pandas: wire expansion, Python cells, keys и copies.
                    job = state['job']
                    growth = resource['bytes'] * 256 + 16 * 1024**2
                    _a3_reserve(job.get('collector_bytes', 0) + growth,
                                job.get('metadata_bytes', 0), growth)
                phase = 'a3_evidence'
                capture = _a3_capture(content, page_ordinal, offset, attempt)
                phase = 'validation'
                code, category = 'A2_ISS_RESPONSE_SCHEMA_INVALID', 'SOURCE'
                codec = _a2_candle_codec(response, content)
                capture['codec'] = codec
                checkpoint()
                code = 'A2_ISS_RESPONSE_DECODE_ERROR'
                text = content.decode(codec, errors='strict')
                checkpoint()
                code = 'A2_ISS_RESPONSE_SCHEMA_INVALID'
                rows = _a2_candle_records(text, local_duplicate)
                checkpoint()
                _a3_charge('rows', len(rows))
                code = 'A2_ISS_PARSE_ERROR'
                df = pd.read_csv(StringIO(text), sep=';', header=1)
                checkpoint()
                code = 'A2_ISS_RESPONSE_SCHEMA_INVALID'
                _a2_candle_frame(df, rows)
                page_received = len(rows)
                rows_received_total += page_received
                pages_validated += 1
                _a3_charge('pages', 1)
                capture['admission_state'] = 'A2_VALIDATED'
                checkpoint()
            except BaseException as error:
                primary = error
                if error is deadline_error:
                    code, category, terminal = 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED'
                elif error.args == ('A3_ISS_RANGE_DEADLINE_EXCEEDED',):
                    code, category, terminal = error.args[0], 'TIMEOUT', 'DEADLINE_EXCEEDED'
                elif error.args == ('A3_ISS_RANGE_CAPACITY_EXCEEDED',):
                    code, category, terminal = error.args[0], 'CAPACITY', 'CAPACITY_EXCEEDED'
                elif phase == 'a3_evidence':
                    code, category, terminal = 'A3_ISS_RAW_CAPTURE_FAILED', 'IO', 'EVIDENCE_FAILED'
                elif type(error) is ValueError and error.args == ('A3_ISS_QUERY_BYTES_EXCEEDED',):
                    code, category, terminal = error.args[0], 'CAPACITY', 'QUERY_BYTES_EXCEEDED'
                elif isinstance(error, requests.exceptions.HTTPError):
                    code, category = 'A2_ISS_HTTP_STATUS', 'QUOTA_429' if status == 429 else 'SOURCE'
                    retryable = status in (408, 429, 500, 502, 503, 504)
                    terminal = None if retryable else 'HTTP_NONRETRYABLE'
                elif isinstance(error, MemoryError):
                    code, category, retryable, terminal = _a2_request_policy(error)
                elif phase == 'validation' and isinstance(error, (ValueError, UnicodeDecodeError, csv.Error,
                                                                 pd.errors.ParserError, pd.errors.EmptyDataError)):
                    terminal = 'RESPONSE_REJECTED'
                elif type(error) is ValueError and error.args == ('A2_ISS_BODY_LIMIT_EXCEEDED',):
                    code, category, terminal = 'A2_ISS_BODY_LIMIT_EXCEEDED', 'CAPACITY', 'BODY_LIMIT_EXCEEDED'
                elif type(error) is ValueError and error.args in (
                        ('A2_ISS_CONTENT_ENCODING_UNSUPPORTED',), ('A2_ISS_RESPONSE_SCHEMA_INVALID',)):
                    code, category, terminal = error.args[0], 'SOURCE', 'RESPONSE_REJECTED'
                else:
                    code, category, retryable, terminal = _a2_request_policy(error)
            # Закрываем и falsey HTTPError response. Cleanup не заменяет native primary.
            cleanup_failed = False
            responses = [response] if response is not None else []
            attached = getattr(primary, 'response', None)
            if attached is not None and all(attached is not item for item in responses):
                responses.append(attached)
            for closing_response in responses:
                try:
                    closing_response.close()
                except BaseException as error:
                    cleanup_failed = True
                    if primary is None:
                        primary, code, category = error, 'A2_ISS_CLOSE_ERROR', 'IO'
                    else:
                        try:
                            BaseException.add_note(primary, 'A2_ISS_SECONDARY_CLOSE_ERROR')
                        except BaseException:
                            pass
                    terminal, retryable = 'REQUEST_FAILED', False

            # Диагностика и health находятся вне request/parser retry-catching блоков.
            if primary is not None:
                if not cleanup_failed or code != 'A2_ISS_CLOSE_ERROR':
                    deadline(primary, code, category)
                if terminal is not None or not retryable or attempt == max_retries or cleanup_failed:
                    emit('ERROR', 'a2_query_failed', terminal or 'RETRY_EXHAUSTED', primary,
                         code, category, final=True)
                    raise primary
                server_wait = 0
                if isinstance(primary, requests.exceptions.HTTPError):
                    try:
                        server_wait, retry_after_note = _a2_retry_after(response, time.time())
                    except Exception:
                        # HTTP primary уже существует; metadata fault не начинает retry.
                        server_wait, retry_after_note = None, 'RETRY_AFTER_UNAVAILABLE'
                local_wait = retry_sleep_start * (2 ** (attempt - 1))
                if retry_jitter:
                    local_wait += random.uniform(0, retry_jitter)
                local_wait = min(30, local_wait)
                wait = max(local_wait, server_wait or 0)
                deadline(primary, code, category)
                if server_wait is None or wait > 30 or wait >= remaining():
                    emit('ERROR', 'a2_query_failed', 'RETRY_NOT_ADMITTED', primary, code, category, final=True)
                    raise primary
                header = get_next_header(header)
                emit('WARNING', 'a2_query_retry', 'RETRY_PENDING', primary,
                     code, category, rotated=True)
                deadline(primary, code, category)
                if wait >= remaining():
                    emit('ERROR', 'a2_query_failed', 'RETRY_NOT_ADMITTED', primary, code, category, final=True)
                    raise primary
                time.sleep(wait)
                deadline(primary, code, category)
                continue

            deadline()
            # Структура проверена целиком; identity/quality/coverage ещё не доказаны.
            deadline()
            if remaining() <= 3:
                primary = requests.exceptions.Timeout('A2_ISS_DEADLINE_EXCEEDED')
                emit('ERROR', 'a2_query_failed', 'DEADLINE_EXCEEDED', primary,
                     'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', final=True)
                raise primary
            try:
                time.sleep(3)
            except BaseException:
                success_sleep_interrupted = True
                raise
            deadline()
            return df, rows, capture

    try:
        while True:
            attempt = added = 0
            page_received = None
            emit('DEBUG', 'a3_page_started', 'PAGE_STARTED')
            df, rows, capture = page()
            if not rows:
                emit('INFO', 'a3_page_terminal', 'OFFSET_TERMINAL_EMPTY', received=0)
                break
            digest = capture['body_sha256']
            for ordinal, row in enumerate(rows, 1):
                key = ('ISS_CANDLES_BOARDLESS', 'stock', request['market'], request['security'],
                       request['interval'], row[6])
                if key in seen:
                    previous_capture, previous_ordinal = seen[key]
                    primary = ValueError('A3_ISS_PAGE_OVERLAP')
                    try:
                        previous_rows = _a2_candle_records(previous_capture['body'].decode(
                            previous_capture['codec'], errors='strict'))
                        _a3_evidence(primary, 'CROSS_PAGE_OVERLAP', request,
                            ((previous_capture, previous_ordinal, previous_rows[previous_ordinal - 1]),
                             (capture, ordinal, row)), logging_context)
                    except BaseException:
                        _a3_evidence_note(primary)
                    raise primary
            if digest in payloads:
                raise ValueError('A3_ISS_PAGE_REPEATED')
            for ordinal, (first, second) in enumerate(zip(rows, rows[1:]), 1):
                if first[6] >= second[6]:
                    refuse_order('INTRA_PAGE_ORDER',
                        ((capture, ordinal, first), (capture, ordinal + 1, second)))
            if captures and rows[0][6] <= captures[-1]['last_begin']:
                refuse_order('CROSS_PAGE_ORDER', (last_raw, (capture, 1, rows[0])))
            capture['admission_state'] = 'A3_STAGED'
            capture['last_begin'] = rows[-1][6]
            captures.append(capture)
            # Only one prior raw boundary row is retained; no second CSV parser.
            last_raw = (capture, len(rows), tuple(rows[-1]))
            payloads.add(digest)
            for ordinal, row in enumerate(rows, 1):
                key = ('ISS_CANDLES_BOARDLESS', 'stock', request['market'], request['security'],
                       request['interval'], row[6])
                seen[key] = (capture, ordinal)
            frames.append(df)
            added = len(rows)
            accumulated += added
            emit('INFO', 'a3_page_returned', 'PAGE_STAGED', received=len(rows))
            if page_ordinal >= _A3_MAX_PAGES:
                raise ValueError('A3_ISS_PAGE_LIMIT_EXCEEDED')
            offset += len(rows)
            page_ordinal += 1
        checkpoint()
        result = _a3_assemble(frames, ticker_in)
        checkpoint()
        emit('INFO', 'a2_query_returned',
             'QUERY_RETURNED_UNVALIDATED' if accumulated else 'QUERY_EMPTY_UNVALIDATED',
             received=rows_received_total)
        return result
    except BaseException as primary:
        try:
            _a3_evidence(primary, 'QUERY_REFUSED', request, logging_context=logging_context)
        except BaseException:
            _a3_evidence_note(primary)
        if not failed_emitted and not diagnostic_failed and not success_sleep_interrupted:
            if primary is deadline_error:
                code, category, outcome = 'A2_ISS_DEADLINE_EXCEEDED', 'TIMEOUT', 'DEADLINE_EXCEEDED'
            elif primary.args == ('A3_ISS_RANGE_DEADLINE_EXCEEDED',):
                code, category, outcome = primary.args[0], 'TIMEOUT', 'DEADLINE_EXCEEDED'
            elif primary.args == ('A3_ISS_RANGE_CAPACITY_EXCEEDED',):
                code, category, outcome = primary.args[0], 'CAPACITY', 'CAPACITY_EXCEEDED'
            else:
                _, category, _, _ = _a2_request_policy(primary)
                code = (primary.args[0] if type(primary) is ValueError and primary.args
                        and primary.args[0] in ('A3_ISS_PAGE_OVERLAP', 'A3_ISS_PAGE_REPEATED',
                            'A3_ISS_PAGE_ORDER_INVALID', 'A3_ISS_PAGE_LIMIT_EXCEEDED',
                            'A3_ISS_NUMERIC_ASSEMBLY_LOSS') else 'A3_ISS_QUERY_FAILED')
                if code == 'A3_ISS_PAGE_LIMIT_EXCEEDED':
                    category = 'CAPACITY'
                elif code != 'A3_ISS_QUERY_FAILED':
                    category = 'SOURCE'
                outcome = 'QUERY_REFUSED'
            emit('ERROR', 'a2_query_failed', outcome, primary, code, category, final=True)
        raise
    finally:
        seen.clear()
        captures.clear()
        payloads.clear()
        frames.clear()
        capture = None
        last_raw = None


# %%
## Функция выгрузки данных через ручку MOEX
_A3_ANNUAL_EVIDENCE_BYTES = 64 * 1024
_A3_ANNUAL_METADATA_BYTES = 16 * 1024

# Aggregate ceilings: не меняют native 180s/1000-pages/64MiB и full sleep.
_A3_RANGE_STATE = ContextVar('a3_range_state', default=None)
_A3_RANGE_LIMITS = {
    'job': dict(ranges=8192, gets=262144, bytes=8 * 1024**3, rows=24000000,
                final_bytes=8 * 1024**3, live_bytes=16 * 1024**3, metadata_bytes=8 * 1024**3),
    'dataset': dict(ranges=1048576, gets=16777216, bytes=128 * 1024**3, rows=128000000,
                    final_bytes=16 * 1024**3, live_bytes=32 * 1024**3, metadata_bytes=8 * 1024**3),
    'run': dict(ranges=4194304, gets=67108864, bytes=512 * 1024**3, rows=512000000,
                datasets=64, jobs=4096,
                live_bytes=32 * 1024**3, metadata_bytes=8 * 1024**3),
}
_A3_MEMORY_API = None
_A3_SOURCE_DIAGNOSTIC_LIMITS = dict(seconds=2700, gets=512, bytes=128 * 1024**2,
    rows=250000, final_bytes=128 * 1024**2)
# Требование к отдельному reference harness, не расходный счётчик acquisition.
_A3_SOURCE_DIAGNOSTIC_REFERENCE_LIMITS = dict(evidence_bytes=256 * 1024**2)


def _a3_linux_read(path):
    # Только bounded read kernel metadata; никаких writes или cgroup changes.
    with open(path, 'r', encoding='utf-8', errors='strict') as stream:
        text = stream.read(65537)
    if len(text) > 65536:
        raise ValueError('A3_MEMORY_INPUT_LIMIT')
    return text


def _a3_linux_runtime():
    import resource
    if sys.maxsize <= 2**32:
        raise ValueError('A3_MEMORY_ARCH_UNKNOWN')
    return dict(namespace_inode=os.stat('/proc/thread-self/ns/cgroup').st_ino,
        namespace_link=os.readlink('/proc/thread-self/ns/cgroup'),
        page_size=os.sysconf('SC_PAGE_SIZE'),
        rlimit_as=resource.getrlimit(resource.RLIMIT_AS),
        rlimit_data=resource.getrlimit(resource.RLIMIT_DATA))


def _a3_linux_number(text, unlimited=False):
    text = text.strip()
    if unlimited and text == 'max':
        return None
    if not re.fullmatch(r'[0-9]{1,20}', text) or int(text) > 2**63 - 1:
        raise ValueError('A3_MEMORY_NUMBER_INVALID')
    return int(text)


def _a3_linux_kib(text, names):
    values = {}
    for line in text.splitlines():
        key, separator, value = line.partition(':')
        if key not in names:
            continue
        parts = value.split()
        if not separator or key in values or len(parts) != 2 or parts[1] != 'kB':
            raise ValueError('A3_MEMORY_UNITS_INVALID')
        count = _a3_linux_number(parts[0])
        if count > (2**63 - 1) // 1024:
            raise ValueError('A3_MEMORY_NUMBER_INVALID')
        values[key] = count * 1024
    if set(values) != set(names):
        raise ValueError('A3_MEMORY_FIELDS_MISSING')
    return values


def _a3_linux_cgroup_headroom(membership, mounts, controllers, available, swap_total):
    # Initial namespace + a full-root mount are required: a namespace root or
    # delegated mount alone cannot prove that no hidden ancestor limit applies.
    entries = []
    for line in membership.splitlines():
        hierarchy, names, path = line.split(':', 2)
        hierarchy = _a3_linux_number(hierarchy)
        parts = path.split('/')[1:]
        if (not path.startswith('/') or len(path) > 4096 or len(parts) > 64 or
                any(part in ('.', '..', '') for part in parts) and path != '/'):
            raise ValueError('A3_MEMORY_CGROUP_PATH_INVALID')
        entries.append((hierarchy, names.split(',') if names else [], path))
    memory = [line.split() for line in controllers.splitlines()
              if line and not line.startswith('#') and line.split()[0] == 'memory']
    if len(memory) != 1 or len(memory[0]) != 4:
        raise ValueError('A3_MEMORY_CONTROLLER_UNKNOWN')
    _, hierarchy, _, enabled = memory[0]
    hierarchy, enabled = _a3_linux_number(hierarchy), _a3_linux_number(enabled)
    if enabled not in (0, 1):
        raise ValueError('A3_MEMORY_CONTROLLER_UNKNOWN')

    def unescape(value):
        value = re.sub(r'\\(040|011|012|134)', lambda m: chr(int(m[1], 8)), value)
        if '\\' in value or not value.startswith('/'):
            raise ValueError('A3_MEMORY_MOUNT_UNKNOWN')
        return value

    mount_rows = []
    for line in mounts.splitlines():
        before, after = line.split(' - ', 1)
        before, after = before.split(), after.split()
        if len(before) < 6 or len(after) < 3:
            raise ValueError('A3_MEMORY_MOUNT_UNKNOWN')
        if after[0] in ('cgroup', 'cgroup2'):
            mount_rows.append((after[0], unescape(before[3]), unescape(before[4]), after[2].split(',')))
    v1 = [entry for entry in entries if 'memory' in entry[1]]
    v2 = [entry for entry in entries if entry[0] == 0 and not entry[1]]
    if not enabled:
        if v1:
            raise ValueError('A3_MEMORY_CONTROLLER_UNKNOWN')
        return available, 'disabled'
    if len(v1) == 1 and hierarchy == v1[0][0] and hierarchy > 0:
        version, path = 'v1', v1[0][2]
        candidates = [row for row in mount_rows if row[0] == 'cgroup' and row[1] == '/' and 'memory' in row[3]]
    elif not v1 and len(v2) == 1 and hierarchy == 0:
        version, path = 'v2', v2[0][2]
        candidates = [row for row in mount_rows if row[0] == 'cgroup2' and row[1] == '/']
    else:
        raise ValueError('A3_MEMORY_CONTROLLER_UNKNOWN')
    if not candidates:
        raise ValueError('A3_MEMORY_ANCESTORS_UNKNOWN')
    root = sorted(row[2].rstrip('/') for row in candidates)[0]
    if version == 'v2' and 'memory' not in _a3_linux_read(root+'/cgroup.controllers').split():
        raise ValueError('A3_MEMORY_CONTROLLER_UNKNOWN')
    parent, active = root, True
    # Kernel root cgroups are exempt from memory limit controls. Every non-root
    # ancestor, including the current group, participates in the minimum.
    for component in path.strip('/').split('/') if path != '/' else ():
        node = parent + '/' + component
        if version == 'v2':
            enabled_here = 'memory' in _a3_linux_read(parent+'/cgroup.subtree_control').split()
            if enabled_here and not active:
                raise ValueError('A3_MEMORY_CONTROLLER_UNKNOWN')
            active = active and enabled_here
            if active:
                limit = _a3_linux_number(_a3_linux_read(node+'/memory.max'), True)
                used = _a3_linux_number(_a3_linux_read(node+'/memory.current'))
                try:
                    swap_limit = _a3_linux_number(_a3_linux_read(node+'/memory.swap.max'), True)
                    swap_used = _a3_linux_number(_a3_linux_read(node+'/memory.swap.current'))
                except FileNotFoundError:
                    if swap_total:
                        raise ValueError('A3_MEMORY_SWAP_QUOTA_UNKNOWN') from None
                    swap_limit, swap_used = 0, 0
                if limit is not None:
                    available = min(available, max(0, limit-used))
                    if swap_limit is not None:
                        available = min(available, max(0, limit+swap_limit-used-swap_used))
        else:
            limit = _a3_linux_number(_a3_linux_read(node+'/memory.limit_in_bytes'))
            used = _a3_linux_number(_a3_linux_read(node+'/memory.usage_in_bytes'))
            available = min(available, max(0, limit-used))
            try:
                memsw_limit = _a3_linux_number(_a3_linux_read(node+'/memory.memsw.limit_in_bytes'))
                memsw_used = _a3_linux_number(_a3_linux_read(node+'/memory.memsw.usage_in_bytes'))
            except FileNotFoundError:
                if swap_total:
                    raise ValueError('A3_MEMORY_SWAP_QUOTA_UNKNOWN') from None
            else:
                available = min(available, max(0, memsw_limit-memsw_used))
        parent = node
    return available, version


def _a3_linux_memory_snapshot():
    try:
        sampled_at = time.perf_counter()
        runtime = _a3_linux_runtime()
        # Upstream proc_ns.h PROC_CGROUP_INIT_INO. Unknown/new representations
        # refuse; PID 1 equality alone is not proof of the initial namespace.
        if (runtime['namespace_inode'] != 0xEFFFFFFB or
                runtime['namespace_link'] != 'cgroup:[4026531835]' or
                type(runtime['page_size']) is not int or runtime['page_size'] <= 0):
            return None
        membership = _a3_linux_read('/proc/thread-self/cgroup')
        mounts = _a3_linux_read('/proc/self/mountinfo')
        controllers = _a3_linux_read('/proc/cgroups')
        memory = _a3_linux_kib(_a3_linux_read('/proc/meminfo'),
            ('MemTotal', 'MemAvailable', 'CommitLimit', 'Committed_AS', 'SwapTotal'))
        process = _a3_linux_kib(_a3_linux_read('/proc/self/status'), ('VmSize', 'VmData', 'VmRSS'))
        if not (process['VmData'] <= process['VmSize'] and process['VmRSS'] <= process['VmSize']):
            return None
        if _a3_linux_number(_a3_linux_read('/proc/sys/vm/overcommit_memory')) not in (0, 1, 2):
            return None
        # Conservative client commit ceiling in every overcommit mode. This is
        # a snapshot, not a reservation or a promise that malloc cannot fail.
        commit_available = max(0, memory['CommitLimit']-memory['Committed_AS'])
        process_available = commit_available
        for key, used in (('rlimit_as', process['VmSize']), ('rlimit_data', process['VmData'])):
            soft, hard = runtime[key]
            if (type(soft) is not int or type(hard) is not int or soft < -1 or hard < -1 or
                    hard != -1 and (soft == -1 or soft > hard)):
                return None
            for limit in (soft, hard):
                if limit != -1:
                    process_available = min(process_available, max(0, limit-used))
        quota, version = _a3_linux_cgroup_headroom(membership, mounts, controllers,
                                                process_available, memory['SwapTotal'])
        if (membership != _a3_linux_read('/proc/thread-self/cgroup') or
                mounts != _a3_linux_read('/proc/self/mountinfo') or
                controllers != _a3_linux_read('/proc/cgroups') or runtime != _a3_linux_runtime()):
            return None
        return dict(status='AVAILABLE', platform='linux', sampled_at=sampled_at,
            total_phys=memory['MemTotal'], available_phys=memory['MemAvailable'],
            commit_limit=memory['CommitLimit'], commit_available=commit_available,
            process_available=process_available, quota_available=quota,
            private_bytes=None, private_bytes_reason='NOT_EXPOSED_BY_PROCFS',
            vm_size_bytes=process['VmSize'], vm_data_bytes=process['VmData'],
            working_set=process['VmRSS'], page_size=runtime['page_size'], quotas_verified=True,
            quota_basis='INITIAL_CGROUP_NAMESPACE_FULL_ANCESTRY', cgroup_version=version)
    except MemoryError:
        raise
    except Exception:
        return None


def _a3_memory_snapshot():
    if sys.platform.startswith('linux'):
        return _a3_linux_memory_snapshot()
    # Read-only Win32 APIs; никакого psutil/dependency или изменения job limits.
    global _A3_MEMORY_API
    try:
        import ctypes
        from ctypes import wintypes
        if sys.platform != 'win32' or ctypes.sizeof(ctypes.c_void_p) != 8:
            return None
        if _A3_MEMORY_API is None:
            size = ctypes.c_size_t
            class Memory(ctypes.Structure):
                _fields_ = [('length', wintypes.DWORD), ('load', wintypes.DWORD)] + [
                    (name, ctypes.c_ulonglong) for name in ('total_phys', 'available_phys',
                    'total_pagefile', 'available_pagefile', 'total_virtual', 'available_virtual', 'reserved')]
            class Performance(ctypes.Structure):
                _fields_ = [('length', wintypes.DWORD)] + [(name, size) for name in (
                    'commit_total', 'commit_limit', 'commit_peak', 'physical_total', 'physical_available',
                    'cache', 'kernel_total', 'kernel_paged', 'kernel_nonpaged', 'page_size')] + [
                    (name, wintypes.DWORD) for name in ('handles', 'processes', 'threads')]
            class Process(ctypes.Structure):
                _fields_ = [('length', wintypes.DWORD), ('faults', wintypes.DWORD)] + [
                    (name, size) for name in ('peak_working_set', 'working_set', 'peak_paged', 'paged',
                    'peak_nonpaged', 'nonpaged', 'pagefile', 'peak_pagefile', 'private_bytes')]
            class Basic(ctypes.Structure):
                _fields_ = [('process_time', ctypes.c_longlong), ('job_time', ctypes.c_longlong),
                    ('flags', wintypes.DWORD), ('minimum_ws', size), ('maximum_ws', size),
                    ('active_processes', wintypes.DWORD), ('affinity', size),
                    ('priority', wintypes.DWORD), ('scheduling', wintypes.DWORD)]
            class Job(ctypes.Structure):
                _fields_ = [('basic', Basic), ('io', ctypes.c_ulonglong * 6)] + [
                    (name, size) for name in ('process_memory', 'job_memory', 'peak_process', 'peak_job')]
            class AppMemory(ctypes.Structure):
                _fields_ = [(name, ctypes.c_ulonglong) for name in
                            ('available_commit', 'private_commit', 'peak_private_commit', 'total_commit')]
            api = ctypes.WinDLL('kernel32.dll', use_last_error=True)
            signatures = {
                'GlobalMemoryStatusEx': ([ctypes.POINTER(Memory)], wintypes.BOOL),
                'K32GetPerformanceInfo': ([ctypes.POINTER(Performance), wintypes.DWORD], wintypes.BOOL),
                'GetCurrentProcess': ([], wintypes.HANDLE),
                'K32GetProcessMemoryInfo': ([wintypes.HANDLE, ctypes.POINTER(Process), wintypes.DWORD], wintypes.BOOL),
                'IsProcessInJob': ([wintypes.HANDLE, wintypes.HANDLE, ctypes.POINTER(wintypes.BOOL)], wintypes.BOOL),
                'GetProcessInformation': ([wintypes.HANDLE, ctypes.c_int, ctypes.POINTER(AppMemory),
                                           wintypes.DWORD], wintypes.BOOL),
                'QueryInformationJobObject': ([wintypes.HANDLE, ctypes.c_int, ctypes.POINTER(Job),
                                               wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)], wintypes.BOOL),
            }
            for name, (arguments, result) in signatures.items():
                function = getattr(api, name)
                function.argtypes, function.restype = arguments, result
            _A3_MEMORY_API = (api, Memory, Performance, Process, Job, AppMemory)
        api, Memory, Performance, Process, Job, AppMemory = _A3_MEMORY_API
        memory, performance, process = Memory(), Performance(), Process()
        memory.length, performance.length, process.length = map(ctypes.sizeof, (Memory, Performance, Process))
        handle = api.GetCurrentProcess()
        if not (api.GlobalMemoryStatusEx(ctypes.byref(memory)) and
                api.K32GetPerformanceInfo(ctypes.byref(performance), performance.length) and
                api.K32GetProcessMemoryInfo(handle, ctypes.byref(process), process.length)):
            return None
        if performance.page_size <= 0 or performance.commit_total > performance.commit_limit:
            return None
        # PageSize переводит страницы PERFORMANCE_INFORMATION в bytes ровно один раз.
        commit_limit = int(performance.commit_limit) * int(performance.page_size)
        commit_free = (int(performance.commit_limit) - int(performance.commit_total)) * int(performance.page_size)
        app = AppMemory()
        # ProcessAppMemoryInfo=2: additional commit available to this process,
        # а не попытка вывести ancestor limits из immediate job handle NULL.
        if not api.GetProcessInformation(handle, 2, ctypes.byref(app), ctypes.sizeof(app)):
            return None
        quota = min(int(memory.available_pagefile), int(memory.available_virtual), int(app.available_commit))
        in_job = wintypes.BOOL()
        if not api.IsProcessInJob(handle, None, ctypes.byref(in_job)):
            return None
        job_peak, job_limit = 0, None
        if in_job.value:
            job, returned = Job(), wintypes.DWORD()
            if not api.QueryInformationJobObject(None, 9, ctypes.byref(job), ctypes.sizeof(job), ctypes.byref(returned)):
                return None
            if returned.value != ctypes.sizeof(job):
                return None
            if job.basic.flags & 0x100:
                quota = min(quota, max(0, int(job.process_memory) - int(process.private_bytes)))
            if job.basic.flags & 0x200:
                job_limit, job_peak = int(job.job_memory), int(job.peak_job)
                # Peak >= current: консервативно, без ложного текущего aggregate usage.
                quota = min(quota, max(0, job_limit - job_peak))
        return dict(status='AVAILABLE', sampled_at=time.perf_counter(),
            total_phys=int(memory.total_phys), available_phys=int(memory.available_phys),
            commit_limit=commit_limit, commit_available=commit_free,
            process_available=int(memory.available_pagefile), quota_available=quota,
            private_bytes=int(process.private_bytes), working_set=int(process.working_set),
            page_size=int(performance.page_size), quotas_verified=not bool(in_job.value),
            quota_basis='UNKNOWN_ANCESTOR_JOBS' if in_job.value else 'NATIVE_PROCESS_WITHOUT_JOB',
            app_available_commit=int(app.available_commit),
            job_member=bool(in_job.value), immediate_job_limit=job_limit, immediate_job_peak=job_peak)
    except MemoryError:
        raise
    except Exception:
        return None


def _a3_capacity(reason):
    primary = ValueError('A3_ISS_RANGE_CAPACITY_EXCEEDED')
    primary.a3_capacity_reason = reason  # fixed local alias, no raw identifiers
    return primary


def _a3_memory_admit(growth_bytes):
    if type(growth_bytes) is not int or growth_bytes < 0:
        raise _a3_capacity('MEMORY_GROWTH_UNKNOWN')
    sample = _a3_memory_snapshot()
    fields = ('total_phys', 'available_phys', 'commit_limit', 'commit_available',
              'process_available', 'quota_available', 'working_set', 'page_size')
    private_valid = (type(sample) is dict and (
        type(sample.get('private_bytes')) is int and sample['private_bytes'] >= 0 or
        sample.get('platform') == 'linux' and sample.get('private_bytes') is None and
        all(type(sample.get(key)) is int and sample[key] >= 0 for key in ('vm_size_bytes', 'vm_data_bytes'))))
    if (type(sample) is not dict or sample.get('status') != 'AVAILABLE' or
            sample.get('quotas_verified') is not True or
            not private_valid or
            any(type(sample.get(name)) is not int or sample[name] < 0 for name in fields) or
            sample['page_size'] <= 0 or type(sample.get('sampled_at')) not in (int, float) or
            not math.isfinite(sample['sampled_at']) or
            not 0 <= time.perf_counter() - sample['sampled_at'] <= 1 or
            not 0 < sample['total_phys'] >= sample['available_phys'] or
            not 0 < sample['commit_limit'] >= sample['commit_available']):
        raise _a3_capacity('MEMORY_ADMISSION_UNAVAILABLE')
    reserve_phys = max(8 * 1024**3, (sample['total_phys'] + 3) // 4)
    reserve_commit = max(8 * 1024**3, (sample['commit_limit'] + 9) // 10)
    headroom = min(sample['available_phys'] - reserve_phys,
                   sample['commit_available'] - reserve_commit,
                   sample['process_available'], sample['quota_available'])
    if 2 * growth_bytes + 1024**3 > headroom:
        raise _a3_capacity('MEMORY_HEADROOM_EXCEEDED')
    return sample


def _a3_charge(counter, amount, primary=None):
    state = _A3_RANGE_STATE.get()
    if state is None:
        return
    try:
        if type(amount) is not int or amount < 0:
            raise _a3_capacity('COUNTER_INVALID')
        if state.get('scaled_offline') and counter in ('rows', 'bytes'):
            cap = 100000 if counter == 'rows' else 32 * 1024**2
            if state['run'].get(counter, 0) + amount > cap:
                raise _a3_capacity('SCALED_OFFLINE_' + counter.upper())
        for kind in ('run', 'dataset', 'job'):
            item = state.get(kind)
            limit = state.get('limits', _A3_RANGE_LIMITS)[kind].get(counter)
            if item is not None and limit is not None and item.get(counter, 0) + amount > limit:
                raise _a3_capacity(kind.upper() + '_' + counter.upper())
        for kind in ('run', 'dataset', 'job'):
            if kind in state:
                state[kind][counter] = state[kind].get(counter, 0) + amount
    except BaseException:
        if primary is not None:
            raise primary
        raise


def _a3_checkpoint(primary=None):
    state = _A3_RANGE_STATE.get()
    if state is None:
        return
    now = time.perf_counter()
    if any(now - state[kind]['started'] >= state[kind]['seconds']
           for kind in ('run', 'dataset', 'job') if kind in state):
        if primary is not None:
            raise primary
        raise requests.exceptions.Timeout('A3_ISS_RANGE_DEADLINE_EXCEEDED')


def _a3_offline_test_profile():
    # Observe an already running guard; production never imports/starts it.
    # Environment strings alone are neither runner nor admission evidence.
    try:
        guard = sys.modules.get('mds_offline_guard')
        namespace = vars(guard)
        context = namespace.get('_CONTEXT')
        report = namespace.get('report_metadata')
        root = os.path.dirname(os.path.abspath(__file__))
        canonical = lambda path: os.path.normcase(os.path.abspath(path))
        expected = canonical(os.path.join(root, 'tools', 'offline_guard.py'))
        if (canonical(namespace.get('__file__', '')) != expected or
                getattr(report, '__globals__', None) is not namespace or
                canonical(report.__code__.co_filename) != expected or
                context is None or context.started is not True or context.finished is not False or
                canonical(context.root) != canonical(root)):
            return False
        metadata = report()
        if (metadata.get('pid') != os.getpid() or metadata.get('version') != 1 or
                metadata.get('source_mode') not in ('commit', 'working-tree') or
                any(metadata.get(key) != getattr(context, key) for key in
                    ('run_id', 'tested_sha', 'source_digest', 'source_mode')) or
                not os.environ.get('MDS_OFFLINE_ROOT') or
                canonical(os.environ['MDS_OFFLINE_ROOT']) != canonical(root) or
                os.environ.get('MDS_OFFLINE_RUN_ID') != context.run_id or
                os.environ.get('MDS_OFFLINE_SOURCE_MODE') != context.source_mode):
            return False
        if namespace.get('_INSTALLED') is True:
            return True
        # Native F2/F-LOG keep their existing policies. Their actual installed
        # adapter shares this counter object and marks context ready afterwards.
        counts = context.native_counts
        native = {'f2': ('tests', 'f2', '_probe.py'),
                  'flog': ('tests', 'logging', '_probe.py')}.get(context.lane)
        if type(counts) is not dict or native is None:
            return False
        probe_path = canonical(os.path.join(root, *native))
        for module in tuple(sys.modules.values()):
            attributes = getattr(module, '__dict__', {})
            if (attributes.get('_guard_counts') is counts and
                    canonical(attributes.get('__file__', '')) == probe_path):
                return True
        return False
    except Exception:
        return False


@contextmanager
def _a3_scope(kind, planned=0):
    previous = _A3_RANGE_STATE.get()
    state = dict(previous or {})
    now = time.perf_counter()
    if 'run' not in state:
        state['run'] = dict(started=now, seconds=256 * 86400)
        state['scaled_offline'] = _a3_offline_test_profile()
    if kind in ('job', 'dataset') and 'dataset' not in state:
        state['dataset'] = dict(started=now, seconds=86400, retained_bytes=0)
        state['run']['datasets'] = state['run'].get('datasets', 0) + 1
        if state['run']['datasets'] > _A3_RANGE_LIMITS['run']['datasets']:
            raise _a3_capacity('RUN_DATASETS')
    if kind == 'job':
        seconds = min(96 * 3600, 180 * planned + 6 * 3600)
        state['job'] = dict(started=now, seconds=seconds, planned=planned, rows=0, ranges=0)
        state['dataset']['seconds'] = min(96 * 86400, state['dataset']['seconds'] + seconds)
    token = _A3_RANGE_STATE.set(state)
    try:
        if kind == 'job':
            _a3_charge('jobs', 1)
            for scope in ('job', 'dataset', 'run'):
                if state[scope].get('ranges', 0) + planned > state.get('limits', _A3_RANGE_LIMITS)[scope]['ranges']:
                    raise _a3_capacity(scope.upper() + '_PLANNED_RANGES')
        _a3_checkpoint()
        yield state
    finally:
        _A3_RANGE_STATE.reset(token)


def _a3_operation(kind):
    # wraps сохраняет public signature; без нового public resource argument.
    def decorate(function):
        @wraps(function)
        def bounded(*args, **kwargs):
            with _a3_scope(kind):
                return function(*args, **kwargs)
        return bounded
    return decorate


def _a3_reserve(frame_bytes, metadata_bytes, growth):
    state = _A3_RANGE_STATE.get()
    if state is None:
        return
    if 'job' in state:
        state['job']['frame_bytes'] = frame_bytes
        state['job']['metadata_bytes'] = metadata_bytes
    retained = state.get('dataset', {}).get('retained_bytes', 0)
    for kind in ('run', 'dataset', 'job'):
        if kind not in state:
            continue
        live = frame_bytes + (0 if kind == 'job' else retained)
        limits = state.get('limits', _A3_RANGE_LIMITS)[kind]
        if live > limits['live_bytes'] or metadata_bytes > limits['metadata_bytes']:
            raise _a3_capacity(kind.upper() + '_LIVE_MEMORY')
        state[kind]['peak_frame_bytes'] = max(state[kind].get('peak_frame_bytes', 0), live)
        state[kind]['peak_metadata_bytes'] = max(state[kind].get('peak_metadata_bytes', 0), metadata_bytes)
    _a3_allocation_admit(growth, frame_bytes + retained, metadata_bytes)


def _a3_allocation_admit(growth, live=0, metadata=0):
    state = _A3_RANGE_STATE.get()
    if state is not None and state.get('scaled_offline'):
        if not _a3_offline_test_profile():
            raise _a3_capacity('OFFLINE_PROFILE_IDENTITY_LOST')
        # Заведомо bounded fixtures, не production capacity admission.
        if growth > 128 * 1024**2 or live > 128 * 1024**2 or metadata > 64 * 1024**2:
            raise _a3_capacity('SCALED_OFFLINE_MEMORY')
        return
    _a3_memory_admit(growth)


def _a3_final_payload(frame, kind):
    payload = int(frame.memory_usage(index=True, deep=True).sum())
    state = _A3_RANGE_STATE.get()
    limits = state.get('limits', _A3_RANGE_LIMITS) if state else _A3_RANGE_LIMITS
    if payload > limits[kind]['final_bytes']:
        raise _a3_capacity(kind.upper() + '_FINAL_PAYLOAD')
    if state is not None and state.get('scaled_offline') and payload > 64 * 1024**2:
        raise _a3_capacity('SCALED_OFFLINE_PAYLOAD')
    return payload


def _a3_range_event(context, event, ticker, interval, planned, returned=None):
    state = _A3_RANGE_STATE.get()
    job = state['job']
    outcome = 'RANGE_PLAN_STARTED' if returned is None else 'CLIENT_PLAN_COMPLETE_SOURCE_UNVERIFIED'
    fields = dict(stage='data_gathering', fields={}, outcome=outcome,
        counts=dict(rows_received=job.get('rows', 0), rows_accepted=None, rows_quarantined=None,
            pages=job.get('pages', 0), instruments=1,
            null_reasons={'rows_accepted': 'NOT_ASSESSED', 'rows_quarantined': 'NOT_ASSESSED'}))
    if type(ticker) is str:
        fields['instrument'] = ticker
    if type(interval) is int and interval > 0:
        fields['interval'] = interval
    if type(context) is dict:
        for key in ('snapshot_id', 'dataset_id', 'file', 'artifact_id', 'target_id', 'page'):
            value = context.get(key)
            if type(value) is str and len(value) <= 256 or key == 'page' and type(value) is int and value >= 0:
                fields[key] = value
    message = (f'История {outcome}; planned={planned}; completed={job.get("ranges", 0)}; '
               f'get={job.get("gets", 0)}; retry={job.get("retries", 0)}; '
               f'bytes={job.get("bytes", 0)}; rows={job.get("rows", 0)}')
    _a2_diagnostic(context, 'INFO', event, message, fields)


def _a3_source_diagnostic_plan():
    # Это protocol data, не live permission и не source availability inventory.
    dates = {24: ('2023-02-27', '2024-03-02'), 60: ('2023-02-27', '2024-03-02'),
             10: ('2024-01-30', '2024-03-02'), 1: ('2024-02-23', '2024-03-02')}
    return [dict(ticker=ticker, interval=interval, from_date=dates[interval][0],
                 till_date=dates[interval][1], authority='SEPARATE_LIVE_GRANT_REQUIRED')
            for ticker in ('SBER', 'MOEX') for interval in (24, 60, 10, 1)]


@contextmanager
def _a3_source_diagnostic_scope():
    # Accounting envelope only. Это не live grant и не обход transport guard.
    if _A3_RANGE_STATE.get() is not None:
        raise ValueError('A3_DIAGNOSTIC_NESTING_REJECTED')
    with _a3_scope('run') as state:
        limits = {kind: dict(values) for kind, values in _A3_RANGE_LIMITS.items()}
        limits['run'].update({key: value for key, value in _A3_SOURCE_DIAGNOSTIC_LIMITS.items()
                              if key not in ('seconds', 'final_bytes')})
        for kind in ('job', 'dataset'):
            limits[kind]['final_bytes'] = _A3_SOURCE_DIAGNOSTIC_LIMITS['final_bytes']
        state['limits'] = limits
        state['run']['seconds'] = _A3_SOURCE_DIAGNOSTIC_LIMITS['seconds']
        yield state


def _a3_range_failure(context, ticker, interval, primary):
    code = primary.args[0] if primary.args and type(primary.args[0]) is str and primary.args[0] in (
        'A3_ISS_RANGE_CAPACITY_EXCEEDED', 'A3_ISS_RANGE_DEADLINE_EXCEEDED',
        'A3_ISS_ANNUAL_KEY_CONFLICT', 'A3_ISS_RANGE_KEY_CONFLICT',
        'A3_ISS_NUMERIC_ASSEMBLY_LOSS') else 'A3_ISS_RANGE_FAILED'
    if code == 'A3_ISS_RANGE_FAILED':
        # Native query/logger уже доставил свой primary; не дублируем его.
        return
    category = ('CAPACITY' if code == 'A3_ISS_RANGE_CAPACITY_EXCEEDED' else
                'TIMEOUT' if code == 'A3_ISS_RANGE_DEADLINE_EXCEEDED' else 'SOURCE')
    nullable = ('run_id', 'snapshot_id', 'dataset_id', 'instrument', 'interval',
                'page', 'file', 'artifact_id', 'target_id', 'endpoint', 'attempt', 'exception_type')
    error = dict(category=category, code=code, stage='data_gathering',
                 retryable=False, final=True, message='Отказ всей истории: ' + code)
    error.update({key: None for key in nullable})
    fields = dict(stage='data_gathering', fields={}, outcome='RANGE_PLAN_REFUSED')
    if type(ticker) is str:
        fields['instrument'] = ticker
    if type(interval) is int and interval > 0:
        fields['interval'] = interval
    if type(context) is dict:
        for key in nullable:
            if key not in ('exception_type', 'attempt', 'endpoint'):
                value = context.get(key)
                if type(value) is str and len(value) <= 256 or key == 'page' and type(value) is int and value >= 0:
                    error[key] = value
                    if key != 'run_id':
                        fields[key] = value
    error.update(instrument=fields.get('instrument'), interval=fields.get('interval'),
                 endpoint='ISS_CANDLES', exception_type='Timeout' if isinstance(primary, requests.Timeout)
                 else 'ValueError' if isinstance(primary, ValueError) else 'Exception')
    error['null_reasons'] = {key: 'NOT_AVAILABLE_OR_NOT_APPLICABLE'
                             for key in nullable if error[key] is None}
    fields['error'] = error
    _a2_diagnostic(context, 'ERROR', 'a3_ranges_failed', 'Отказ всей истории: ' + code,
                   fields, primary)


def _a3_calendar_anniversary(anchor, years):
    target_year = anchor.year - years
    day = anchor.day
    if anchor.month == 2 and day == 29 and not calendar.isleap(target_year):
        day = 28
    return anchor.replace(year=target_year, day=day)


def _a3_annual_row(frame, position):
    # Отдельные scalar values: iterrows может округлить целое до float.
    columns = ('open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker')
    return dict(values=tuple(frame[column].iloc[position] for column in columns),
                dtypes=tuple(str(frame[column].dtype) for column in columns),
                row_position=position, index=frame.index[position])


def _a3_typed_evidence_payload(request, pairs, logging_context=None,
                               format_name='A3_ANNUAL_TYPED_ROWS_V1', reason='ANNUAL_KEY_CONFLICT'):
    # Только parsed scalar evidence: исходный CSV public query не возвращает.
    def identifier(value):
        bounded = _a3_metadata(value)
        if type(bounded) is str:
            content = bounded.encode('utf-8')
            return dict(value=None, state='HASH_ONLY_IDENTIFIER', bytes=len(content),
                        sha256=hashlib.sha256(content).hexdigest(), encoding='UTF8')
        if type(bounded) is dict:
            return bounded
        return dict(value=None, state='REDACTED_IDENTIFIER')

    typed_rows, provenance = [], []
    for row, window, lower, upper in pairs[:2]:
        cells = []
        for position, value in enumerate(row['values'][:8]):
            cell = dict(dtype=row['dtypes'][position])
            if position < 6:
                numerator, denominator, negative_zero = _a3_numeric_identity(value)
                if pd.api.types.is_integer(value) and not isinstance(value, bool):
                    cell.update(kind='integer', decimal=str(int(value)))
                else:
                    cell.update(kind='float', hex=float(value).hex(),
                                ratio=[numerator, denominator], negative_zero=negative_zero)
            elif type(value) is str:
                cell.update(kind='text', value=value)
            else:
                raise ValueError('A3_ANNUAL_EVIDENCE_CELLS_INVALID')
            cells.append(cell)
        typed_rows.append(tuple(cells))
        provenance.append(dict(window_ordinal=window, from_date=lower, till_date=upper,
            row_position=row['row_position'], index=_a3_metadata(row['index']),
            dtypes=list(row['dtypes']), ticker=identifier(row['values'][8])))
    # Оба cap измеряются точно по JSON_ASCII_COMPACT_SORTED_V1.
    payload = json.dumps(typed_rows, ensure_ascii=True, sort_keys=True, separators=(',', ':')).encode('utf-8')
    if len(payload) > _A3_ANNUAL_EVIDENCE_BYTES:
        raise ValueError('A3_ANNUAL_EVIDENCE_CELLS_LIMIT')
    context = logging_context if type(logging_context) is dict else {}
    metadata = dict(format=format_name, reason=reason, state='CAPTURED',
        serialization='JSON_ASCII_COMPACT_SORTED_V1',
        request=dict(market=_a3_metadata(request['market']), security=identifier(request['security']),
                     interval=identifier(request['interval'])),
        engine='stock', endpoint='ISS_CANDLES',
        identity='ISS_CANDLES_BOARDLESS/stock/market/security/interval/raw_begin',
        board=None, board_reason='NOT_PRESENT_IN_REQUEST_A5_UNVERIFIED',
        source_identity='NOT_VERIFIED', availability='NOT_VERIFIED',
        source_representation='PARSED_SCALARS_ONLY',
        columns=['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end'],
        row_cap=2, cell_bytes_cap=_A3_ANNUAL_EVIDENCE_BYTES, cell_bytes=len(payload),
        metadata_bytes_cap=_A3_ANNUAL_METADATA_BYTES, rows=provenance,
        correlation={key: identifier(context.get(key))
                     for key in ('run_id', 'snapshot_id', 'dataset_id')},
        verified_sha=None, sha_reason='NOT_VERIFIED', persistence='NOT_REQUESTED')
    encoded = json.dumps(metadata, ensure_ascii=True, sort_keys=True, separators=(',', ':')).encode('utf-8')
    if len(encoded) > _A3_ANNUAL_METADATA_BYTES:
        raise ValueError('A3_ANNUAL_EVIDENCE_METADATA_LIMIT')
    return dict(metadata=metadata, typed_rows=tuple(typed_rows))


def _a3_annual_evidence(primary, request, pairs, logging_context=None):
    try:
        if not hasattr(primary, 'a3_annual_evidence'):
            primary.a3_annual_evidence = _a3_typed_evidence_payload(request, pairs, logging_context)
    except BaseException:
        _a3_evidence_note(primary)


def _a3_range_date(value):
    try:
        if type(value) is not str or len(value) != 10:
            raise ValueError()
        parsed = datetime.datetime.strptime(value, '%Y-%m-%d').date()
        if parsed.isoformat() != value:
            raise ValueError()
        return parsed
    except (ValueError, TypeError, OverflowError):
        raise ValueError('A3_ISS_RANGE_BOUNDS_INVALID') from None


def _a3_month_boundary(anchor, months):
    # Всегда от исходного anchor: Jan31 восстанавливается после Feb28/29.
    ordinal = (anchor.year - 1) * 12 + anchor.month - 1 - months
    year, month = divmod(ordinal, 12)
    if year < 0:
        raise ValueError('A3_ISS_RANGE_BOUNDS_INVALID')
    year, month = year + 1, month + 1
    return anchor.replace(year=year, month=month,
                          day=min(anchor.day, calendar.monthrange(year, month)[1]))


def _a3_physical_plan(cores, interval):
    if type(interval) is not int or interval not in (24, 60, 10, 1):
        raise ValueError('A3_ISS_RANGE_INTERVAL_UNSUPPORTED')
    plan = []
    for logical, (lower, upper, requested_upper) in enumerate(cores, 1):
        boundary = upper
        step = 0
        while True:
            if lower == upper:
                following = lower
            elif interval in (24, 60):
                following = lower
            elif interval == 10:
                step += 1
                available_months = (upper.year - 1) * 12 + upper.month - 1
                following = lower if step > available_months else max(lower, _a3_month_boundary(upper, step))
            else:
                following = lower if (boundary - lower).days <= 7 else boundary - datetime.timedelta(days=7)
            # Сначала сравнение; date.max + day никогда не вычисляется.
            physical_upper = requested_upper if boundary >= requested_upper else min(
                requested_upper, boundary + datetime.timedelta(days=1))
            plan.append(dict(logical_ordinal=logical, physical_ordinal=len(plan) + 1,
                from_date=following.isoformat(), till_date=physical_upper.isoformat(),
                core_lower=following.isoformat(), core_upper=boundary.isoformat(),
                logical_lower=lower.isoformat(), logical_upper=requested_upper.isoformat()))
            if len(plan) > _A3_RANGE_LIMITS['job']['ranges']:
                raise ValueError('A3_ISS_RANGE_CAPACITY_EXCEEDED')
            if following == lower:
                break
            boundary = following
    return plan


def _a3_range_plan(lower, upper, interval):
    lower, upper = _a3_range_date(lower), _a3_range_date(upper)
    if lower > upper:
        raise ValueError('A3_ISS_RANGE_BOUNDS_INVALID')
    cores = []
    previous, ordinal = upper, 1
    while True:
        boundary = lower if ordinal >= upper.year else max(lower, _a3_calendar_anniversary(upper, ordinal))
        requested_upper = upper if previous == upper else min(upper, previous + datetime.timedelta(days=1))
        cores.append((boundary, previous, requested_upper))
        if boundary == lower:
            break
        previous, ordinal = boundary, ordinal + 1
    return _a3_physical_plan(cores, interval)


def _a3_range_evidence(primary, request, pairs, logging_context):
    # Parsed scalar evidence не выдаётся за raw CSV/page SHA.
    if hasattr(primary, 'a3_range_evidence'):
        return
    try:
        if len(pairs) != 2:
            raise ValueError('A3_RANGE_EVIDENCE_PAIR_INVALID')
        cross_annual = pairs[0][1]['logical_ordinal'] != pairs[1][1]['logical_ordinal']
        if cross_annual:
            try:
                _a3_annual_evidence(primary, request,
                    tuple((row, leaf['logical_ordinal'], leaf['logical_lower'], leaf['logical_upper'])
                          for row, leaf in pairs), logging_context)
            except BaseException:
                _a3_evidence_note(primary)
        source = _a3_typed_evidence_payload(request,
            tuple((row, leaf['logical_ordinal'], leaf['from_date'], leaf['till_date'])
                  for row, leaf in pairs), logging_context, 'A3_RANGE_TYPED_ROWS_V1', 'RANGE_KEY_CONFLICT')
        metadata = dict(source['metadata'],
                        conflict_scope='CROSS_ANNUAL' if cross_annual else 'INTERNAL_PHYSICAL')
        metadata['rows'] = [dict(previous, logical_ordinal=leaf['logical_ordinal'],
            physical_ordinal=leaf['physical_ordinal'], logical_lower=leaf['logical_lower'],
            logical_upper=leaf['logical_upper'])
            for previous, (_, leaf) in zip(source['metadata']['rows'], pairs)]
        encoded = json.dumps(metadata, ensure_ascii=True, sort_keys=True, separators=(',', ':')).encode('utf-8')
        if len(encoded) > _A3_ANNUAL_METADATA_BYTES:
            raise ValueError('A3_RANGE_EVIDENCE_METADATA_LIMIT')
        primary.a3_range_evidence = dict(metadata=metadata, typed_rows=source['typed_rows'])
    except BaseException:
        _a3_evidence_note(primary)


def _a3_acquire(ticker_in, ticker_type, plan, interval, logging_context):
    market = 'bonds' if ticker_type in ['Облигации', 'Еврооблигации'] else 'shares'
    request = dict(market=market, security=f'{ticker_in}', interval=f'{interval}')
    identity = ('ISS_CANDLES_BOARDLESS', 'stock', market, request['security'], request['interval'])
    frames, seen, positions = [], {}, []
    offset, frame_bytes, metadata_bytes = 0, 0, 0
    with _a3_scope('job', len(plan)):
        try:
            _a3_range_event(logging_context, 'a3_ranges_started', ticker_in, interval, len(plan))
            for leaf in plan:
                _a3_checkpoint()
                _a3_reserve(frame_bytes, metadata_bytes, 16 * 1024**2)
                _A3_RANGE_STATE.get()['job']['collector_bytes'] = frame_bytes
                _a3_charge('ranges', 1)
                before_rows = _A3_RANGE_STATE.get()['job']['rows']
                df = moex_query(ticker_in, ticker_type, leaf['from_date'], leaf['till_date'], interval,
                                logging_context=logging_context)
                # Real query charges originals before pandas parsing; mocks still audit rows.
                if _A3_RANGE_STATE.get()['job']['rows'] == before_rows:
                    _a3_charge('rows', len(df))
                _a3_checkpoint()
                if len(df) == 0:
                    continue
                frame_bytes += int(df.memory_usage(index=True, deep=True).sum())
                _a3_reserve(frame_bytes, metadata_bytes, 0)
                frames.append(df)
                for position in range(len(df)):
                    row = _a3_annual_row(df, position)
                    key = identity + (row['values'][6],)
                    if key in seen:
                        previous_frame, previous_position, previous_leaf = seen[key]
                        previous = _a3_annual_row(frames[previous_frame], previous_position)
                        same = (all(_a3_numeric_identity(before) == _a3_numeric_identity(after)
                                    for before, after in zip(previous['values'][:6], row['values'][:6]))
                                and previous['values'][6:] == row['values'][6:])
                        if not same:
                            code = ('A3_ISS_ANNUAL_KEY_CONFLICT' if previous_leaf['logical_ordinal'] !=
                                    leaf['logical_ordinal'] else 'A3_ISS_RANGE_KEY_CONFLICT')
                            primary = ValueError(code)
                            try:
                                _a3_range_evidence(primary, request, ((previous, previous_leaf), (row, leaf)),
                                                   logging_context)
                            except BaseException:
                                _a3_evidence_note(primary)
                            raise primary
                    else:
                        # Только locator; ранний exact repeat не заменяет первого.
                        seen[key] = (len(frames) - 1, position, leaf)
                        positions.append(offset + position)
                        metadata_bytes += (sys.getsizeof(key) + sum(sys.getsizeof(item) for item in key)
                                           + sys.getsizeof(seen[key]) + sys.getsizeof(positions[-1]) + 64)
                _a3_reserve(frame_bytes, metadata_bytes + sys.getsizeof(seen) + sys.getsizeof(positions), 0)
                offset += len(df)
            seen.clear()
            # Flat originals всех logical/physical leafs до dedup, включая exact repeats.
            growth = frame_bytes * 4 + sum(len(frame) for frame in frames) * 512 + 16 * 1024**2
            _a3_reserve(frame_bytes + growth, sys.getsizeof(positions), growth)
            result = _a3_assemble(frames, ticker_in)
            if frames:
                result = result.iloc[positions].sort_values(by='begin', kind='stable').reset_index(drop=True)
            _a3_final_payload(result, 'job')
            _a3_checkpoint()
            _a3_range_event(logging_context, 'a3_ranges_returned', ticker_in, interval, len(plan), len(result))
            return result
        except BaseException as primary:
            _a3_range_failure(logging_context, ticker_in, interval, primary)
            raise

def _a3_history(ticker_in, ticker_type, lower, upper, interval, *, logging_context=None):
    return _a3_acquire(ticker_in, ticker_type, _a3_range_plan(lower, upper, interval),
                       interval, logging_context)


def _a3_dataset_append(frames, frame):
    # Empty frames участвуют в pandas dtype rules, как в прежнем concat.
    state = _A3_RANGE_STATE.get()
    payload = int(frame.memory_usage(index=True, deep=True).sum())
    retained = state['dataset'].get('retained_bytes', 0) + payload
    state['dataset']['retained_bytes'] = retained
    _a3_reserve(0, len(frames) * 128, 16 * 1024**2)
    frames.append(frame)


def _a3_dataset_union(frames):
    state = _A3_RANGE_STATE.get()
    retained = state['dataset'].get('retained_bytes', 0)
    growth = retained * 4 + sum(len(frame) for frame in frames) * 512 + 16 * 1024**2
    _a3_reserve(growth, len(frames) * 128, growth)
    _a3_checkpoint()
    # full_reload сохраняет исходные ticker indices; update reset делает ниже.
    result = pd.concat(frames) if frames else pd.DataFrame()
    if len(result):
        _a3_numeric_union(frames, result)
    _a3_final_payload(result, 'dataset')
    _a3_checkpoint()
    return result


def moex (ticker_in, ticker_type, years, interval, *, logging_context=None):
    global exception_list
    today = datetime.datetime.now().date()
    # range сохраняет native __index__/TypeError; bool остаётся no-query профилем.
    count = range(1, years).stop
    if isinstance(years, bool) or count <= 0:
        return pd.DataFrame()
    anniversaries = [today] + [_a3_calendar_anniversary(today, i) for i in range(1, count + 1)]
    cores = [(anniversaries[i], anniversaries[i - 1],
              today if i == 1 else min(today, anniversaries[i - 1] + datetime.timedelta(days=1)))
             for i in range(1, count + 1)]
    plan = _a3_physical_plan(cores, interval)
    try:
        return _a3_acquire(ticker_in, ticker_type, plan, interval, logging_context)
    except BaseException as primary:
        try:
            exception_list.append(ticker_in)
        except BaseException:
            try:
                BaseException.add_note(primary, 'A3_ANNUAL_EXCEPTION_LIST_FAILED')
            except BaseException:
                pass
        raise

# %%
## Функция выгружает даты торгов по каждому тикеру (когда начали торговать по этому тикеру и конца закончили)
## полезно, чтобы перестать выгружать данные по тем тикерам, которые уже делистили
def build_tickers_dates(all_stocks_ru, current_path):
    """
    По уникальным тикерам из all_stocks_ru['TRADE_CODE'] получает:
    - дату начала торгов (ISSUEDATE) => issue_date
    - дату последнего дня торгов, если бумага больше не торгуется => stopped_date

    Возвращает DataFrame tickers_dates с колонками:
    ['TRADE_CODE', 'issue_date', 'stopped_date']
    """

    session = requests.Session()

    # Базовые URL'ы
    desc_url = "https://iss.moex.com/iss/securities/{secid}.json"
    securities_url = "https://iss.moex.com/iss/securities.json"
    history_url = (
        "https://iss.moex.com/iss/history/engines/stock/markets/shares/"
        "boards/{board}/securities/{secid}.json"
    )

    # Берём уникальные тикеры, убираем NaN и пустоты, приводим к верхнему регистру
    tickers = (
        all_stocks_ru["TRADE_CODE"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    tickers = tickers[tickers != ""].str.upper().unique()

    rows = []

    for secid in tickers:
        issue_date = pd.NaT
        stopped_date = pd.NaT
        is_traded = None
        primary_boardid = None

        # --- 1. Дата начала торгов (ISSUEDATE) из description ---
        try:
            params_desc = {
                "iss.meta": "off",
                "iss.only": "description",
                "description.columns": "name,value",
            }
            r = session.get(
                desc_url.format(secid=secid),
                params=params_desc,
                timeout=5,
            )
            r.raise_for_status()
            j = r.json()

            desc = j.get("description", {})
            cols = desc.get("columns", [])
            data = desc.get("data", [])

            if "name" in cols and "value" in cols:
                name_idx = cols.index("name")
                value_idx = cols.index("value")

                for row_ in data:
                    if row_[name_idx] == "ISSUEDATE":
                        date_str = row_[value_idx]
                        if date_str:
                            issue_date = pd.to_datetime(date_str)
                        break
        except Exception:
            # если что-то пошло не так — оставляем issue_date = NaT
            pass

        # --- 2. is_traded и primary_boardid из /iss/securities ---
        try:
            params_sec = {
                "q": secid,
                "iss.meta": "off",
                "iss.only": "securities",
                "securities.columns": "secid,group,is_traded,primary_boardid",
            }
            r = session.get(
                securities_url,
                params=params_sec,
                timeout=5,
            )
            r.raise_for_status()
            j = r.json()

            sec = j.get("securities", {})
            cols = sec.get("columns", [])
            data = sec.get("data", [])

            if all(c in cols for c in ("secid", "group", "is_traded", "primary_boardid")):
                secid_idx = cols.index("secid")
                group_idx = cols.index("group")
                is_traded_idx = cols.index("is_traded")
                pb_idx = cols.index("primary_boardid")

                for row_ in data:
                    # выбираем именно акцию (group == 'stock_shares') и нужный SECID
                    if str(row_[secid_idx]).upper() == secid and row_[group_idx] == "stock_shares":
                        is_traded = row_[is_traded_idx]
                        primary_boardid = row_[pb_idx]
                        break
        except Exception:
            pass

        # --- 3. Если бумага больше не торгуется (is_traded == 0), берём последний день торгов ---
        if primary_boardid and is_traded == 0:
            try:
                params_hist = {
                    "iss.meta": "off",
                    "iss.only": "history",
                    "history.columns": "TRADEDATE",
                    "sort_column": "TRADEDATE",
                    "sort_order": "desc",
                    "limit": 1,
                }
                r = session.get(
                    history_url.format(board=primary_boardid, secid=secid),
                    params=params_hist,
                    timeout=5,
                )
                r.raise_for_status()
                j = r.json()

                hist = j.get("history", {})
                cols = hist.get("columns", [])
                data = hist.get("data", [])

                if "TRADEDATE" in cols and data:
                    td_idx = cols.index("TRADEDATE")
                    date_str = data[0][td_idx]
                    if date_str:
                        stopped_date = pd.to_datetime(date_str)
            except Exception:
                # если история не доступна — оставляем NaT
                pass

        rows.append(
            {
                "TRADE_CODE": secid,
                "issue_date": issue_date,
                # Для торгуемых бумаг будет NaT, для делистнутых — дата последнего дня торгов
                "stopped_date": stopped_date,
            }
        )

    tickers_dates = pd.DataFrame(rows)
    
    ## Сохранение файлов
    tickers_dates.to_excel(("{}/datasets/ticker_lists/tickers_dates.xlsx").format(current_path))
    tickers_dates.to_csv(("{}/datasets/ticker_lists/tickers_dates.csv").format(current_path))

    return tickers_dates


# %%
### Функция для выгрузки данных с нуля
@_a3_operation('dataset')
def full_reload (all_stocks_ru, interval, years, filename, word, current_path, tickers_dates, *, logging_context=None):
    operation_started = time.perf_counter()
    active_fields = dict(dataset_id=filename, interval=interval, file=filename + '.csv')
    _a1_event(logging_context, 'INFO', 'a1_full_reload_started', active_fields)
    try:
        input_rows = len(all_stocks_ru)
        all_stocks_ru = _prepare_ticker_catalogue(all_stocks_ru)
        _a1_event(logging_context, 'INFO', 'a1_catalogue_prepared',
            dict(active_fields, counts=_a1_counts(input_rows, len(all_stocks_ru), len(all_stocks_ru))))
        frames = []
        today = datetime.datetime.now()
        start_date = today

        ##определяем границу нужного диапазона выгрузки
        if years != 0:
            date_shift_needed = _a3_calendar_anniversary(start_date.date(), years).isoformat()
        else:
            date_shift_needed = '0'


        for i in range(0,len(all_stocks_ru)):
            ticker_in = all_stocks_ru.iloc[i]['TRADE_CODE']
            ticker_type = all_stocks_ru.iloc[i]['SUPERTYPE']
            active_fields = dict(dataset_id=filename, interval=interval,
                                 instrument=ticker_in, file=filename + '.csv')

            if len(ticker_in) > 0: #проверка что тикер выгрузился и есть

                #определение левой границы выгрузки: или дата листинга или самое раннее нужное значение
                end_date_mx = tickers_dates[tickers_dates['TRADE_CODE'] == ticker_in]['issue_date'].values[0]
                end_date_mx = str(end_date_mx)[:10]
                if date_shift_needed > end_date_mx:
                    end_date_mx = date_shift_needed

                if tickers_dates[tickers_dates['TRADE_CODE'] == ticker_in]['stopped_date'].isna().values[0] == True:
                    start_date_mx = start_date.date().isoformat()
                else:
                    start_date_mx = tickers_dates[tickers_dates['TRADE_CODE'] == ticker_in]['stopped_date'].values[0]
                    start_date_mx = str(start_date_mx)[:10]

                _a1_event(logging_context, 'INFO', 'a1_instrument_started',
                    dict(dataset_id=filename, interval=interval, instrument=ticker_in, file=filename + '.csv'))
                query_started = time.perf_counter()
                df = _a3_history(ticker_in, ticker_type, end_date_mx, start_date_mx, interval,
                                logging_context=logging_context)
                _a1_event(logging_context, 'INFO', 'a1_instrument_returned',
                    dict(active_fields, outcome='RETURNED',
                         duration_ms=max(0, (time.perf_counter() - query_started) * 1000),
                         counts=_a1_counts(len(df), len(df), 1)))
                if len(df) > 0:
                    _a3_dataset_append(frames, df)
            else:
                print(ticker_in)

        df_full = _a3_dataset_union(frames)
        print("Записей для промежутка {} лет с интервалом {} {}.: {}".format(years,interval, word, len(df_full)))
        active_fields = dict(dataset_id=filename, interval=interval, file=filename + '.xlsx')
        if len(df_full) > 0 and len(df_full) < 1048576: df_full.to_excel(('{}/datasets/{}'.format(current_path,filename + '.xlsx')),index = False)
        active_fields = dict(dataset_id=filename, interval=interval, file=filename + '.csv')
        if len(df_full) > 0: df_full.to_csv(('{}/datasets/{}'.format(current_path, filename + '.csv')),index = False)
        _a1_event(logging_context, 'INFO', 'a1_full_reload_returned',
            dict(dataset_id=filename, interval=interval, file=filename + '.csv', outcome='RETURNED',
                 duration_ms=max(0, (time.perf_counter() - operation_started) * 1000), counts=_a1_counts(len(df_full), len(df_full), len(all_stocks_ru))))
    except BaseException as error:
        _a1_failure(logging_context, error,
            dict(active_fields, duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
        raise

# %%
### Функция для обновления текущих датасетов по конфигу
@_a3_operation('run')
def data_update (config, current_path, all_stocks_ru, tickers_dates, *, logging_context=None):
    operation_started = time.perf_counter()
    active_fields = {}
    _a1_event(logging_context, 'INFO', 'a1_data_update_started', active_fields)
    try:
        today = datetime.datetime.now()
        input_rows = len(all_stocks_ru)
        all_stocks_ru = _prepare_ticker_catalogue(all_stocks_ru)
        _a1_event(logging_context, 'INFO', 'a1_catalogue_prepared',
            dict(counts=_a1_counts(input_rows, len(all_stocks_ru), len(all_stocks_ru))))
        active_fields = dict(file='ticker_lists/moex_full.csv')
        moex_full_catalogue = pd.read_csv(("{}/datasets/ticker_lists/moex_full.csv").format(current_path), index_col=0)
        lookup = _prepare_ticker_catalogue(moex_full_catalogue)
        _a1_event(logging_context, 'INFO', 'a1_lookup_prepared',
            dict(file='ticker_lists/moex_full.csv',
                 counts=_a1_counts(len(moex_full_catalogue), len(lookup), len(lookup))))
        catalogue_keys = set(all_stocks_ru['TRADE_CODE']) | set(lookup['TRADE_CODE'])

        ## обновление готовых файлов
        for j in range(0,len(config)):
            with _a3_scope('dataset'):
                filename_j = config[j]['filename']
                interval = config[j]['interval']
                years = config[j]['years']
                word = config[j]['word']
                active_fields = dict(dataset_id=filename_j, interval=interval, file=filename_j + '.csv')
                _a1_event(logging_context, 'INFO', 'a1_dataset_started', active_fields)

                dataset_path = current_path + "datasets/{}.csv".format(filename_j)

                # проверка что файл существует
                if os.path.isfile(dataset_path) == False:
                    print("Файл текущего набора отсутствует, не могу его обновить")
                    print("Начинаю выгружать его с нуля")
                    _a1_event(logging_context, 'INFO', 'a1_cold_start',
                        dict(dataset_id=filename_j, interval=interval, file=filename_j + '.csv', outcome='MISSING_DATASET'))
                    full_reload (all_stocks_ru, interval, years, filename_j, word, current_path, tickers_dates,
                                 logging_context=logging_context)

                else:
                    # Bounded admission до чтения previous; deep payload не RSS oracle.
                    previous_bytes = os.path.getsize(dataset_path)
                    _a3_allocation_admit(previous_bytes * 256 + 16 * 1024**2)
                    if dataset_path.endswith('csv') and "~$" not in dataset_path:
                        df = pd.read_csv(dataset_path)
                    elif dataset_path.endswith('xlsx') and "~$" not in dataset_path:
                        df = pd.read_excel(dataset_path)

                    _check_stored_ticker_identity(df, catalogue_keys)
                    print("Записей до обновления: {}".format(len(df)))

                    # убираем ненужные колонки теханализа - их потом с нуля пересчитаем
                    ## ПРОВЕРИТЬ ЧТО ЕСЛИ ЭТО НЕ ДЕЛАТЬ
                    columns = df.columns
                    white_list_columns = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end',
                        'ticker']
                    columns_to_remove = [i for i in columns if i not in white_list_columns]
                    columns_to_remove
                    df.drop(columns_to_remove, axis=1,inplace=True)
                    _a3_charge('rows', len(df))
                    frames = []
                    _a3_dataset_append(frames, df)
                    ticker_list_actual_dataset = set(df['ticker'])
                    # df.head(2)

                    # для каждого тикера выбираем последнюю дату за которую есть выгрузка
                    df_last_date = df.sort_values(by=['end']).drop_duplicates(subset='ticker', keep='last')
                    df_last_date = df_last_date.loc[:,['end','ticker']]
                    df_last_date.drop_duplicates(inplace=True)
                    df_last_date.reset_index(inplace=True,drop=True)

                    # оставляем только тикеры, которые выгружались в последние 30 дней (чтобы не брать тикеры, которые делистили)
                    filter_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
                    df_last_date = df_last_date[df_last_date['end'] >= filter_date]



                    # обновление текущих данных
                    for i in range(0,len(df_last_date)):
                        end_date = df_last_date.iloc[i]['end']
                        ticker_in = df_last_date.iloc[i]['ticker']
                        active_fields = dict(dataset_id=filename_j, interval=interval,
                                             instrument=ticker_in, file=filename_j + '.csv')
                        start_date = today
                        ticker_type = lookup[lookup['TRADE_CODE'] == ticker_in]['SUPERTYPE'].values[0]

                        start_date_mx = start_date.date().isoformat()
                        end_date_mx = datetime.datetime.strptime(end_date,'%Y-%m-%d %H:%M:%S').date().isoformat()

                        _a1_event(logging_context, 'INFO', 'a1_instrument_started',
                            dict(dataset_id=filename_j, interval=interval, instrument=ticker_in, file=filename_j + '.csv'))
                        query_started = time.perf_counter()
                        df_ticker = _a3_history(ticker_in, ticker_type, end_date_mx, start_date_mx, interval,
                                              logging_context=logging_context)
                        _a1_event(logging_context, 'INFO', 'a1_instrument_returned',
                            dict(active_fields, outcome='RETURNED',
                                 duration_ms=max(0, (time.perf_counter() - query_started) * 1000),
                                 counts=_a1_counts(len(df_ticker), len(df_ticker), 1)))
                        _a3_dataset_append(frames, df_ticker)
                        if len(df_ticker):
                            ticker_list_actual_dataset.update(df_ticker['ticker'])


                    ### Проверка не появилось ли новых тикеров с момента последнего обновления

                    ticker_list_actual = all_stocks_ru['TRADE_CODE'].to_list()
                    delta = [ticker for ticker in ticker_list_actual if ticker not in ticker_list_actual_dataset]
                    if len(delta) > 0:
                        for t in range (0,len(delta)):
                            ticker_in = delta[t]
                            active_fields = dict(dataset_id=filename_j, interval=interval,
                                                 instrument=ticker_in, file=filename_j + '.csv')
                            start_date_mx = today.date().isoformat()
                            end_date_mx = _a3_calendar_anniversary(today.date(), 1).isoformat()
                            ticker_type = lookup[lookup['TRADE_CODE'] == ticker_in]['SUPERTYPE'].values[0]

                            _a1_event(logging_context, 'INFO', 'a1_instrument_started',
                                dict(dataset_id=filename_j, interval=interval, instrument=ticker_in, file=filename_j + '.csv'))
                            query_started = time.perf_counter()
                            df_ticker = _a3_history(ticker_in, ticker_type, end_date_mx, start_date_mx, interval,
                                                  logging_context=logging_context)
                            _a1_event(logging_context, 'INFO', 'a1_instrument_returned',
                                dict(active_fields, outcome='RETURNED',
                                     duration_ms=max(0, (time.perf_counter() - query_started) * 1000),
                                     counts=_a1_counts(len(df_ticker), len(df_ticker), 1)))
                            _a3_dataset_append(frames, df_ticker)

                    df = _a3_dataset_union(frames)
                    df.sort_values(by=['ticker','begin'],inplace=True)
                    df.drop_duplicates(inplace=True)
                    df.reset_index(inplace=True,drop=True)
                    _a3_checkpoint()
                    # Inputs прошли audit; не держим предыдущий dataset до нового read.
                    frames.clear()
                    df_ticker = df_last_date = None
                    print("Записей после обновления: {}".format(len(df)))

                    active_fields = dict(dataset_id=filename_j, interval=interval, file=filename_j + '.xlsx')
                    if len(df) > 0 and len(df) < 1048576: df.to_excel(('{}/datasets/{}'.format(current_path,filename_j + '.xlsx')),index = False)
                    active_fields = dict(dataset_id=filename_j, interval=interval, file=filename_j + '.csv')
                    if len(df) > 0: df.to_csv(('{}/datasets/{}'.format(current_path, filename_j + '.csv')),index = False)
                    df = None
        _a1_event(logging_context, 'INFO', 'a1_data_update_returned',
            dict(outcome='RETURNED',
                 duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
    except BaseException as error:
        _a1_failure(logging_context, error,
            dict(active_fields, duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
        raise

# %%
@_a3_operation('run')
def main(current_path, force_reload = False, *, logging_context=None):
    global exception_list
    global config

    operation_started = time.perf_counter()
    active_fields = {}
    _a1_event(logging_context, 'INFO', 'a1_main_started', active_fields)
    try:
        all_stocks_ru = moex_tickerlists (current_path, logging_context=logging_context)
        input_rows = len(all_stocks_ru)
        all_stocks_ru = _prepare_ticker_catalogue(all_stocks_ru)
        _a1_event(logging_context, 'INFO', 'a1_catalogue_prepared',
            dict(counts=_a1_counts(input_rows, len(all_stocks_ru), len(all_stocks_ru))))

        # Builder пишет XLSX, затем CSV; конкретный failed writer здесь неизвестен.
        active_fields = dict(file=None)
        _a1_event(logging_context, 'INFO', 'a1_metadata_started', active_fields)
        metadata_started = time.perf_counter()
        tickers_dates = build_tickers_dates(all_stocks_ru, current_path)
        _a1_event(logging_context, 'INFO', 'a1_metadata_returned',
            dict(file='ticker_lists/tickers_dates.csv', outcome='RETURNED',
                 duration_ms=max(0, (time.perf_counter() - metadata_started) * 1000)))
        active_fields = {}

        if force_reload == True: ## Если нужно с нуля перевыгрузить данные, то это этот необязательный параметр нужно передать как True
            _a1_event(logging_context, 'INFO', 'a1_force_reload', dict(outcome='FORCE_RELOAD'))
            for k in range(0, len(config)):
                full_reload(all_stocks_ru, config[k]['interval'], config[k]['years'], config[k]['filename'],config[k]['word'], current_path, tickers_dates,
                            logging_context=logging_context)

        else:
            data_update(config,current_path, all_stocks_ru, tickers_dates, logging_context=logging_context)


        exception_list = list(set(exception_list)) #дедупликация
        print("Пропущено тикеров при разных интервалах: {}".format(len(exception_list)))
        _a1_event(logging_context, 'INFO', 'a1_main_returned',
            dict(active_fields, outcome='RETURNED',
                 duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
    except BaseException as error:
        _a1_failure(logging_context, error,
            dict(active_fields, duration_ms=max(0, (time.perf_counter() - operation_started) * 1000)))
        raise

# %%
if __name__ == "__main__":
    main(current_path, force_reload = False)
    # main(current_path, force_reload = True)


