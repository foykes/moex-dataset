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
#     display_name: base
#     language: python
#     name: python3
# ---

# %%
import pandas as pd, sys
import urllib.request, urllib.parse, urllib.error, json
import re, time, socket, ssl, errno, math
from datetime import date, timezone
from email.utils import parsedate_to_datetime

current_path = sys.path[0]

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}

divs_all = []


def _network_clock():
    return time.monotonic()


def _network_wall_clock():
    return time.time()


def _retry_wait(delay):
    time.sleep(delay)


def _check_budget(deadline, last_error=None):
    remaining = deadline - _network_clock()
    if remaining <= 0:
        raise TimeoutError('DIVIDEND_BUDGET_EXHAUSTED: бюджет получения дивидендов исчерпан') from last_error
    return remaining


def _network_errors(error):
    # URLError.reason бывает строкой, исключением и даже циклической цепочкой.
    pending, seen = [(error, 0)], set()
    while pending and len(seen) < 32:
        current, depth = pending.pop(0)
        if not isinstance(current, Exception) or id(current) in seen or depth > 8:
            continue
        seen.add(id(current))
        yield current
        reason = current.reason if isinstance(current, urllib.error.URLError) else None
        pending.append((reason, depth + 1))
        pending.append((current.__cause__, depth + 1))
        if current.__cause__ is None and not current.__suppress_context__:
            pending.append((current.__context__, depth + 1))


def _network_policy(error):
    reset_codes = {errno.ECONNRESET, errno.ECONNABORTED}
    reset_codes.update(value for value in (
        getattr(socket, 'WSAECONNRESET', None), getattr(socket, 'WSAECONNABORTED', None))
        if value is not None)
    # Текущий typed failure важнее исторического cause/context. HTTPError
    # проверяется до URLError/OSError; TLS и DNS не угадываем по тексту.
    for current in _network_errors(error):
        if isinstance(current, urllib.error.HTTPError):
            if current.code in (401, 403):
                return 'ACCESS', False, 'HTTP_ACCESS'
            if current.code == 429:
                return 'QUOTA_429', True, 'HTTP_429'
            return 'SOURCE', current.code in (500, 502, 503, 504), 'HTTP_STATUS'
        if isinstance(current, ssl.SSLError):
            return 'SOURCE', False, 'TLS_FAILURE'
        if isinstance(current, socket.gaierror):
            return 'SOURCE', current.errno == socket.EAI_AGAIN, 'DNS_FAILURE'
        if isinstance(current, TimeoutError):
            return 'TIMEOUT', True, 'NETWORK_TIMEOUT'
        if (isinstance(current, (ConnectionResetError, ConnectionAbortedError))
                or isinstance(current, OSError) and (current.errno in reset_codes
                    or getattr(current, 'winerror', None) in reset_codes)):
            return 'SOURCE', True, 'CONNECTION_INTERRUPTED'
        if not isinstance(current, OSError):
            return 'SOURCE', False, 'PERMANENT_SOURCE_FAILURE'
    return 'SOURCE', False, 'PERMANENT_SOURCE_FAILURE'


def _close_failed_response(response, error):
    # Закрываем и HTTPError.fp после failed OPEN. Вторичный close не заменяет
    # первичную ошибку READ/OPEN; сами close calls не имеют strict deadline A1.
    closed = set()
    for current in _network_errors(error):
        if not isinstance(current, urllib.error.HTTPError) or id(current) in closed:
            continue
        closed.add(id(current))
        if current.fp is not None:
            closed.add(id(current.fp))
        try:
            current.close()
        except BaseException:
            pass
    if response is not None and id(response) not in closed:
        try:
            response.close()
        except BaseException:
            pass


def _retry_delay(error, attempt):
    delay = attempt  # Только 1 s / 2 s, не после третьей попытки.
    if _network_policy(error)[2] not in ('HTTP_429', 'HTTP_STATUS'):
        return delay
    http_error = next((current for current in _network_errors(error)
                      if isinstance(current, urllib.error.HTTPError)), None)
    if http_error is None:
        return delay
    try:
        value = http_error.headers.get('Retry-After')
        if type(value) is not str:
            return delay
        value = value.strip()
        if re.fullmatch(r'[0-9]+', value):
            digits = value.lstrip('0') or '0'
            # Не превращаем огромный корректный delay в invalid/fallback.
            if len(digits) > 2 or len(digits) == 2 and digits > '10':
                return None
            server_delay = int(digits)
        else:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            server_delay = parsed.timestamp() - _network_wall_clock()
    except Exception:
        return delay
    if server_delay > 10:
        return None  # Не повторять раньше разрешённого сервером момента.
    return max(delay, server_delay)


def _decode_json(raw):
    return json.loads(raw)


def _request_phase_marker(state):
    phase = state.get('phase')
    if type(phase) is str and phase in ('OPEN', 'READ', 'CLOSE', 'DECODE'):
        return '; request_phase=' + phase
    return ''


def _retry_event(logging_context, error, instrument, endpoint, code, state):
    if logging_context is None:
        return True
    try:
        import run_logging
        record = run_logging.make_error(logging_context, error, 'dividends', code=code)
        record.update(category=state['category'], retryable=True, final=False,
                      attempt=state['attempt'], endpoint=endpoint)
        record['null_reasons'].pop('attempt', None)
        record['null_reasons'].pop('endpoint', None)
        if type(instrument) is str:
            record['instrument'] = instrument
            record['null_reasons'].pop('instrument', None)
        _dividend_event(logging_context, 'WARNING', 'dividend_request_retry',
            'Запланирован повтор запроса дивидендов: ' + state['reason'] + _request_phase_marker(state), instrument,
            outcome='RETRY', error=record, retries={'attempt': state['attempt'], 'limit': 3},
            duration_ms=max(0, (_network_clock() - state['started']) * 1000))
        return True
    except BaseException:
        return False


def _request_json(query, deadline, logging_context, instrument, code, state):
    state.clear()
    state.update(started=_network_clock(), category='SOURCE', reason='', attempt=0)
    last_network_error = None
    for attempt in range(1, 4):
        remaining = _check_budget(deadline, last_network_error)
        state.update(attempt=attempt, phase='OPEN', category='SOURCE', reason='')
        response = None
        try:
            response = urllib.request.urlopen(query, timeout=min(10, remaining))
            state['phase'] = 'READ'
            raw = response.read()
        except Exception as error:
            last_network_error = error
            _close_failed_response(response, error)
            category, retryable, reason = _network_policy(error)
            state.update(category=category, reason=reason)
            if not retryable:
                raise
            if attempt == 3:
                state['reason'] = 'ATTEMPTS_EXHAUSTED'
                raise
            delay = _retry_delay(error, attempt)
            if delay is None:
                state['reason'] = 'RETRY_AFTER_REFUSED'
                raise
            state['reason'] = 'BUDGET_EXHAUSTED'
            if delay >= _check_budget(deadline, error):
                raise TimeoutError('DIVIDEND_BUDGET_EXHAUSTED: нет бюджета для следующей попытки') from error
            state['reason'] = reason
            if not _retry_event(logging_context, error, instrument, query, code, state):
                state['reason'] = 'RETRY_LOGGING_FAILED'
                raise
            state['reason'] = 'BUDGET_EXHAUSTED'
            if delay >= _check_budget(deadline, error):
                raise TimeoutError('DIVIDEND_BUDGET_EXHAUSTED: нет бюджета для следующей попытки') from error
            _retry_wait(delay)
            _check_budget(deadline, error)
            continue
        # CLOSE-only, JSON/Unicode и parsing не входят в retry-зону.
        state.update(phase='CLOSE', category='IO')
        response.close()
        _check_budget(deadline)
        state.update(phase='DECODE', category='SOURCE')
        data = _decode_json(raw)
        _check_budget(deadline)
        state['phase'] = ''  # Parser/barrier failures не являются DECODE failures.
        return data


# %%
### Функция выгрузки дивидендов по ISIN

def _select_secid(data, isin):
    # Search может вернуть похожие бумаги и переставить колонки. Shortname
    # не является SECID; принимать можно только точное совпадение ISIN.
    if type(data) is not dict or type(data.get('securities')) is not dict:
        raise ValueError('DIVIDEND_SEARCH_SCHEMA_INVALID: отсутствует таблица securities')
    columns = data['securities'].get('columns')
    rows = data['securities'].get('data')
    if (type(columns) is not list or type(rows) is not list
            or any(type(column) is not str for column in columns)
            or columns.count('secid') != 1 or columns.count('isin') != 1):
        raise ValueError('DIVIDEND_SEARCH_SCHEMA_INVALID: нужны единственные колонки secid и isin')
    secid_column = columns.index('secid')
    isin_column = columns.index('isin')
    secid_list = []
    matching_rows = 0
    for row in rows:
        if type(row) is not list or len(row) != len(columns):
            raise ValueError('DIVIDEND_SEARCH_SCHEMA_INVALID: длина строки не соответствует columns')
        if type(row[isin_column]) is not str or row[isin_column] != isin:
            continue
        matching_rows += 1
        secid = row[secid_column]
        # Не навязываем stocks-only шаблон кода: допустимый SECID остаётся
        # непрозрачным сегментом URL, но разделители/управляющие символы опасны.
        if (type(secid) is not str or not secid or secid in ('.', '..')
                or any(char.isspace() or ord(char) < 32 or 127 <= ord(char) <= 159
                       or char in '/\\?#' for char in secid)):
            raise ValueError('DIVIDEND_SECID_INVALID: недопустимый SECID точного совпадения')
        try:
            secid.encode('utf-8')
        except UnicodeEncodeError:
            raise ValueError('DIVIDEND_SECID_INVALID: SECID не кодируется в UTF-8') from None
        if secid not in secid_list:
            secid_list.append(secid)
    if not secid_list:
        raise ValueError('DIVIDEND_IDENTITY_NOT_FOUND: точный ISIN не найден')
    # Несколько разных кодов нельзя считать алиасами без evidence/контракта.
    # Одинаковые строки search — один кандидат, а не несколько выплат.
    if len(secid_list) != 1:
        raise ValueError('DIVIDEND_IDENTITY_AMBIGUOUS: точному ISIN соответствуют разные SECID')
    return secid_list[0], matching_rows


def _parse_dividend_rows(data, isin, ticker):
    # Это проверка структуры одного ответа SECID, не доказательство полноты
    # источника или качества даты/суммы/валюты. Missing block сохраняет прежний
    # KeyError('dividends'); HTTP 200 и найденный SECID не заменяют этот блок.
    if type(data) is not dict:
        raise ValueError('DIVIDEND_SOURCE_SCHEMA_INVALID: ответ должен быть объектом')
    table = data['dividends']
    if type(table) is not dict:
        raise ValueError('DIVIDEND_SOURCE_SCHEMA_INVALID: dividends должен быть объектом')
    columns = table.get('columns')
    rows = table.get('data')
    if (type(columns) is not list or type(rows) is not list
            or any(type(column) is not str or not column.strip() for column in columns)
            or len(set(columns)) != len(columns)
            or any(name not in columns for name in ('registryclosedate', 'value', 'currencyid'))):
        raise ValueError('DIVIDEND_SOURCE_SCHEMA_INVALID: нужны уникальные columns и список data')
    date_column = columns.index('registryclosedate')
    value_column = columns.index('value')
    currency_column = columns.index('currencyid')
    prepared_rows = []
    for row in rows:
        if type(row) is not list or len(row) != len(columns):
            raise ValueError('DIVIDEND_SOURCE_SCHEMA_INVALID: длина строки не соответствует columns')
        # Сохраняем decoded values, порядок и отдельные выплаты. Не округляем
        # малые/нулевые суммы, не исправляем 2111 или unknown currency догадками.
        prepared_rows.append([isin, ticker, row[date_column], row[value_column], row[currency_column]])
    return prepared_rows


def _validate_dividend_rows(rows):
    # Локальный профиль не подтверждает source semantics, precision или полноту.
    for ordinal, row in enumerate(rows):
        dt, value, currency = row[2:]
        rule, field = None, 'dt'
        if type(dt) is not str:
            rule = 'DATE_TYPE'
        elif re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', dt) is None:
            rule = 'DATE_SHAPE'
        else:
            try:
                date(int(dt[:4]), int(dt[5:7]), int(dt[8:]))
            except ValueError:
                rule = 'DATE_CALENDAR'
        if rule is None:
            field = 'value'
            if type(value) not in (int, float):
                rule = 'VALUE_TYPE'
            elif type(value) is float and not math.isfinite(value):
                rule = 'VALUE_NONFINITE'
        if rule is None:
            field = 'currency'
            if type(currency) is not str:
                rule = 'CURRENCY_TYPE'
            elif not currency.strip():
                rule = 'CURRENCY_BLANK'
            elif re.search(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', currency):
                rule = 'CURRENCY_CONTROL'
        if rule is not None:
            # Только закрытые метки и ordinal; исходные значения не выводятся.
            raise ValueError('DIVIDEND_SOURCE_SEMANTIC_INVALID: rule={}; field={}; row={}'.format(
                rule, field, ordinal)) from None


def _dividend_event(logging_context, level, event, message, ticker=None,
                    *, counts=None, outcome=None, duration_ms=None, error=None, file=None, retries=None):
    if logging_context is None:
        return
    import run_logging
    fields = {'stage': 'dividends', 'instrument': ticker if type(ticker) is str else None,
              'outcome': outcome}
    if counts is not None:
        fields['counts'] = counts
    if duration_ms is not None:
        fields['duration_ms'] = duration_ms
    if error is not None:
        fields['error'] = error
    if file is not None:
        fields['file'] = file
    if retries is not None:
        fields['retries'] = retries
    receipt = run_logging.emit_event(logging_context, level, event, message, fields)
    # INFO progress подтверждается barrier позже; confirmed=False здесь штатно.
    if (not receipt['accepted'] or level == 'ERROR' and not receipt['confirmed']
            or not run_logging.check_logging_health(logging_context)['healthy']):
        raise RuntimeError('LOGGING_INCOMPLETE')


def _dividend_failure(logging_context, error, ticker, endpoint, code,
                      *, event='dividend_loader_failed', outcome='FAILED', request_state=None):
    if logging_context is None:
        return
    try:
        import run_logging
        record = run_logging.make_error(logging_context, error, 'dividends', code=code)
        record['category'] = 'QUALITY' if code in (
            'DIVIDEND_ISIN_INVALID', 'DIVIDEND_SCOPE_EMPTY_UNVERIFIED',
            'DIVIDEND_EMPTY_RELEASE_BLOCKED', 'DIVIDEND_SOURCE_SEMANTIC_INVALID') else 'SOURCE'
        if code == 'LOGGING_INCOMPLETE':
            record['category'] = 'IO'
        elif code == 'DIVIDEND_BUDGET_EXHAUSTED':
            record['category'] = 'TIMEOUT'
        elif request_state and code in ('DIVIDEND_SEARCH_FAILED', 'DIVIDEND_SOURCE_REQUEST_FAILED'):
            record['category'] = request_state['category']
        record['retryable'] = False
        record['final'] = True
        # Endpoint входит только в существующий error envelope, не event.fields.
        for name, value in (('instrument', ticker), ('endpoint', endpoint)):
            if type(value) is str:
                record[name] = value
                record['null_reasons'].pop(name, None)
        retries, duration_ms = None, None
        message = 'Выгрузка дивидендов прервана: ' + code
        if request_state and request_state['attempt']:
            record['attempt'] = request_state['attempt']
            record['null_reasons'].pop('attempt', None)
            retries = {'attempt': request_state['attempt'], 'limit': 3}
            duration_ms = max(0, (_network_clock() - request_state['started']) * 1000)
            message += '; ' + request_state['reason']
            message += _request_phase_marker(request_state)
        _dividend_event(logging_context, 'ERROR', event, message, ticker,
            outcome=outcome, error=record, retries=retries, duration_ms=duration_ms)
    except BaseException:
        # Уже есть первичная ошибка. Вторичный отказ диагностики не заменяет
        # её; вызывающий stage также получает исходное исключение/nonzero.
        try:
            print('Не удалось записать диагностику дивидендов.', file=sys.stderr)
        except BaseException:
            pass


def div_loader(isin, ticker, *, logging_context=None):
    # Самостоятельный loader сохраняет прежний публичный накопитель и None.
    # main передаёт свой отдельный список и не меняет этот legacy state.
    return _div_loader(isin, ticker, _network_clock() + 600,
                       rows_target=divs_all, logging_context=logging_context)


def _div_loader(isin, ticker, source_deadline, *, rows_target, logging_context=None):
    query = None
    instrument = ticker
    failure_code = 'DIVIDEND_ISIN_INVALID'
    started = time.perf_counter()
    request_state = {}
    try:
        # Не превращаем NaN в строку "nan" и не исправляем identity догадками.
        if type(isin) is not str or re.fullmatch(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', isin) is None:
            raise ValueError('DIVIDEND_ISIN_INVALID: требуется ISIN из 12 символов')
        query = 'https://iss.moex.com/iss/securities.json?' + urllib.parse.urlencode(
            {'q': isin, 'iss.meta': 'off'})
        failure_code = 'DIVIDEND_SEARCH_FAILED'
        operation_deadline = min(source_deadline, _network_clock() + 35)
        _dividend_event(logging_context, 'INFO', 'dividend_identity_search_started',
            'Поиск точного ISIN', ticker, outcome='STARTED')
        data = _request_json(query, operation_deadline, logging_context, ticker,
                             failure_code, request_state)
        secid, matching_rows = _select_secid(data, isin)
        _check_budget(operation_deadline)
        instrument = secid
        counts = {'rows_received': len(data['securities']['data']),
                  'rows_accepted': matching_rows, 'rows_quarantined': None,
                  'pages': 1, 'instruments': 1,
                  'null_reasons': {'rows_quarantined': 'NOT_EVALUATED'}}
        _dividend_event(logging_context, 'INFO', 'dividend_identity_selected',
            'Совпадений ISIN: {}; уникальных SECID: 1'.format(matching_rows),
            secid, counts=counts, outcome='SELECTED')
        _check_budget(operation_deadline)

        query = 'https://iss.moex.com/iss/securities/{}/dividends.json'.format(
            urllib.parse.quote(secid, safe=''))
        failure_code = 'DIVIDEND_SOURCE_REQUEST_FAILED'
        operation_deadline = min(source_deadline, _network_clock() + 35)
        request_state = {}
        _dividend_event(logging_context, 'INFO', 'dividend_request_started',
            'Запрос дивидендов выбранного SECID', secid, outcome='STARTED')
        data = _request_json(query, operation_deadline, logging_context, secid,
                             failure_code, request_state)
        failure_code = 'DIVIDEND_SOURCE_SCHEMA_INVALID'
        rows = _parse_dividend_rows(data, isin, ticker)
        _check_budget(operation_deadline)
        failure_code = 'DIVIDEND_SOURCE_SEMANTIC_INVALID'
        _validate_dividend_rows(rows)
        _check_budget(operation_deadline)
        counts = {'rows_received': len(rows), 'rows_accepted': len(rows),
                  'rows_quarantined': None, 'pages': 1, 'instruments': 1,
                  'null_reasons': {'rows_quarantined': 'NOT_EVALUATED'}}
        _dividend_event(logging_context, 'INFO', 'dividend_loader_returned',
            ('Локально проверено событий: {}; source_semantics=NOT_VERIFIED; '
             'precision=NOT_VERIFIED; полнота не проверена').format(len(rows)), secid,
            counts=counts, outcome='RETURNED' if rows else 'VALID_EMPTY',
            duration_ms=(time.perf_counter() - started) * 1000)
        if logging_context is not None:
            import run_logging
            receipt = run_logging.flush_logging(logging_context)
            if not receipt['confirmed'] or not run_logging.check_logging_health(logging_context)['healthy']:
                raise RuntimeError('LOGGING_INCOMPLETE')
        _check_budget(operation_deadline)
        # Один повреждённый ответ или отказ barrier не оставляет его первые
        # строки в переданном накопителе. Успешные предыдущие ответы сохраняются
        # в нём; каждый main владеет отдельным списком только своего вызова.
        rows_target.extend(rows)
    except Exception as error:
        if error.args and type(error.args[0]) is str:
            code = error.args[0].split(':', 1)[0]
            if code in ('DIVIDEND_ISIN_INVALID', 'DIVIDEND_SEARCH_SCHEMA_INVALID',
                        'DIVIDEND_IDENTITY_NOT_FOUND', 'DIVIDEND_IDENTITY_AMBIGUOUS',
                        'DIVIDEND_SECID_INVALID', 'LOGGING_INCOMPLETE', 'DIVIDEND_BUDGET_EXHAUSTED',
                        'DIVIDEND_SOURCE_SEMANTIC_INVALID'):
                failure_code = code
        _dividend_failure(logging_context, error, instrument, query, failure_code,
                          request_state=request_state)
        raise


# %%
def _source_checkpoint(deadline, logging_context):
    try:
        _check_budget(deadline)
    except TimeoutError as error:
        _dividend_failure(logging_context, error, None, None, 'DIVIDEND_BUDGET_EXHAUSTED',
                          event='dividend_collection_failed')
        raise


def main(*, logging_context=None):
    source_deadline = _network_clock() + 600
    run_rows = []
    _dividend_event(logging_context, 'INFO', 'dividend_collection_started',
        'Подготовка списка инструментов для дивидендов', outcome='STARTED')
    _source_checkpoint(source_deadline, logging_context)
    ## Подготовка списка для чего будут выгружаться дивиденды
    path = current_path + "/datasets/ticker_lists/moex_full.xlsx"
    df = pd.read_excel(path)
    df_isin = df[['TRADE_CODE','ISIN']]
    df_isin = df_isin.dropna(how='all')
    df_isin.drop_duplicates(keep='first', inplace=True)
    df_isin.reset_index(drop=True, inplace=True)
    _source_checkpoint(source_deadline, logging_context)
    if len(df_isin) == 0:
        error = ValueError('DIVIDEND_SCOPE_EMPTY_UNVERIFIED: список инструментов пуст')
        _dividend_failure(logging_context, error, None, None,
            'DIVIDEND_SCOPE_EMPTY_UNVERIFIED',
            event='dividend_collection_failed', outcome='BLOCKED')
        raise error

    for i in range(0, len(df_isin)):
        _source_checkpoint(source_deadline, logging_context)
        isin = df_isin['ISIN'][i]
        ticker = df_isin['TRADE_CODE'][i]
        _div_loader(isin, ticker, source_deadline, rows_target=run_rows,
                    logging_context=logging_context)
    _source_checkpoint(source_deadline, logging_context)
    # Владелец выбрал BLOCKED + previous для пустого выпуска. Здесь проверяются
    # только строки текущего вызова; standalone div_loader не влияет на gate.
    if len(run_rows) == 0:
        error = ValueError('DIVIDEND_EMPTY_RELEASE_BLOCKED: текущий обход не получил событий')
        _dividend_failure(logging_context, error, None, None,
            'DIVIDEND_EMPTY_RELEASE_BLOCKED',
            event='dividend_collection_failed', outcome='BLOCKED')
        raise error

    print('Выгружено записей о дивидендах: {}'.format(len(run_rows)))

    df_divs_all = pd.DataFrame(run_rows, columns=['ISIN','TRADE_CODE','dt','value','currency'])
    # Размер экспортного входа этого вызова не доказывает уникальность событий,
    # полноту источника или готовность публикации.
    counts = {'rows_received': None, 'rows_accepted': len(df_divs_all),
              'rows_quarantined': None, 'pages': None, 'instruments': len(df_isin),
              'null_reasons': {'rows_received': 'NOT_MEASURED_FOR_EXPORT_INPUT',
                              'rows_quarantined': 'NOT_EVALUATED', 'pages': 'NOT_APPLICABLE'}}
    _dividend_event(logging_context, 'INFO', 'dividend_export_input',
        'Строк в экспортном входе: {}'.format(len(df_divs_all)),
        counts=counts, outcome='INPUT_PREPARED', file='datasets/dividends/all')

    path = current_path + "/datasets/dividends/" + "all"
    # Writers остаются вне acquisition budget: этот gate предшествует обоим,
    # но не обеспечивает атомарность файлов или ограничение времени записи.
    _source_checkpoint(source_deadline, logging_context)
    if len(df_divs_all) > 0: df_divs_all.to_excel(path + ".xlsx",index = False)
    if len(df_divs_all) > 0: df_divs_all.to_csv(path + ".csv",index = False)
    _dividend_event(logging_context, 'INFO', 'dividend_collection_returned',
        'Этап дивидендов вернулся; проверка публикации не выполнялась', outcome='RETURNED')

# %%
if __name__ == "__main__":
    main()
