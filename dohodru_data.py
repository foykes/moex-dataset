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
import requests, pandas as pd, sys
from bs4 import BeautifulSoup
import errno, hashlib, re, socket, ssl, time
from datetime import timezone
from email.utils import parsedate_to_datetime
from urllib3 import exceptions as urllib3_errors
from io import BytesIO
from types import SimpleNamespace
from urllib.parse import urljoin, urlsplit

df_overview_full = pd.DataFrame()
df_years_full = pd.DataFrame()
df_each_full = pd.DataFrame()

current_path = sys.path[0]

url = 'https://www.dohod.ru/ik/analytics/dividend'

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}


def _read_dohod_html(raw):
    with BytesIO(raw) as buffer:
        # Generic reader сохраняет bytes и выбор encoding обеими parser-ветками.
        reader = SimpleNamespace(
            read=buffer.read,
            seek=buffer.seek,
            seekable=buffer.seekable,
            tell=buffer.tell,
        )
        return pd.read_html(reader, encoding=None)


def _network_clock():
    return time.monotonic()


def _network_wall_clock():
    return time.time()


def _retry_wait(delay):
    time.sleep(delay)


def _check_budget(deadline, last_error=None, state=None):
    checked = _network_clock()
    remaining = deadline - checked
    if state is not None:
        state['last_checked'] = checked
    if remaining <= 0:
        if state is not None:
            state.update(category='TIMEOUT', reason='BUDGET_EXHAUSTED', failure_kind='legacy')
        raise TimeoutError('DOHOD_BUDGET_EXHAUSTED: бюджет получения данных исчерпан') from last_error
    return remaining


def _network_errors(error):
    # Только typed wrappers и explicit cause; исторический context не policy.
    pending, seen = [(error, 0)], set()
    while pending and len(seen) < 32:
        current, depth = pending.pop(0)
        if not isinstance(current, Exception) or id(current) in seen or depth > 8:
            continue
        seen.add(id(current))
        yield current
        pending.append((current.__cause__, depth + 1))
        if isinstance(current, (requests.RequestException, urllib3_errors.HTTPError)):
            pending.extend((arg, depth + 1) for arg in current.args if isinstance(arg, Exception))
        if isinstance(current, urllib3_errors.MaxRetryError):
            pending.append((current.reason, depth + 1))
        if isinstance(current, urllib3_errors.NameResolutionError):
            pending.append((current._reason, depth + 1))
        if isinstance(current, urllib3_errors.ProxyError):
            pending.append((current.original_error, depth + 1))


def _network_policy(error):
    reset_codes = {errno.ECONNRESET, errno.ECONNABORTED, 10054, 10053}
    for current in _network_errors(error):
        # Текущая typed ошибка важнее её исторической причины. HTTP headers
        # разрешены только для текущего HTTPError, не вложенного старого HTTP.
        if isinstance(current, requests.HTTPError):
            if current is not error:
                continue
            status = current.response.status_code if current.response is not None else None
            if status in (401, 403):
                return 'ACCESS', False, 'HTTP_ACCESS'
            if status == 429:
                return 'QUOTA_429', True, 'HTTP_429'
            return 'SOURCE', status in (500, 502, 503, 504), 'HTTP_STATUS'
        if isinstance(current, (requests.exceptions.ProxyError, urllib3_errors.ProxyError)):
            return 'SOURCE', False, 'PROXY_FAILURE'
        if isinstance(current, (requests.exceptions.SSLError, urllib3_errors.SSLError, ssl.SSLError)):
            return 'SOURCE', False, 'TLS_FAILURE'
        if isinstance(current, socket.gaierror):
            return 'SOURCE', current.errno == socket.EAI_AGAIN, 'DNS_FAILURE'
        if isinstance(current, urllib3_errors.NameResolutionError):
            reason = current._reason
            return 'SOURCE', isinstance(reason, socket.gaierror) and reason.errno == socket.EAI_AGAIN, 'DNS_FAILURE'
        # NewConnectionError/NameResolutionError наследуют ConnectTimeoutError.
        if isinstance(current, urllib3_errors.NewConnectionError):
            continue  # Typed дети проходят тот же bounded walk, без reset depth.
        if isinstance(current, (requests.Timeout, urllib3_errors.TimeoutError, TimeoutError)):
            return 'TIMEOUT', True, 'NETWORK_TIMEOUT'
        if (isinstance(current, (ConnectionResetError, ConnectionAbortedError))
                or isinstance(current, OSError) and (current.errno in reset_codes
                    or getattr(current, 'winerror', None) in reset_codes)):
            return 'SOURCE', True, 'CONNECTION_INTERRUPTED'
        if isinstance(current, (requests.ConnectionError, requests.exceptions.ChunkedEncodingError,
                                urllib3_errors.MaxRetryError, urllib3_errors.ProtocolError)):
            continue
        return 'SOURCE', False, 'PERMANENT_SOURCE_FAILURE'
    return 'SOURCE', False, 'PERMANENT_SOURCE_FAILURE'


def _close_failed_response(response, error, closed=None):
    if closed is None:
        closed = {}
    responses = [response]
    responses.extend(current.response for current in _network_errors(error)
                     if isinstance(current, requests.RequestException))
    for current in responses:
        if current is None or id(current) in closed:
            continue
        closed[id(current)] = current  # Держим identity до конца attempt, без reuse id.
        try:
            current.close()
        except BaseException:
            pass  # Вторичный CLOSE не заменяет первичную OPEN/READ ошибку.


def _retry_delay(error, attempt):
    delay = attempt  # 1 s / 2 s; третья попытка не планирует wait.
    if not isinstance(error, requests.HTTPError) or error.response is None:
        return delay
    if error.response.status_code not in (429, 500, 502, 503, 504):
        return delay
    try:
        value = error.response.headers.get('Retry-After')
        if type(value) is not str:
            return delay
        value = value.strip()
        if re.fullmatch(r'[0-9]+', value):
            digits = value.lstrip('0') or '0'
            if len(digits) > 2 or len(digits) == 2 and digits > '10':
                return None  # Огромный integer не становится invalid/fallback.
            server_delay = int(digits)
        else:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            server_delay = parsed.timestamp() - _network_wall_clock()
    except Exception:
        return delay
    if server_delay > 10:
        return None  # Не повторяем раньше разрешённого сервером момента.
    return max(delay, server_delay)


def _dohod_event(logging_context, level, event, message, *, error=None, state=None, outcome=None):
    if logging_context is None:
        return
    import run_logging
    fields = {'stage': 'dohodru_data', 'outcome': outcome}
    if error is not None:
        fields['error'] = error
    if state is not None:
        fields['duration_ms'] = max(0, (_network_clock() - state['started']) * 1000)
        if state['attempt']:
            fields['retries'] = {'attempt': state['attempt'], 'limit': 3}
    try:
        receipt = run_logging.emit_event(logging_context, level, event, message, fields)
        if (not receipt['accepted'] or level == 'ERROR' and not receipt['confirmed']
                or not run_logging.check_logging_health(logging_context)['healthy']):
            raise RuntimeError('LOGGING_INCOMPLETE')
    except Exception as error:
        raise RuntimeError('LOGGING_INCOMPLETE') from error


def _error_record(logging_context, error, state, code, *, retryable=False, final=True):
    import run_logging
    record = run_logging.make_error(logging_context, error, 'dohodru_data', code=code)
    record.update(category=state['category'], retryable=retryable, final=final)
    # Requests HTTPError.args содержит server reason. Full F0 shape сохраняем,
    # но B diagnostics передаёт только фиксированные причины/phase, без args.
    record['message'] = code + '; ' + state['reason'] + _phase_marker(state)
    for name in ('endpoint', 'attempt'):
        value = state.get(name)
        if value:
            record[name] = value
            record['null_reasons'].pop(name, None)
    return record


def _phase_marker(state):
    phase = state.get('phase')
    return '; request_phase=' + phase if phase in ('OPEN', 'READ', 'CLOSE', 'DECODE') else ''


def _retry_event(logging_context, error, state):
    if logging_context is None:
        return True
    try:
        record = _error_record(logging_context, error, state, 'DOHOD_SOURCE_REQUEST_FAILED',
                               retryable=True, final=False)
        _dohod_event(logging_context, 'WARNING', 'dohod_request_retry',
            'Запланирован повтор запроса доход.ру: ' + state['reason'] + _phase_marker(state),
            error=record, state=state, outcome='RETRY')
        return True
    except BaseException:
        return False


def _safe_error_record(logging_context, state):
    # Новые C3 failures не пропускаем через make_error(raw): его args/stack
    # и legacy endpoint не являются безопасной проекцией semantic report.
    import run_logging
    code = state['failure_code']
    record = dict(category=state['category'], code=code, stage='dohodru_data',
                  retryable=False, final=True, message=code)
    record.update({name: None for name in run_logging.ERROR_NULLABLE})
    record.update(run_id=logging_context['run_id'], page=state.get('page_ref'),
                  attempt=state['attempt'] or None,
                  exception_type='RuntimeError' if code == 'LOGGING_INCOMPLETE' else 'ValueError')
    record['null_reasons'] = {name: 'NOT_AVAILABLE_OR_NOT_APPLICABLE'
                              for name in run_logging.ERROR_NULLABLE if record[name] is None}
    record['null_reasons']['endpoint'] = 'RAW_ENDPOINT_NOT_EMITTED'
    return record


def _failure(logging_context, error, state, *, export=False, coverage_report=None):
    if logging_context is None:
        return
    try:
        if state.get('failure_kind', 'legacy') != 'legacy':
            record = _safe_error_record(logging_context, state)
            _dohod_event(logging_context, 'ERROR', 'dohod_acquisition_failed',
                'Проверка данных доход.ру прервана: ' + record['code'],
                error=record, state=state, outcome='FAILED')
            return
        code = 'DOHOD_EXPORT_FAILED' if export else 'DOHOD_SOURCE_REQUEST_FAILED'
        if not export and state.get('reason') == 'BUDGET_EXHAUSTED':
            code = 'DOHOD_BUDGET_EXHAUSTED'
        elif not export and type(error) is RuntimeError and error.args == ('LOGGING_INCOMPLETE',):
            code = 'LOGGING_INCOMPLETE'
            state['category'] = 'IO'
        record = _error_record(logging_context, error, state, code)
        _dohod_event(logging_context, 'ERROR', 'dohod_export_failed' if export else 'dohod_acquisition_failed',
            'Выгрузка доход.ру прервана: ' + code + '; ' + state['reason'] + _phase_marker(state),
            error=record, state=state, outcome='FAILED')
    except BaseException:
        if coverage_report is not None:
            coverage_report['diagnostics_status'] = 'INCOMPLETE'
        try:
            print('Не удалось записать диагностику доход.ру.', file=sys.stderr)
        except BaseException:
            pass


def _logging_barrier(logging_context):
    if logging_context is None:
        return
    import run_logging
    try:
        receipt = run_logging.flush_logging(logging_context)
        if not receipt['confirmed'] or not run_logging.check_logging_health(logging_context)['healthy']:
            raise RuntimeError('LOGGING_INCOMPLETE')
    except Exception as error:
        raise RuntimeError('LOGGING_INCOMPLETE') from error


def _canonical_url(value, base=None):
    # Проверяем raw reference до urlsplit/urljoin: они могут убрать controls
    # или нормализовать dot segments раньше проверки допуска.
    if (type(value) is not str or not value or '\\' in value
            or any(ord(char) <= 32 or ord(char) >= 127 for char in value)):
        raise ValueError('URL_RAW_INVALID')
    if re.search(r'%(?![0-9a-fA-F]{2})', value):
        raise ValueError('URL_ESCAPE_INVALID')
    parts = None
    try:
        parts = urlsplit(value)
    except ValueError:
        pass
    if parts is None:
        raise ValueError('URL_AUTHORITY_INVALID')
    if parts.scheme and (parts.scheme != 'https' or not parts.netloc):
        raise ValueError('URL_SCHEME_NOT_ALLOWED')
    if parts.netloc and not re.fullmatch(r'www\.dohod\.ru(?::443)?', parts.netloc, re.I):
        raise ValueError('URL_HOST_NOT_ALLOWED')
    if parts.query:
        raise ValueError('URL_QUERY_NOT_ALLOWED')

    def unreserved(match):
        char = chr(int(match.group()[1:], 16))
        if not re.fullmatch(r'[A-Za-z0-9._~-]', char):
            raise ValueError('URL_ESCAPE_NOT_ALLOWED')
        return char

    path = re.sub(r'%[0-9a-fA-F]{2}', unreserved, parts.path)
    if any(segment in ('.', '..') for segment in path.split('/')):
        raise ValueError('URL_DOT_SEGMENT')
    reference = parts._replace(path=path, query='', fragment='').geturl()
    target = urlsplit(urljoin(base, reference) if base is not None else reference)
    if target.scheme != 'https':
        raise ValueError('URL_SCHEME_NOT_ALLOWED')
    if not re.fullmatch(r'www\.dohod\.ru(?::443)?', target.netloc, re.I):
        raise ValueError('URL_HOST_NOT_ALLOWED')
    overview_path = '/ik/analytics/dividend'
    if target.path not in (overview_path, overview_path + '/'):
        if not re.fullmatch(overview_path + r'/[A-Za-z0-9._~-]+', target.path):
            reason = ('URL_DETAIL_PATH_NOT_ALLOWED' if target.path.startswith(overview_path + '/')
                      else 'URL_PATH_NOT_ALLOWED')
            raise ValueError(reason)
        if '.pdf' in target.path.rsplit('/', 1)[-1].lower():
            raise ValueError('URL_PDF_NOT_ALLOWED')
    return 'https://www.dohod.ru' + target.path


def _url_kind(url):
    return 'overview' if url in (
        'https://www.dohod.ru/ik/analytics/dividend',
        'https://www.dohod.ru/ik/analytics/dividend/') else 'detail'


def _redirect_url(response, current, original, page_kind, visited, transitions, state):
    # response.headers уже объединяет повторные поля и теряет cardinality.
    locations = None
    try:
        locations = response.raw.headers.getlist('Location')
    except Exception:
        pass
    reason = None
    if type(locations) not in (list, tuple):
        reason = 'REDIRECT_LOCATION_UNAVAILABLE'
    elif len(locations) == 0:
        state['reason'] = 'HTTP_STATUS'
        raise requests.HTTPError('DOHOD_HTTP_STATUS_INVALID', response=response)
    elif len(locations) > 1:
        reason = 'REDIRECT_LOCATION_DUPLICATE'
    elif type(locations[0]) is not str or not locations[0]:
        reason = 'REDIRECT_LOCATION_INVALID'
    target = None
    if reason is None:
        try:
            target = _canonical_url(locations[0], current)
        except ValueError:
            reason = 'REDIRECT_LOCATION_INVALID'
    if reason is None:
        if _url_kind(target) != page_kind:
            reason = 'REDIRECT_ROLE_CHANGED'
        elif page_kind == 'detail' and target != original:
            reason = 'REDIRECT_DETAIL_IDENTITY_CHANGED'
        elif target in visited:
            reason = 'REDIRECT_LOOP'
        elif transitions >= 30:
            reason = 'REDIRECT_LIMIT'
    if reason is not None:
        state['reason'] = reason
        # Вне except: raw Location/header exception не попадает даже в context.
        raise ValueError(reason)
    return target


def _request_page(url, deadline, logging_context, state, *, page_kind, text=False):
    state['endpoint'] = None
    try:
        original = _canonical_url(url)
    except ValueError as error:
        state['reason'] = error.args[0]
        raise
    state['endpoint'] = original
    if page_kind not in ('overview', 'detail') or _url_kind(original) != page_kind:
        state['reason'] = 'URL_ROLE_NOT_ALLOWED'
        raise ValueError('URL_ROLE_NOT_ALLOWED')
    last_error = None
    for attempt in range(1, 4):
        _check_budget(deadline, last_error, state)
        state.update(attempt=attempt, phase='', category='SOURCE', reason='')
        response, session, retry_error = None, None, None
        session_closed = False
        closed = {}
        try:
            session = requests.Session()
            current, visited, transitions = original, {original}, 0
            while True:
                state.update(endpoint=current, phase='', category='SOURCE')
                prepared = session.prepare_request(requests.Request('GET', current, headers=headers))
                prepared_url = None
                try:
                    prepared_url = _canonical_url(prepared.url)
                except ValueError:
                    pass
                if prepared_url != current or prepared.url != current:
                    state['reason'] = 'PREPARED_URL_CHANGED'
                    raise ValueError('PREPARED_URL_CHANGED')
                settings = session.merge_environment_settings(prepared.url, {}, True, None, None)
                adapter = session.get_adapter(prepared.url)
                remaining = _check_budget(deadline, last_error, state)
                state['phase'] = 'OPEN'
                try:
                    # Session.send даже с allow_redirects=False читает 3xx body
                    # ради _next. Публичный adapter.send этого не делает.
                    state['sends'] = state.get('sends', 0) + 1
                    response = adapter.send(prepared, timeout=(min(10, remaining), min(10, remaining)),
                                            **settings)
                except BaseException as error:
                    retry_error = error
                    raise
                _check_budget(deadline, state=state)
                state['phase'] = ''
                redirect = response.status_code in (301, 302, 303, 307, 308)
                if redirect:
                    state['phase'] = 'OPEN'
                    target = _redirect_url(response, current, original, page_kind,
                                           visited, transitions, state)
                cookies_ok = True
                try:
                    requests.cookies.extract_cookies_to_jar(session.cookies, prepared, response.raw)
                except Exception:
                    cookies_ok = False
                if not cookies_ok:
                    state['reason'] = 'COOKIE_PROCESSING_FAILED'
                    raise ValueError('COOKIE_PROCESSING_FAILED')
                _check_budget(deadline, state=state)
                if redirect:
                    state.update(phase='CLOSE', category='IO')
                    closed[id(response)] = response
                    response.close()
                    _check_budget(deadline, state=state)
                    state.update(phase='', category='SOURCE')
                    _dohod_event(logging_context, 'INFO', 'dohod_redirect_followed',
                        'Разрешён переход HTTP доход.ру', state=state, outcome='RECEIVED')
                    _check_budget(deadline, state=state)
                    visited.add(target)
                    transitions += 1
                    current = target
                    continue
                state['phase'] = 'OPEN'
                try:
                    if not 200 <= response.status_code < 300:
                        response.raise_for_status()
                        raise requests.HTTPError('DOHOD_HTTP_STATUS_INVALID', response=response)
                    _check_budget(deadline, state=state)
                    state['phase'] = 'READ'
                    raw = response.content
                    _check_budget(deadline, state=state)
                except BaseException as error:
                    retry_error = error
                    raise
                break
            # CLOSE, cookies, preparation, cached decoding и diagnostics
            # находятся вне retry-классификации сетевого OPEN/READ.
            state.update(phase='CLOSE', category='IO')
            closed[id(response)] = response
            response.close()
            session_closed = True
            session.close()
            _check_budget(deadline, state=state)
            state.update(phase='DECODE', category='SOURCE')
            result = response.text if text else raw
            _check_budget(deadline, state=state)
            state['phase'] = ''
            _dohod_event(logging_context, 'INFO', 'dohod_request_received',
                'Получено тело HTTP-ответа доход.ру; качество и полнота не проверены',
                state=state, outcome='RECEIVED')
            _check_budget(deadline, state=state)
            return result, current
        except BaseException as error:
            _close_failed_response(response, error, closed)
            if session is not None and not session_closed:
                session_closed = True
                try:
                    session.close()
                except BaseException:
                    pass
            if (error is not retry_error or not isinstance(error, Exception)
                    or state['reason'] == 'BUDGET_EXHAUSTED'):
                raise
            last_error = error
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
            if delay >= _check_budget(deadline, error, state):
                state.update(category='TIMEOUT', reason='BUDGET_EXHAUSTED')
                raise TimeoutError('DOHOD_BUDGET_EXHAUSTED: нет бюджета для следующей попытки') from error
            if not _retry_event(logging_context, error, state):
                state['reason'] = 'RETRY_LOGGING_FAILED'
                raise
            if delay >= _check_budget(deadline, error, state):
                state.update(category='TIMEOUT', reason='BUDGET_EXHAUSTED')
                raise TimeoutError('DOHOD_BUDGET_EXHAUSTED: нет бюджета для следующей попытки') from error
            _retry_wait(delay)
            _check_budget(deadline, error, state)
            continue


def _operation_state(url):
    started = _network_clock()
    return {'started': started, 'last_checked': started, 'endpoint': None, 'attempt': 0,
            'category': 'SOURCE', 'reason': '', 'phase': '', 'sends': 0,
            'failure_kind': 'legacy'}


def _source_checkpoint(deadline, logging_context):
    state = _operation_state(None)
    try:
        _check_budget(deadline, state=state)
    except Exception as error:
        _failure(logging_context, error, state)
        raise


def _table_profiles():
    # Это утверждённые consumer profiles после pandas, не live HTML contract.
    # Исторические и synthetic legacy поля не переименовываем друг в друга.
    return {
        'overview': (
            ('Тикер', 'Дивиденд'), ('Тикер', 'Компания', 'Дивиденд'),
            ('Акция', 'Сектор', 'Период', 'Выплата на акцию (прогноз)',
             'Доходность выплаты', 'Цена расчета доходности',
             'Дата закрытия реестра (оценка)', 'Капитализация, млн. руб.', 'DSI')),
        'history': (('Год', 'Сумма'), ('Год', 'Дивиденд (руб.)', 'Изм. к пред. году')),
        'payments': (('Дата', 'Выплата'), ('Дата объявления дивиденда',
                     'Дата закрытия реестра', 'Год для учета дивиденда', 'Дивиденд')),
        'metrics': (('текущая доходность', 'доля от прибыли', 'индекс DSI'),),
    }


def _semantic_error(state, code, *, page_ref=None):
    # Вызывается только вне исходного except и после относящегося к нему budget
    # check. Route выбираем по месту отказа, не по тексту чужого исключения.
    state.update(failure_kind='semantic', failure_code=code, reason=code,
                 category='SOURCE' if code == 'DOHOD_DETAIL_PARSE_FAILED' else 'QUALITY',
                 phase='', page_ref=page_ref)
    return ValueError(code)


def _select_tables(tables, page_kind):
    profiles = _table_profiles()
    allowed = ('overview',) if page_kind == 'overview' else ('metrics', 'history', 'payments')
    required = ('overview',) if page_kind == 'overview' else ('metrics', 'history')
    selected = {}
    if type(tables) is not list:
        return selected, 'DOHOD_TABLE_SCHEMA_UNSUPPORTED'
    for source in tables:
        if not isinstance(source, pd.DataFrame):
            return selected, 'DOHOD_TABLE_SCHEMA_UNSUPPORTED'
        if source.columns.has_duplicates or isinstance(source.columns, pd.MultiIndex):
            return selected, 'DOHOD_TABLE_SCHEMA_UNSUPPORTED'
        columns = source.columns.tolist()
        role, profile, clean = None, None, columns
        if page_kind == 'overview':
            clean = [name for name in columns if not (type(name) is str and
                     ('Unnamed:' in name or 'Капитализация, sorting' in name))]
        for candidate in allowed:
            if candidate == 'metrics':
                # Только physical row 1 задаёт technical header. Исходные
                # labels трёх колонок и повторные source rows не меняем.
                if (len(columns) == 3 and len(source) >= 2
                        and all(type(value) is str and value == label
                                for value, label in zip(source.iloc[1], profiles['metrics'][0]))):
                    role, profile = candidate, 0
            elif all(type(name) is str for name in clean):
                for number, signature in enumerate(profiles[candidate]):
                    if tuple(clean) == signature:
                        if role is not None:
                            return selected, 'DOHOD_TABLE_ROLE_CONFLICT'
                        role, profile = candidate, number
        if role is None:
            anchors = {label for variants in profiles.values() for signature in variants for label in signature}
            anchored = any(type(name) is str and name in anchors for name in columns)
            anchored = anchored or any(type(value) is str and value in profiles['metrics'][0]
                                       for value in source.to_numpy().flat)
            code = ('DOHOD_TABLE_SCHEMA_UNSUPPORTED' if anchored else
                    'DOHOD_UNKNOWN_EMPTY_TABLE' if source.empty else 'DOHOD_UNKNOWN_NONEMPTY_TABLE')
            return selected, code
        if role in selected:
            return selected, 'DOHOD_TABLE_ROLE_AMBIGUOUS'
        # Только исходные payload cells. Header-as-data, index и добавленные
        # page_url/ticker не могут сделать пустой источник непустым.
        payload = source.loc[:, clean] if role == 'overview' else source
        if role == 'metrics':
            payload = source.iloc[[i for i in range(len(source)) if i != 1]]
        if not payload.notna().to_numpy().any():
            return selected, 'DOHOD_EMPTY_ROLE_NOT_VERIFIED'
        selected[role] = (source.copy(deep=True), profile)
    if any(role not in selected for role in required):
        return selected, 'DOHOD_TABLE_REQUIRED_MISSING'
    return selected, None


def _page_ref(url):
    return 'dohod-page-' + hashlib.sha256(url.encode('ascii')).hexdigest()


def _new_coverage():
    return dict(version=1, scope='ADMITTED_DISCOVERY', overview_final_url=None,
        expected=None, pages=[], missing=None,
        discovery=dict(hrefs=0, admitted=0, deduplicated=0, rejected={}, ignored=0),
        counts=dict(expected=None, requested=0, received=0, parsed=0, validated=0,
                    accepted=0, failed=0, not_run=0, missing=None),
        observed_coverage='NOT_VERIFIED', source_coverage='NOT_VERIFIED',
        payments_coverage='NOT_EVALUATED', late_group_input_status='BLOCKED_FAILED',
        execution_outcome='STARTED', diagnostics_status='NOT_REQUESTED',
        finalized=False, coverage_delivery_attempted=False, elapsed_ms=0)


def _new_page(url):
    return dict(url=url, ref=_page_ref(url), stage='DISCOVERED', outcome='NOT_RUN',
        reason='NOT_REQUESTED', requested=False, received=False, parsed=False,
        validated=False, accepted=False, attempts=0, sends=0, roles={},
        elapsed_ms=0, budget_remaining_ms=None)


def _page_progress(state):
    page = state.get('page_record')
    if page is not None:
        page.update(attempts=state['attempt'], sends=state['sends'],
                    requested=state['sends'] > 0)
        if page['requested'] and page['stage'] == 'DISCOVERED':
            page['stage'] = 'REQUESTED'


def _coverage_counts(report):
    pages = report['pages']
    report['counts'].update({key: sum(page[key] for page in pages)
                            for key in ('requested', 'received', 'parsed', 'validated', 'accepted')})
    report['counts'].update(failed=sum(page['outcome'] in ('FAILED', 'NOT_VERIFIED', 'INTERRUPTED')
                                      for page in pages),
                            not_run=sum(page['outcome'] == 'NOT_RUN' for page in pages))
    if report['expected'] is not None:
        report['missing'] = [page['url'] for page in pages if not page['accepted']]
        report['counts'].update(expected=len(pages), missing=len(report['missing']))


def _coverage_fields(report, page=None):
    # Разные роли имеют разные строки/header semantics. Не выдаём их сумму
    # за rows_received и не приравниваем canonical pages к instruments.
    counts = dict(rows_received=None, rows_accepted=None, rows_quarantined=None,
                  pages=int(page['received']) if page is not None else report['counts']['received'],
                  instruments=None, null_reasons={
                      'rows_received': 'HETEROGENEOUS_TABLE_ROWS_NOT_AGGREGATED',
                      'rows_accepted': 'HETEROGENEOUS_TABLE_ROWS_NOT_AGGREGATED',
                      'rows_quarantined': 'NOT_EVALUATED',
                      'instruments': 'SOURCE_UNIVERSE_NOT_VERIFIED'})
    retries = {'attempt': page['attempts'], 'limit': 3} if page is not None and page['attempts'] else None
    return dict(stage='dohodru_data', page=page['ref'] if page is not None else None,
        outcome=page['outcome'] if page is not None else report['observed_coverage'],
        elapsed_ms=report['elapsed_ms'],
        duration_ms=page['elapsed_ms'] if page is not None else report['elapsed_ms'],
        counts=counts, retries=retries, error=None, fields={}, null_reasons={
            **({'page': 'AGGREGATE_COVERAGE'} if page is None else {}),
            **({'retries': 'MULTIPLE_LOGICAL_OPERATIONS' if page is None else 'NO_REQUEST_ATTEMPTED'}
               if retries is None else {}),
            'error': 'NO_ERROR' if report['execution_outcome'] == 'RETURNED'
                     else 'PRIMARY_ERROR_REPORTED_SEPARATELY'})


def _coverage_event(logging_context, event, message, fields, state):
    if logging_context is None:
        return
    import run_logging
    delivered = False
    try:
        receipt = run_logging.emit_event(logging_context, 'INFO', event, message, fields)
        delivered = receipt['accepted'] and run_logging.check_logging_health(logging_context)['healthy']
    except Exception:
        pass
    if not delivered:
        state.update(failure_kind='coverage', failure_code='LOGGING_INCOMPLETE',
                     category='IO', reason='LOGGING_INCOMPLETE', phase='')
        # Вне except: underlying logging exception не остаётся в context/cause.
        raise RuntimeError('LOGGING_INCOMPLETE')


def _coverage_batch(logging_context, report, state):
    if report['coverage_delivery_attempted'] or logging_context is None:
        return
    report['coverage_delivery_attempted'] = True
    report['diagnostics_status'] = 'PENDING'
    for page in report['pages']:
        _coverage_event(logging_context, 'dohod_page_coverage',
            'Покрытие страницы доход.ру: ' + page['outcome'] + '; ' + (page['reason'] or 'ACCEPTED'),
            _coverage_fields(report, page), state)
    values = report['counts']
    summary = '; '.join(key + '=' + (str(value) if value is not None else 'NOT_VERIFIED')
                        for key, value in values.items())
    _coverage_event(logging_context, 'dohod_coverage_summary',
        'Покрытие обнаруженного набора доход.ру: ' + summary
        + '; payments=' + report['payments_coverage']
        + '; late_group_input=' + report['late_group_input_status'],
        _coverage_fields(report), state)
    import run_logging
    confirmed = False
    try:
        receipt = run_logging.flush_logging(logging_context)
        confirmed = receipt['confirmed'] and run_logging.check_logging_health(logging_context)['healthy']
    except Exception:
        pass
    if not confirmed:
        state.update(failure_kind='coverage', failure_code='LOGGING_INCOMPLETE',
                     category='IO', reason='LOGGING_INCOMPLETE', phase='')
        raise RuntimeError('LOGGING_INCOMPLETE')
    report['diagnostics_status'] = 'CONFIRMED'


# %%
### Составление общего списка
def _prepare_main(url, source_deadline, logging_context):
    state = _operation_state(url)
    deadline = min(source_deadline, state['started'] + 35)
    try:
        return _main_frame(url, deadline, logging_context, state)
    except Exception as error:
        _failure(logging_context, error, state)
        raise


def _main_frame(url, deadline, logging_context, state):
    html, _ = _request_page(url, deadline, logging_context, state, page_kind='overview')
    df_list = _read_dohod_html(html)
    _check_budget(deadline, state=state)
    selected, reason = _select_tables(df_list, 'overview')
    _check_budget(deadline, state=state)
    if reason is not None:
        raise _semantic_error(state, reason)
    df_mainpage = selected['overview'][0]

    # очистка от ненужных колонок
    bad_columns = []
    for i in list(df_mainpage.columns):
        if "Unnamed:" in i or "Капитализация, sorting" in i:
            bad_columns.append(i)

    # print(bad_columns)
    df_mainpage.drop(columns=bad_columns, inplace=True)
    _logging_barrier(logging_context)
    _check_budget(deadline, state=state)
    return df_mainpage


def _write_overview(df_mainpage):

    # len(df_mainpage)
    if len(df_mainpage) > 0:
        df_mainpage.to_excel('{}/datasets/dividends/dohodru/obzor_vsex_divs_tickerov.xlsx'.format(current_path))
    else:
        print('Не удалось скачать актуальные данные о дивах с доход.ру')


def get_main (url, *, logging_context=None):
    source_deadline = _network_clock() + 600
    df_mainpage = _prepare_main(url, source_deadline, logging_context)
    _source_checkpoint(source_deadline, logging_context)
    try:
        _write_overview(df_mainpage)
    except Exception as error:
        state = _operation_state(None)
        state.update(category='IO', reason='EXPORT_FAILED')
        _failure(logging_context, error, state, export=True)
        raise
    # После допуска writer не проверяем acquisition deadline.
    return df_mainpage


# %%
def get_page_info (url, *, logging_context=None):
    return _prepare_pages(url, _network_clock() + 600, logging_context)


def _prepare_pages(url, source_deadline, logging_context):
    frames, _ = _prepare_pages_result(url, source_deadline, logging_context, coverage_sink={})
    return frames


def _finish_coverage(report, state, error=None):
    # Здесь только уже измеренные значения и собственные dict/list. Даже без
    # logger private caller получает тот же sink до повторного raise ошибки.
    _page_progress(state)
    report['elapsed_ms'] = max(0, (state['last_checked'] - report['started']) * 1000)
    page = state.get('page_record')
    if page is not None:
        page['elapsed_ms'] = max(0, (state['last_checked'] - state['started']) * 1000)
        if 'deadline' in state:
            page['budget_remaining_ms'] = max(0, state['deadline'] - state['last_checked']) * 1000
        if error is not None and not page['accepted']:
            reason = state.get('failure_code') if state.get('failure_kind') != 'legacy' else state['reason']
            page['reason'] = reason or 'DOHOD_COLLECTION_FAILED'
            if not isinstance(error, Exception):
                page['outcome'] = 'INTERRUPTED'
            else:
                unverified = ('UNKNOWN', 'UNSUPPORTED', 'EMPTY_ROLE', 'PROFILE_CHANGED')
                page['outcome'] = 'NOT_VERIFIED' if any(token in page['reason'] for token in unverified) else 'FAILED'
    for pending in report['pages']:
        if pending['outcome'] == 'NOT_RUN' and error is not None:
            pending['reason'] = 'PRIOR_PAGE_FAILED'
    _coverage_counts(report)
    complete = bool(report['expected']) and not report['missing']
    report['observed_coverage'] = ('COMPLETE_OBSERVED' if complete else
                                   'INCOMPLETE' if report['expected'] else 'NOT_VERIFIED')
    report['execution_outcome'] = ('RETURNED' if error is None else
                                    'FAILED' if isinstance(error, Exception) else 'INTERRUPTED')
    report['late_group_input_status'] = ('PASS' if error is None and complete
        and report['payments_coverage'] == 'PRESENT_FOR_ALL' else
        'BLOCKED_NOT_VERIFIED' if error is None and complete else 'BLOCKED_FAILED')
    report['finalized'] = True


def _prepare_pages_result(url, source_deadline, logging_context, *, coverage_sink):
    if type(coverage_sink) is not dict or coverage_sink:
        raise ValueError('DOHOD_COVERAGE_SINK_INVALID')
    report = _new_coverage()
    coverage_sink['report'] = report
    state = dict(started=0, last_checked=0, endpoint=None, attempt=0, sends=0,
                 category='SOURCE', reason='', phase='', failure_kind='legacy')
    report['started'] = 0
    try:
        state.update(_operation_state(url))
        report['started'] = state['started']
        frames = _page_frames(url, source_deadline, logging_context, state, report)
        _finish_coverage(report, state)
        _coverage_batch(logging_context, report, state)
        _check_budget(source_deadline, state=state)
        report['elapsed_ms'] = max(0, (state['last_checked'] - report['started']) * 1000)
        return frames, report
    except BaseException as error:
        # Это единственный collection failure owner. Wrapper и main не
        # повторяют _failure; частично доставленный batch не воспроизводим.
        primary_state = dict(state)
        _finish_coverage(report, primary_state, error)
        try:
            _coverage_batch(logging_context, report, state)
        except BaseException:
            report['diagnostics_status'] = 'INCOMPLETE'
        if report['coverage_delivery_attempted'] and report['diagnostics_status'] == 'PENDING':
            report['diagnostics_status'] = 'INCOMPLETE'
        if isinstance(error, Exception):
            _failure(logging_context, error, primary_state, coverage_report=report)
        raise


def _page_frames(url, source_deadline, logging_context, state, report):

    df_overview_full = pd.DataFrame()
    df_years_full = pd.DataFrame()
    df_each_full = pd.DataFrame()

    ### Сбор всех ссылок на страницы
    _check_budget(source_deadline, state=state)
    deadline = min(source_deadline, state['started'] + 35)
    page_text, overview_base = _request_page(url, deadline, logging_context, state,
                                          page_kind='overview', text=True)
    report['overview_final_url'] = overview_base
    soup = BeautifulSoup(page_text, 'lxml')
    _check_budget(deadline, state=state)

    url_list = []
    seen, warned = set(), set()
    for link in soup.find_all('a', href=True):
        report['discovery']['hrefs'] += 1
        href = link.get('href')
        if not href:
            report['discovery']['ignored'] += 1
            continue
        try:
            target = _canonical_url(href, overview_base)
        except ValueError as error:
            reason = error.args[0]
            rejected = report['discovery']['rejected']
            rejected[reason] = rejected.get(reason, 0) + 1
            if reason != 'URL_PATH_NOT_ALLOWED' and reason not in warned:
                warned.add(reason)
                _dohod_event(logging_context, 'WARNING', 'dohod_url_rejected',
                    'Ссылка доход.ру отклонена: ' + reason, state=state, outcome='REJECTED')
            _check_budget(deadline, state=state)
            continue
        if _url_kind(target) == 'detail':
            report['discovery']['admitted'] += 1
            if target not in seen:
                seen.add(target)
                url_list.append(target)
            else:
                report['discovery']['deduplicated'] += 1
        else:
            report['discovery']['ignored'] += 1
    

    print("Количество страниц для обхода: {}".format(len(url_list)))
    _logging_barrier(logging_context)
    _check_budget(deadline, state=state)
    # Expected и records становятся видимыми вместе. Прерывание при их
    # подготовке оставляет membership неизвестным, а не ложно полным.
    pages = [_new_page(target) for target in url_list]
    report.update(expected=list(url_list), pages=pages)
    if not url_list:
        raise _semantic_error(state, 'DOHOD_DISCOVERY_EMPTY_NOT_VERIFIED')


    ### Обход каждой страницы

    profiles_seen = {}
    for page in report['pages']:
        url = page['url']
        next_state = _operation_state(url)
        state.clear()
        state.update(next_state)
        state['page_record'] = page
        state['page_ref'] = page['ref']
        _check_budget(source_deadline, state=state)
        deadline = min(source_deadline, state['started'] + 35)
        state['deadline'] = deadline
        html, _ = _request_page(url, deadline, logging_context, state, page_kind='detail')
        _page_progress(state)
        page.update(received=True, stage='RECEIVED')
        parse_failed = False
        try:
            df_list = _read_dohod_html(html)
        except Exception:
            parse_failed = True

        _check_budget(deadline, state=state)
        if parse_failed:
            raise _semantic_error(state, 'DOHOD_DETAIL_PARSE_FAILED', page_ref=page['ref'])
        page.update(parsed=True, stage='PARSED')
        selected, reason = _select_tables(df_list, 'detail')
        _check_budget(deadline, state=state)
        if reason is not None:
            raise _semantic_error(state, reason, page_ref=page['ref'])
        for role in ('history', 'payments'):
            if role in selected:
                profile = selected[role][1]
                if role in profiles_seen and profile != profiles_seen[role]:
                    raise _semantic_error(state, 'DOHOD_TABLE_PROFILE_CHANGED', page_ref=page['ref'])
                profiles_seen[role] = profile

        # Legacy projection выполняется на локальных копиях только после
        # проверки всех ролей. Никаких новых dropna/reset/dedup для source rows.
        rate = selected['metrics'][0]
        rate.columns = rate.iloc[1]
        rate.reset_index(inplace=True)
        rate = rate.drop(index=1)
        rate = rate[list(_table_profiles()['metrics'][0])]
        years = selected['history'][0]
        payments = selected['payments'][0] if 'payments' in selected else pd.DataFrame()
        for frame in (rate, years) + ((payments,) if 'payments' in selected else ()):
            frame['page_url'] = url
            frame['ticker'] = url.rsplit('/', 1)[-1].upper()
        page['roles'] = {role: dict(status='PRESENT', profile=profile,
                                    rows=len(rate) if role == 'metrics' else len(frame))
                         for role, (frame, profile) in selected.items()}
        if 'payments' not in selected:
            page['roles']['payments'] = dict(status='ABSENT_ALLOWED', profile=None, rows=0)
        page.update(validated=True, stage='VALIDATED')
        # Сначала готовим весь набор новых frames, затем принимаем страницу.
        # При failure ни одна часть её данных не возвращается caller как success.
        prepared = (pd.concat([df_overview_full, rate]), pd.concat([df_years_full, years]),
                    pd.concat([df_each_full, payments]) if 'payments' in selected else df_each_full)
        _logging_barrier(logging_context)
        _check_budget(deadline, state=state)
        df_overview_full, df_years_full, df_each_full = prepared
        page.update(accepted=True, stage='ACCEPTED', outcome='ACCEPTED', reason=None,
                    elapsed_ms=max(0, (state['last_checked'] - state['started']) * 1000),
                    budget_remaining_ms=max(0, deadline - state['last_checked']) * 1000)

    _logging_barrier(logging_context)
    _check_budget(source_deadline, state=state)
    report['payments_coverage'] = ('PRESENT_FOR_ALL' if all(
        page['roles']['payments']['status'] == 'PRESENT' for page in report['pages']) else 'NOT_VERIFIED')
    
    return df_overview_full, df_years_full, df_each_full


# %%
def main(url, *, logging_context=None):
    source_deadline = _network_clock() + 600
    df_mainpage = _prepare_main(url, source_deadline, logging_context)
    _source_checkpoint(source_deadline, logging_context)
    try:
        _write_overview(df_mainpage)
        df_mainpage.to_excel('{}/datasets/dividends/dohodru/main_page.xlsx'.format(current_path))
        df_mainpage.to_csv('{}/datasets/dividends/dohodru/main_page.csv'.format(current_path))
    except Exception as error:
        state = _operation_state(None)
        state.update(category='IO', reason='EXPORT_FAILED')
        _failure(logging_context, error, state, export=True)
        raise
    

    # Время ранних writes учитывается исходным source deadline. Внутри уже
    # допущенной группы checkpoint нет; поздний network failure не откатывает её.
    frames, coverage = _prepare_pages_result(url, source_deadline, logging_context, coverage_sink={})
    if coverage['late_group_input_status'] != 'PASS':
        state = _operation_state(None)
        error = _semantic_error(state, 'DOHOD_PAYMENTS_COVERAGE_NOT_VERIFIED')
        primary_state = dict(state)
        try:
            _coverage_event(logging_context, 'dohod_late_group_blocked',
                'Поздняя группа доход.ру запрещена: полнота payments не подтверждена',
                dict(stage='dohodru_data', outcome='BLOCKED_NOT_VERIFIED', fields={}), state)
        except BaseException:
            coverage['diagnostics_status'] = 'INCOMPLETE'
        _failure(logging_context, error, primary_state, coverage_report=coverage)
        raise error
    df_overview_full, df_years_full, df_each_full = frames
    _source_checkpoint(source_deadline, logging_context)
    try:
        # Один admission для всех шести calls, включая следующие XLSX/CSV пары.
        df_overview_full.to_excel('{}/datasets/dividends/dohodru/rate_for_each_ticker.xlsx'.format(current_path))
        df_overview_full.to_csv('{}/datasets/dividends/dohodru/rate_for_each_ticker.csv'.format(current_path))

        df_years_full.to_excel('{}/datasets/dividends/dohodru/sum_each_year_divs.xlsx'.format(current_path))
        df_years_full.to_csv('{}/datasets/dividends/dohodru/sum_each_year_divs.csv'.format(current_path))

        df_each_full.to_excel('{}/datasets/dividends/dohodru/all_payments.xlsx'.format(current_path))
        df_each_full.to_csv('{}/datasets/dividends/dohodru/all_payments.csv'.format(current_path))
    except Exception as error:
        state = _operation_state(None)
        state.update(category='IO', reason='EXPORT_FAILED')
        _failure(logging_context, error, state, export=True)
        raise

# %%
if __name__ == "__main__":
    url = 'https://www.dohod.ru/ik/analytics/dividend'
    main(url)
