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
import errno, re, socket, ssl, time
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
    remaining = deadline - _network_clock()
    if remaining <= 0:
        if state is not None:
            state.update(category='TIMEOUT', reason='BUDGET_EXHAUSTED')
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


def _failure(logging_context, error, state, *, export=False):
    if logging_context is None:
        return
    try:
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
    return {'started': _network_clock(), 'endpoint': None, 'attempt': 0,
            'category': 'SOURCE', 'reason': '', 'phase': ''}


def _source_checkpoint(deadline, logging_context):
    state = _operation_state(None)
    try:
        _check_budget(deadline, state=state)
    except Exception as error:
        _failure(logging_context, error, state)
        raise


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
    df_mainpage = df_list[-1]

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
    state = _operation_state(url)
    try:
        return _page_frames(url, source_deadline, logging_context, state)
    except Exception as error:
        _failure(logging_context, error, state)
        raise


def _page_frames(url, source_deadline, logging_context, state):

    df_overview_full = pd.DataFrame()
    df_years_full = pd.DataFrame()
    df_each_full = pd.DataFrame()

    ### Сбор всех ссылок на страницы
    _check_budget(source_deadline, state=state)
    deadline = min(source_deadline, state['started'] + 35)
    page_text, overview_base = _request_page(url, deadline, logging_context, state,
                                          page_kind='overview', text=True)
    soup = BeautifulSoup(page_text, 'lxml')
    _check_budget(deadline, state=state)

    url_list = []
    seen, warned = set(), set()
    for link in soup.find_all('a', href=True):
        href = link.get('href')
        if not href:
            continue
        try:
            target = _canonical_url(href, overview_base)
        except ValueError as error:
            reason = error.args[0]
            if reason != 'URL_PATH_NOT_ALLOWED' and reason not in warned:
                warned.add(reason)
                _dohod_event(logging_context, 'WARNING', 'dohod_url_rejected',
                    'Ссылка доход.ру отклонена: ' + reason, state=state, outcome='REJECTED')
            _check_budget(deadline, state=state)
            continue
        if _url_kind(target) == 'detail' and target not in seen:
            seen.add(target)
            url_list.append(target)
    

    print("Количество страниц для обхода: {}".format(len(url_list)))
    _logging_barrier(logging_context)
    _check_budget(deadline, state=state)


    ### Обход каждой страницы

    b = []
    for url in url_list:
        state.clear()
        state.update(_operation_state(url))
        _check_budget(source_deadline, state=state)
        deadline = min(source_deadline, state['started'] + 35)
        html, _ = _request_page(url, deadline, logging_context, state, page_kind='detail')
        try:
            df_list = _read_dohod_html(html)
        except:
            df_list = pd.DataFrame()
            print('Не смог найти таблицы на этой странице: {}',format(url))

        _check_budget(deadline, state=state)

        a = len(df_list)
        b.append(a)

        if len(df_list) == 2:
            df = df_list[0]
            df.columns = df.iloc[1]
            df.reset_index(inplace=True)
            df = df.drop (index= 1)
            df = df[['текущая доходность','доля от прибыли','индекс DSI']]
            df['page_url'] = url
            df['ticker'] = url.split(r'/')[-1].upper()
            df_overview_full = pd.concat([df_overview_full,df])

            df = df_list[1]
            df['page_url'] = url
            df['ticker'] = url.split(r'/')[-1].upper()
            df_years_full = pd.concat([df_years_full,df])

        elif len(df_list) == 3:
            df = df_list[0]
            df.columns = df.iloc[1]
            df.reset_index(inplace=True)
            df = df.drop (index= 1)
            df = df[['текущая доходность','доля от прибыли','индекс DSI']]
            df['page_url'] = url
            df['ticker'] = url.split(r'/')[-1].upper()
            if len(df) > 0: df_overview_full = pd.concat([df_overview_full,df])

            df = df_list[1]
            df['page_url'] = url
            df['ticker'] = url.split(r'/')[-1].upper()
            if len(df) > 0: df_years_full = pd.concat([df_years_full,df])

            df = df_list[2]
            df['page_url'] = url
            df['ticker'] = url.split(r'/')[-1].upper()
            if len(df) > 0: df_each_full = pd.concat([df_each_full,df])

        _logging_barrier(logging_context)
        _check_budget(deadline, state=state)


    ## Статистика для отладки. Сколько разных табличек спарсено
    o = list(set(b))

    for i in o:
        print("Тип {}. Количество: {}".format(i, b.count(i)))
        print("___")

    _logging_barrier(logging_context)
    _check_budget(source_deadline, state=state)
    
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
    df_overview_full, df_years_full, df_each_full = _prepare_pages(url, source_deadline, logging_context)
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
