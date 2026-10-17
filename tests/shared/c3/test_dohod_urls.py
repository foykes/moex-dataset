"""Offline URL-policy oracles; synthetic URLs do not establish a live source contract."""

from email.message import Message
import hashlib
import html
from http.cookiejar import CookieJar
from http.cookies import SimpleCookie
import json
import traceback
from types import SimpleNamespace

import pandas as pd
import pytest
import requests
from urllib3._collections import HTTPHeaderDict

from tests.shared.c2.test_dohod_compatibility import FILES, assert_writers
from tests.shared.c2.test_dohod_network import context, events, model, writers
from tests.shared.c3.test_dohod_html import (
    assert_detail, assert_no_accumulation, assert_overview, detail_html,
    expected_detail, expected_overview, fresh_context, overview_html,
)


OVERVIEW = 'https://www.dohod.ru/ik/analytics/dividend'
SLASH_OVERVIEW = OVERVIEW + '/'
AAA = OVERVIEW + '/aaa'
BBB = OVERVIEW + '/bbb'
URL_SECRET = 'C3_URL_SECRET_941'


class WireBody:
    def __init__(self, body, headers, *, error=None, after_read=None, close_error=None):
        self.body = body
        self.error = error
        self.after_read = after_read
        self.close_error = close_error
        self.reads = self.closes = self.releases = 0
        self.headers = HTTPHeaderDict()
        message = Message()
        for name, value in headers:
            self.headers.add(name, value)
            message[name] = value
        self._original_response = SimpleNamespace(msg=message)

    def stream(self, size, decode_content=True):
        self.reads += 1
        yield self.body
        if self.after_read:
            self.after_read()
        if self.error is not None:
            raise self.error

    def close(self):
        self.closes += 1
        if self.close_error is not None:
            raise self.close_error

    def release_conn(self):
        self.releases += 1


def wire_reply(body=b'', *, status=200, headers=(), error=None,
               after_read=None, close_error=None):
    """Real Response semantics and duplicate-capable raw HTTP headers."""
    response = requests.Response()
    response.status_code = status
    response.reason = 'offline fixture'
    response.encoding = 'utf-8'
    for name, value in headers:
        response.headers[name] = value
    response.raw = WireBody(body, headers, error=error,
        after_read=after_read, close_error=close_error)
    response.close_calls = 0
    real_close = response.close

    def close():
        response.close_calls += 1
        real_close()

    response.close = close
    return response


def transport(monkeypatch, module, sequence):
    """Keep old Requests redirect behavior while intercepting every actual send.

    The old requests.get delegates to its real implementation, whose adapters
    are faked. The new controller can send through the same adapter boundary.
    Neither route can reach a socket, and neither route returns fake frames.
    """
    pending = list(sequence)
    trace = {'sends': [], 'get_calls': [], 'responses': [], 'pending': pending}
    real_get = module.requests.get

    def get(url, **kwargs):
        trace['get_calls'].append((url, dict(kwargs)))
        return real_get(url, **kwargs)

    def send(adapter, request, **kwargs):
        trace['sends'].append((request.url, request, dict(kwargs)))
        assert request.method == 'GET' and request.body is None
        assert pending, 'Unexpected offline send: ' + request.url
        expected, result = pending.pop(0)
        assert expected is None or request.url == expected
        if isinstance(result, BaseException):
            raise result
        result.url = request.url
        result.request = request
        trace['responses'].append(result)
        return result

    monkeypatch.setattr(module.requests, 'get', get)
    monkeypatch.setattr(requests.adapters.HTTPAdapter, 'send', send)
    return trace


def sent_urls(trace):
    return [url for url, _, _ in trace['sends']]


def assert_consumed(response):
    assert response.close_calls == 1 and response.raw.reads == 1
    assert response.raw.closes == 0 and response.raw.releases == 1


def assert_unread_closed(response):
    assert response.close_calls == 1 and response.raw.reads == 0
    assert response.raw.closes == response.raw.releases == 1


def discovery(*hrefs, base=None):
    prefix = '' if base is None else '<base href="' + html.escape(base, quote=True) + '">'
    return (prefix + ''.join('<a href="' + html.escape(value, quote=True)
        + '">fixture</a>' for value in hrefs)).encode('utf-8')


def sessions(monkeypatch, *, seed=None, close_errors=None):
    """Observe native attempt ownership without replacing Session behavior."""
    created = []
    closed = []
    close_errors = {} if close_errors is None else close_errors
    original_init = requests.Session.__init__
    original_close = requests.Session.close

    def initialize(session, *args, **kwargs):
        original_init(session, *args, **kwargs)
        created.append(session)
        if seed:
            seed(session, len(created) - 1)

    def close(session):
        closed.append(session)
        original_close(session)
        index = created.index(session)
        if index in close_errors:
            raise close_errors[index]

    monkeypatch.setattr(requests.Session, '__init__', initialize)
    monkeypatch.setattr(requests.Session, 'close', close)
    return created, closed


def assert_safe_events(context):
    values = events(context)
    assert URL_SECRET not in json.dumps(values, ensure_ascii=False)
    return values


@pytest.mark.parametrize('target', [
    'http://www.dohod.ru/ik/analytics/dividend',
    'https://foreign.invalid/ik/analytics/dividend',
    'https://www.dohod.ru.foreign.invalid/ik/analytics/dividend',
    'https://user:' + URL_SECRET + '@www.dohod.ru/ik/analytics/dividend',
    'https://www.dohod.ru:444/ik/analytics/dividend',
    OVERVIEW + '?' + URL_SECRET,
    'https://www.dohod.ru/ik/analytics/dividend%2f',
    'https://www.dohod.ru/ik/analytics/./dividend',
    'https://www.dohod.ru/ik/analytics/other/../dividend',
    'https://www.dohod.ru/ik/analytics/дивиденд',
    'https://www.dohod.ru\\@foreign.invalid/ik/analytics/dividend',
    ' ' + OVERVIEW,
    OVERVIEW + '\x7f',
    AAA,
])
def test_initial_overview_policy_rejects_before_any_http_send(
        monkeypatch, model, tmp_path, target):
    module, _, waits = model
    trace = transport(monkeypatch, module, [(None, wire_reply(overview_html()))])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises((ValueError, requests.RequestException)):
        module.get_main(target)
    assert sent_urls(trace) == [] and waits == [] and written == []
    assert_no_accumulation(module)


def test_discovery_canonical_dedup_keeps_first_dom_order_and_literal_records(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    document = discovery('bbb',
        'https://WWW.DOHOD.RU:443/ik/analytics/dividend/%62bb#section',
        '/ik/analytics/dividend/aaa', '//www.dohod.ru:443/ik/analytics/dividend/aaa',
        '', '#section', '/other', '/ik/analytics/dividend/re%70ort.PDF',
        'https://foreign.invalid/ik/analytics/dividend/forbidden',
        base='https://foreign.invalid/')
    responses = [wire_reply(document), wire_reply(detail_html()), wire_reply(detail_html())]
    trace = transport(monkeypatch, module,
        [(SLASH_OVERVIEW, responses[0]), (BBB, responses[1]), (AAA, responses[2])])
    written = writers(monkeypatch, module, tmp_path)
    frames = module.get_page_info(SLASH_OVERVIEW, logging_context=context)
    expected_bbb = expected_detail(page_url=BBB, ticker='BBB')
    expected_aaa = expected_detail(page_url=AAA, ticker='AAA')
    for actual, first, second in zip(frames, expected_bbb, expected_aaa):
        pd.testing.assert_frame_equal(actual, pd.concat([first, second]), check_exact=True)
    assert frames[0].index.tolist() == [0, 2, 0, 2]
    assert frames[1].index.tolist() == [0, 1, 0, 1]
    assert frames[2].index.tolist() == [0, 0]
    assert sent_urls(trace) == [SLASH_OVERVIEW, BBB, AAA]
    assert trace['pending'] == [] and waits == [] and written == []
    for response in responses:
        assert_consumed(response)
    assert_no_accumulation(module)


def test_overview_redirect_uses_approved_final_route_for_rfc_relative_discovery(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    redirect = wire_reply(b'redirect body must stay unread', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    page = wire_reply(discovery('aaa', base='https://foreign.invalid/'))
    detail = wire_reply(detail_html())
    trace = transport(monkeypatch, module,
        [(OVERVIEW, redirect), (SLASH_OVERVIEW, page), (AAA, detail)])
    written = writers(monkeypatch, module, tmp_path)
    frames = module.get_page_info(OVERVIEW, logging_context=context)
    assert_detail(frames)
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW, AAA]
    assert trace['pending'] == [] and waits == [] and written == []
    assert_unread_closed(redirect)
    assert_consumed(page)
    assert_consumed(detail)


def test_detail_redirect_identity_change_is_rejected_before_target_send(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    page = wire_reply(discovery('/ik/analytics/dividend/aaa'))
    redirect = wire_reply(b'unread identity rejection', status=302,
        headers=[('Location', BBB)])
    trace = transport(monkeypatch, module, [(OVERVIEW, page), (AAA, redirect),
        (BBB, wire_reply(detail_html()))])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises((ValueError, requests.RequestException),
                       match='REDIRECT_DETAIL_IDENTITY_CHANGED'):
        module.get_page_info(OVERVIEW, logging_context=context)
    assert sent_urls(trace) == [OVERVIEW, AAA]
    assert waits == [] and written == []
    assert_consumed(page)
    assert_unread_closed(redirect)
    assert_no_accumulation(module)


def test_foreign_redirect_target_never_reaches_transport(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    target = 'https://foreign.invalid/ik/analytics/dividend#' + URL_SECRET
    redirect = wire_reply(b'unread foreign rejection', status=302,
        headers=[('Location', target)])
    trace = transport(monkeypatch, module,
        [(OVERVIEW, redirect), (target.split('#')[0], wire_reply(overview_html()))])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises((ValueError, requests.RequestException), match='REDIRECT_LOCATION_INVALID'):
        module.get_main(OVERVIEW, logging_context=context)
    assert sent_urls(trace) == [OVERVIEW] and waits == [] and written == []
    assert_unread_closed(redirect)


def test_overview_redirect_does_not_consume_body_before_accepting_final_table(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    redirect = wire_reply(b'redirect body is not an accepted document', status=301,
        headers=[('Location', SLASH_OVERVIEW)])
    final = wire_reply(overview_html())
    trace = transport(monkeypatch, module, [(OVERVIEW, redirect), (SLASH_OVERVIEW, final)])
    written = writers(monkeypatch, module, tmp_path)
    frame = module.get_main(OVERVIEW, logging_context=context)
    assert_overview(frame)
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW]
    assert len(written) == 1 and waits == [] and trace['pending'] == []
    assert_unread_closed(redirect)
    assert_consumed(final)


@pytest.mark.parametrize('target,canonical', [
    (OVERVIEW, OVERVIEW),
    (SLASH_OVERVIEW, SLASH_OVERVIEW),
    ('HTTPS://WWW.DOHOD.RU:443/ik/analytics/dividend#section', OVERVIEW),
    ('https://www.dohod.ru/%69k/analytics/dividend?', OVERVIEW),
    ('https://www.dohod.ru:443/ik/analytics/dividend/#one,two', SLASH_OVERVIEW),
])
def test_initial_canonicalization_preserves_two_distinct_overview_routes(
        monkeypatch, model, tmp_path, target, canonical):
    module, _, waits = model
    response = wire_reply(overview_html())
    trace = transport(monkeypatch, module, [(canonical, response)])
    written = writers(monkeypatch, module, tmp_path)
    frame = module.get_main(target)
    assert_overview(frame)
    assert sent_urls(trace) == [canonical] and trace['pending'] == []
    assert len(written) == 1 and waits == []
    assert_consumed(response)


@pytest.mark.parametrize('href,rejected', [
    ('', False), ('#section', False), ('/other', False),
    ('https://www.dohod.ru/ordinary-navigation', False),
    ('https://foreign.invalid/ik/analytics/dividend/aaa', True),
    ('//www.dohod.ru.foreign.invalid/ik/analytics/dividend/aaa', True),
    ('http://www.dohod.ru/ik/analytics/dividend/aaa', True),
    ('https://user:' + URL_SECRET + '@www.dohod.ru/ik/analytics/dividend/aaa', True),
    ('https://www.dohod.ru:0443/ik/analytics/dividend/aaa', True),
    ('https://www.dohod.ru:/ik/analytics/dividend/aaa', True),
    ('https://www.dohod.ru./ik/analytics/dividend/aaa', True),
    ('/ik/analytics/dividend/aaa?' + URL_SECRET, True),
    ('/ik/analytics/dividend/aaa/', True),
    ('/ik/analytics/dividend/aaa/extra', True),
    ('/ik/analytics/dividend/aaa%2Fextra', True),
    ('/ik/analytics/dividend/aaa%5Cextra', True),
    ('/ik/analytics/dividend/%2500', True),
    ('/ik/analytics/dividend/%GG', True),
    ('/ik/analytics/dividend/%00aaa', True),
    ('/ik/analytics/dividend/%2e%2e/aaa', True),
    ('/ik/analytics/dividend/../dividend/aaa', True),
    ('../dividend/aaa', True),
    ('/ik/analytics/dividend/ааа', True),
    (' /ik/analytics/dividend/aaa', True),
    ('/ik/analytics/dividend/aaa\x7f', True),
    ('/ik/analytics/dividend/aaa\\extra', True),
    ('/ik/analytics/dividend/re%70ort.PdF', True),
    ('/ik/analytics/dividend/aaa.pdfx', True),
])
def test_discovery_ignored_and_rejected_links_never_add_a_send(
        monkeypatch, model, tmp_path, context, href, rejected):
    module, _, waits = model
    page = wire_reply(discovery(href, '/ik/analytics/dividend/aaa'))
    detail = wire_reply(detail_html())
    trace = transport(monkeypatch, module, [(SLASH_OVERVIEW, page), (AAA, detail)])
    written = writers(monkeypatch, module, tmp_path)
    frames = module.get_page_info(SLASH_OVERVIEW, logging_context=context)
    assert_detail(frames)
    assert sent_urls(trace) == [SLASH_OVERVIEW, AAA]
    assert trace['pending'] == [] and written == [] and waits == []
    warnings = [event for event in assert_safe_events(context)
                if event['event'] == 'dohod_url_rejected']
    assert len(warnings) == int(rejected)
    assert all(event['level'] == 'WARNING' for event in warnings)
    assert_consumed(page)
    assert_consumed(detail)


def test_slug_case_is_preserved_and_only_unreserved_escapes_are_decoded_once(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    mixed = OVERVIEW + '/AaA'
    punctuation = OVERVIEW + '/a-b_~.x'
    page = wire_reply(discovery('/ik/analytics/dividend/aaa',
        '/ik/analytics/dividend/A%61A', '/ik/analytics/dividend/a-b_%7e.x',
        '/ik/analytics/dividend/AaA#duplicate'))
    details = [wire_reply(detail_html()) for _ in range(3)]
    trace = transport(monkeypatch, module,
        [(OVERVIEW, page), (AAA, details[0]), (mixed, details[1]), (punctuation, details[2])])
    written = writers(monkeypatch, module, tmp_path)
    frames = module.get_page_info(OVERVIEW, logging_context=context)
    expected = [expected_detail(page_url=AAA, ticker='AAA'),
        expected_detail(page_url=mixed, ticker='AAA'),
        expected_detail(page_url=punctuation, ticker='A-B_~.X')]
    for index, frame in enumerate(frames):
        pd.testing.assert_frame_equal(frame, pd.concat([value[index] for value in expected]),
                                      check_exact=True)
    assert sent_urls(trace) == [OVERVIEW, AAA, mixed, punctuation]
    assert waits == [] and written == [] and trace['pending'] == []


@pytest.mark.parametrize('status', [301, 302, 303, 307, 308])
def test_each_approved_redirect_status_keeps_get_and_leaves_body_unread(
        monkeypatch, model, tmp_path, status):
    module, _, waits = model
    redirect = wire_reply(b'not HTML and not read', status=status,
        headers=[('Location', SLASH_OVERVIEW + '#one,two')])
    final = wire_reply(overview_html())
    trace = transport(monkeypatch, module, [(OVERVIEW, redirect), (SLASH_OVERVIEW, final)])
    written = writers(monkeypatch, module, tmp_path)
    assert_overview(module.get_main(OVERVIEW))
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW]
    assert all(request.method == 'GET' and request.body is None
               for _, request, _ in trace['sends'])
    assert len(written) == 1 and waits == []
    assert_unread_closed(redirect)
    assert_consumed(final)


@pytest.mark.parametrize('status,headers', [
    (300, [('Location', SLASH_OVERVIEW)]),
    (304, [('Location', SLASH_OVERVIEW)]),
    (305, [('Location', SLASH_OVERVIEW)]),
    (306, [('Location', SLASH_OVERVIEW)]),
    (301, []), (302, []), (303, []), (307, []), (308, []),
])
def test_terminal_or_missing_location_3xx_preserves_http_error_before_body(
        monkeypatch, model, tmp_path, status, headers):
    module, _, waits = model
    response = wire_reply(b'unread invalid status', status=status, headers=headers)
    trace = transport(monkeypatch, module, [(OVERVIEW, response)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(requests.HTTPError, match='DOHOD_HTTP_STATUS_INVALID') as caught:
        module.get_main(OVERVIEW)
    assert caught.value.response is response
    assert sent_urls(trace) == [OVERVIEW] and waits == [] and written == []
    assert_unread_closed(response)


@pytest.mark.parametrize('locations,reason', [
    ([SLASH_OVERVIEW, SLASH_OVERVIEW], 'REDIRECT_LOCATION_DUPLICATE'),
    ([SLASH_OVERVIEW, 'https://foreign.invalid/'], 'REDIRECT_LOCATION_DUPLICATE'),
    (['https://foreign.invalid/', SLASH_OVERVIEW], 'REDIRECT_LOCATION_DUPLICATE'),
    ([SLASH_OVERVIEW, OVERVIEW], 'REDIRECT_LOCATION_DUPLICATE'),
    (['', SLASH_OVERVIEW], 'REDIRECT_LOCATION_DUPLICATE'),
    ([SLASH_OVERVIEW, ''], 'REDIRECT_LOCATION_DUPLICATE'),
    ([''], 'REDIRECT_LOCATION_INVALID'),
    ([SLASH_OVERVIEW + ', ' + OVERVIEW], 'REDIRECT_LOCATION_INVALID'),
    ([b'/ik/analytics/dividend/'], 'REDIRECT_LOCATION_INVALID'),
    ([' /ik/analytics/dividend/'], 'REDIRECT_LOCATION_INVALID'),
    (['/ik/analytics/dividend/?' + URL_SECRET], 'REDIRECT_LOCATION_INVALID'),
    (['/ik/analytics/dividend/../dividend/'], 'REDIRECT_LOCATION_INVALID'),
    (['/ik/analytics/dividend%2f'], 'REDIRECT_LOCATION_INVALID'),
])
def test_raw_location_values_are_checked_without_comma_splitting_or_body_read(
        monkeypatch, model, tmp_path, context, locations, reason):
    module, _, waits = model
    response = wire_reply(b'unread raw header rejection', status=302,
        headers=[('Location', value) for value in locations])
    trace = transport(monkeypatch, module, [(OVERVIEW, response)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises((ValueError, requests.RequestException), match=reason):
        module.get_main(OVERVIEW, logging_context=context)
    assert sent_urls(trace) == [OVERVIEW] and waits == [] and written == []
    assert_unread_closed(response)
    assert_safe_events(context)


def test_mixed_case_raw_location_fields_cannot_hide_duplicate_in_merged_mapping(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    response = wire_reply(b'unread duplicate', status=302,
        headers=[('Location', SLASH_OVERVIEW), ('lOcAtIoN', SLASH_OVERVIEW)])
    assert response.raw.headers.getlist('Location') == [SLASH_OVERVIEW] * 2
    response.headers['Location'] = SLASH_OVERVIEW
    trace = transport(monkeypatch, module, [(OVERVIEW, response)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError, match='REDIRECT_LOCATION_DUPLICATE'):
        module.get_main(OVERVIEW, logging_context=context)
    assert sent_urls(trace) == [OVERVIEW] and waits == [] and written == []
    assert_unread_closed(response)
    assert_safe_events(context)


@pytest.mark.parametrize('raw_problem', [
    'none', 'missing_getlist', 'timeout', 'builtin_timeout', 'oserror',
])
def test_unavailable_raw_location_is_a_sanitized_nonretryable_policy_failure(
        monkeypatch, model, tmp_path, context, capsys, raw_problem):
    module, _, waits = model
    response = wire_reply(b'unread raw lookup rejection', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    if raw_problem == 'none':
        response.raw.headers = None
    elif raw_problem == 'missing_getlist':
        response.raw.headers = {'Location': SLASH_OVERVIEW}
    else:
        error_type = {'timeout': requests.Timeout, 'builtin_timeout': TimeoutError,
                      'oserror': OSError}[raw_problem]
        error = error_type(URL_SECRET)

        class UnavailableHeaders:
            def getlist(self, name):
                raise error

        response.raw.headers = UnavailableHeaders()
    trace = transport(monkeypatch, module, [(OVERVIEW, response)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises((ValueError, requests.RequestException),
                       match='REDIRECT_LOCATION_UNAVAILABLE') as caught:
        module.get_main(OVERVIEW, logging_context=context)
    rendered = ''.join(traceback.format_exception(caught.type, caught.value, caught.tb))
    assert URL_SECRET not in rendered and caught.value.__cause__ is None
    assert sent_urls(trace) == [OVERVIEW] and waits == [] and written == []
    assert_unread_closed(response)
    values = assert_safe_events(context)
    failure = [event for event in values if event['event'] == 'dohod_acquisition_failed']
    assert len(failure) == 1 and failure[0]['error']['category'] == 'SOURCE'
    assert not any(event['event'] == 'dohod_request_retry' for event in values)
    captured = capsys.readouterr()
    assert URL_SECRET not in captured.out + captured.err


@pytest.mark.parametrize('start,location,reason', [
    (OVERVIEW, AAA, 'REDIRECT_ROLE_CHANGED'),
    (AAA, OVERVIEW, 'REDIRECT_ROLE_CHANGED'),
    (AAA, SLASH_OVERVIEW, 'REDIRECT_ROLE_CHANGED'),
    (AAA, BBB, 'REDIRECT_DETAIL_IDENTITY_CHANGED'),
    (AAA, OVERVIEW + '/AAA', 'REDIRECT_DETAIL_IDENTITY_CHANGED'),
])
def test_redirect_role_and_detail_identity_are_checked_before_target_send(
        monkeypatch, model, tmp_path, start, location, reason):
    module, _, waits = model
    response = wire_reply(b'unread role rejection', status=302,
        headers=[('Location', location)])
    sequence = [(OVERVIEW, response)] if start == OVERVIEW else [
        (OVERVIEW, wire_reply(discovery('/ik/analytics/dividend/aaa'))), (AAA, response)]
    trace = transport(monkeypatch, module, sequence)
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises((ValueError, requests.RequestException), match=reason):
        if start == OVERVIEW:
            module.get_main(OVERVIEW)
        else:
            module.get_page_info(OVERVIEW)
    assert sent_urls(trace) == ([OVERVIEW] if start == OVERVIEW else [OVERVIEW, AAA])
    assert waits == [] and written == []
    assert_unread_closed(response)


@pytest.mark.parametrize('location', [
    OVERVIEW, 'https://WWW.DOHOD.RU:443/ik/analytics/%64ividend#same', '#same',
])
def test_equivalent_canonical_self_loop_is_rejected_before_second_send(
        monkeypatch, model, tmp_path, location):
    module, _, waits = model
    response = wire_reply(b'unread loop', status=302, headers=[('Location', location)])
    trace = transport(monkeypatch, module, [(OVERVIEW, response)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises((ValueError, requests.RequestException), match='REDIRECT_LOOP'):
        module.get_main(OVERVIEW)
    assert sent_urls(trace) == [OVERVIEW] and waits == [] and written == []
    assert_unread_closed(response)


@pytest.mark.parametrize('location', [
    AAA + '#same', 'https://WWW.DOHOD.RU:443/ik/analytics/dividend/aaa',
    '/ik/analytics/dividend/%61aa', '#same',
])
def test_detail_equivalent_identity_still_hits_loop_guard_without_second_detail_send(
        monkeypatch, model, tmp_path, location):
    module, _, waits = model
    page = wire_reply(discovery('dividend/aaa'))
    redirect = wire_reply(b'unread detail self-loop', status=302,
        headers=[('Location', location)])
    trace = transport(monkeypatch, module, [(OVERVIEW, page), (AAA, redirect)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError, match='REDIRECT_LOOP'):
        module.get_page_info(OVERVIEW)
    assert sent_urls(trace) == [OVERVIEW, AAA] and waits == [] and written == []
    assert_consumed(page)
    assert_unread_closed(redirect)


def test_two_overview_route_cycle_closes_both_responses_without_third_send(
        monkeypatch, model, tmp_path):
    module, _, waits = model
    first = wire_reply(b'unread first', status=302, headers=[('Location', SLASH_OVERVIEW)])
    second = wire_reply(b'unread second', status=302, headers=[('Location', OVERVIEW)])
    trace = transport(monkeypatch, module, [(OVERVIEW, first), (SLASH_OVERVIEW, second)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises((ValueError, requests.RequestException), match='REDIRECT_LOOP'):
        module.get_main(OVERVIEW)
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW] and waits == [] and written == []
    assert_unread_closed(first)
    assert_unread_closed(second)


def test_transient_late_hop_retries_original_chain_with_one_shrinking_deadline(
        monkeypatch, model, tmp_path, context):
    module, clock, waits = model
    first = wire_reply(b'unread first redirect', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    real_close = first.close

    def close_at_32():
        real_close()
        clock[0] = 32

    first.close = close_at_32
    busy = wire_reply(b'unread busy body', status=503)
    second = wire_reply(b'unread retry redirect', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    final = wire_reply(overview_html())
    created, closed = sessions(monkeypatch)
    trace = transport(monkeypatch, module, [(OVERVIEW, first), (SLASH_OVERVIEW, busy),
        (OVERVIEW, second), (SLASH_OVERVIEW, final)])
    written = writers(monkeypatch, module, tmp_path)
    assert_overview(module.get_main(OVERVIEW, logging_context=context))
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW, OVERVIEW, SLASH_OVERVIEW]
    assert [kwargs['timeout'] for _, _, kwargs in trace['sends']] == [
        (10, 10), (3, 3), (2, 2), (2, 2)]
    assert waits == [1] and clock[0] == 33 and len(written) == 1
    assert len(created) == 2 and closed == created
    assert created[0] is not created[1]
    for response in (first, busy, second):
        assert_unread_closed(response)
    assert_consumed(final)
    values = assert_safe_events(context)
    assert sum(event['event'] == 'dohod_redirect_followed' for event in values) == 2
    assert sum(event['event'] == 'dohod_request_retry' for event in values) == 1


def test_three_attempts_bound_the_whole_chain_and_preserve_last_http_response(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    redirects = [wire_reply(b'unread redirect', status=302,
        headers=[('Location', SLASH_OVERVIEW)]) for _ in range(3)]
    failures = [wire_reply(b'unread unavailable', status=503) for _ in range(3)]
    sequence = [item for pair in zip(
        [(OVERVIEW, response) for response in redirects],
        [(SLASH_OVERVIEW, response) for response in failures]) for item in pair]
    created, closed = sessions(monkeypatch)
    trace = transport(monkeypatch, module, sequence)
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(requests.HTTPError) as caught:
        module.get_main(OVERVIEW, logging_context=context)
    assert caught.value.response is failures[-1]
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW] * 3
    assert waits == [1, 2] and written == [] and trace['pending'] == []
    assert len(created) == 3 and closed == created
    for response in redirects + failures:
        assert_unread_closed(response)
    values = assert_safe_events(context)
    final = [event for event in values if event['event'] == 'dohod_acquisition_failed']
    assert len(final) == 1 and final[0]['error']['attempt'] == 3
    assert sum(event['event'] == 'dohod_request_retry' for event in values) == 2


@pytest.mark.parametrize('retry_after,waits_expected', [(None, [1]), ('3', [3]), ('11', [])])
def test_retry_after_belongs_to_current_terminal_response_not_redirect(
        monkeypatch, model, tmp_path, retry_after, waits_expected):
    module, _, waits = model
    redirect = wire_reply(b'unread redirect', status=302,
        headers=[('Location', SLASH_OVERVIEW), ('Retry-After', '9')])
    headers = [] if retry_after is None else [('Retry-After', retry_after)]
    busy = wire_reply(b'unread current response', status=503, headers=headers)
    final = wire_reply(overview_html())
    trace = transport(monkeypatch, module,
        [(OVERVIEW, redirect), (SLASH_OVERVIEW, busy), (OVERVIEW, final)])
    written = writers(monkeypatch, module, tmp_path)
    if retry_after == '11':
        with pytest.raises(requests.HTTPError) as caught:
            module.get_main(OVERVIEW)
        assert caught.value.response is busy
        assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW] and written == []
    else:
        assert_overview(module.get_main(OVERVIEW))
        assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW, OVERVIEW]
        assert len(written) == 1
        assert_consumed(final)
    assert waits == waits_expected
    assert_unread_closed(redirect)
    assert_unread_closed(busy)


def test_late_hop_partial_read_retries_chain_without_parsing_or_writing_partial_body(
        monkeypatch, model, tmp_path):
    module, _, waits = model
    first = wire_reply(b'unread redirect', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    partial = wire_reply(b'<table>incomplete ' + URL_SECRET.encode(),
        error=requests.ReadTimeout(URL_SECRET))
    second = wire_reply(b'unread redirect again', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    final = wire_reply(overview_html())
    trace = transport(monkeypatch, module, [(OVERVIEW, first), (SLASH_OVERVIEW, partial),
        (OVERVIEW, second), (SLASH_OVERVIEW, final)])
    written = writers(monkeypatch, module, tmp_path)
    assert_overview(module.get_main(OVERVIEW))
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW] * 2 and waits == [1]
    assert len(written) == 1
    pd.testing.assert_frame_equal(written[0][3], expected_overview(), check_exact=True)
    assert partial.close_calls == 1 and partial.raw.reads == 1
    assert partial.raw.closes == partial.raw.releases == 1
    assert_unread_closed(first)
    assert_unread_closed(second)
    assert_consumed(final)


@pytest.mark.parametrize('phase', ['close', 'redirect_event'])
def test_redirect_deadline_expiry_prevents_next_send_and_admission(
        monkeypatch, model, tmp_path, context, phase):
    module, clock, waits = model
    redirect = wire_reply(b'unread budget redirect', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    if phase == 'close':
        original = redirect.close

        def close():
            original()
            clock[0] = 35

        redirect.close = close
    else:
        original = module._dohod_event

        def emit(logging_context, level, event, message, **kwargs):
            result = original(logging_context, level, event, message, **kwargs)
            if event == 'dohod_redirect_followed':
                assert_unread_closed(redirect)
                clock[0] = 35
            return result

        monkeypatch.setattr(module, '_dohod_event', emit)
    trace = transport(monkeypatch, module, [(OVERVIEW, redirect)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(TimeoutError, match='DOHOD_BUDGET_EXHAUSTED'):
        module.get_main(OVERVIEW, logging_context=context)
    assert sent_urls(trace) == [OVERVIEW] and waits == [] and written == []
    assert_unread_closed(redirect)
    values = assert_safe_events(context)
    final = [event for event in values if event['event'] == 'dohod_acquisition_failed']
    assert len(final) == 1 and final[0]['error']['code'] == 'DOHOD_BUDGET_EXHAUSTED'


@pytest.mark.parametrize('event_name', ['dohod_redirect_followed', 'dohod_url_rejected'])
def test_url_diagnostic_receipt_failure_is_terminal_io_without_next_send(
        monkeypatch, model, tmp_path, context, event_name):
    module, _, waits = model
    if event_name == 'dohod_redirect_followed':
        response = wire_reply(b'unread diagnostic redirect', status=302,
            headers=[('Location', SLASH_OVERVIEW)])
    else:
        response = wire_reply(discovery('https://foreign.invalid/' + URL_SECRET,
            '/ik/analytics/dividend/aaa'))
    trace = transport(monkeypatch, module, [(OVERVIEW, response)])
    written = writers(monkeypatch, module, tmp_path)
    original = module._dohod_event

    def fail_receipt(logging_context, level, event, message, **kwargs):
        if event == event_name:
            assert response.close_calls == 1
            raise RuntimeError('LOGGING_INCOMPLETE')
        return original(logging_context, level, event, message, **kwargs)

    monkeypatch.setattr(module, '_dohod_event', fail_receipt)
    with pytest.raises(RuntimeError, match='LOGGING_INCOMPLETE'):
        if event_name == 'dohod_redirect_followed':
            module.get_main(OVERVIEW, logging_context=context)
        else:
            module.get_page_info(OVERVIEW, logging_context=context)
    assert sent_urls(trace) == [OVERVIEW] and waits == [] and written == []
    values = assert_safe_events(context)
    final = [event for event in values if event['event'] == 'dohod_acquisition_failed']
    assert len(final) == 1 and final[0]['error']['category'] == 'IO'
    assert not any(event['event'] == 'dohod_request_retry' for event in values)


def test_redirect_close_only_timeout_preserves_identity_and_never_retries(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    error = requests.Timeout(URL_SECRET)
    response = wire_reply(b'unread close failure', status=302,
        headers=[('Location', SLASH_OVERVIEW)], close_error=error)
    created, closed = sessions(monkeypatch)
    trace = transport(monkeypatch, module, [(OVERVIEW, response)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(requests.Timeout) as caught:
        module.get_main(OVERVIEW, logging_context=context)
    assert caught.value is error
    assert response.close_calls == response.raw.closes == 1
    assert response.raw.reads == response.raw.releases == 0
    assert sent_urls(trace) == [OVERVIEW] and waits == [] and written == []
    assert len(created) == 1 and closed == created
    final = [event for event in assert_safe_events(context)
             if event['event'] == 'dohod_acquisition_failed']
    assert len(final) == 1 and final[0]['error']['category'] == 'IO'


def test_session_close_only_timeout_is_io_and_does_not_repeat_successful_http(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    error = requests.Timeout(URL_SECRET)
    created, closed = sessions(monkeypatch, close_errors={0: error})
    response = wire_reply(overview_html())
    trace = transport(monkeypatch, module, [(OVERVIEW, response)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(requests.Timeout) as caught:
        module.get_main(OVERVIEW, logging_context=context)
    assert caught.value is error
    assert sent_urls(trace) == [OVERVIEW] and waits == [] and written == []
    assert_consumed(response)
    assert len(created) == 1 and closed == created
    final = [event for event in assert_safe_events(context)
             if event['event'] == 'dohod_acquisition_failed']
    assert len(final) == 1 and final[0]['error']['category'] == 'IO'


def test_secondary_session_cleanup_cannot_replace_primary_or_block_valid_retry(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    primary = requests.ConnectTimeout('primary ' + URL_SECRET)
    secondary = OSError('secondary ' + URL_SECRET)
    created, closed = sessions(monkeypatch, close_errors={0: secondary})
    response = wire_reply(overview_html())
    trace = transport(monkeypatch, module, [(OVERVIEW, primary), (OVERVIEW, response)])
    written = writers(monkeypatch, module, tmp_path)
    assert_overview(module.get_main(OVERVIEW, logging_context=context))
    assert sent_urls(trace) == [OVERVIEW, OVERVIEW] and waits == [1]
    assert len(created) == 2 and closed == created and len(written) == 1
    assert_consumed(response)
    assert_safe_events(context)


def test_secondary_response_and_session_cleanup_preserve_policy_failure(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    response = wire_reply(b'unread rejected target', status=302,
        headers=[('Location', 'https://foreign.invalid/' + URL_SECRET)],
        close_error=requests.Timeout('response cleanup ' + URL_SECRET))
    created, closed = sessions(monkeypatch,
        close_errors={0: OSError('session cleanup ' + URL_SECRET)})
    trace = transport(monkeypatch, module, [(OVERVIEW, response)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises((ValueError, requests.RequestException),
                       match='REDIRECT_LOCATION_INVALID'):
        module.get_main(OVERVIEW, logging_context=context)
    assert sent_urls(trace) == [OVERVIEW] and waits == [] and written == []
    assert response.close_calls == response.raw.closes == 1
    assert response.raw.reads == response.raw.releases == 0
    assert len(created) == 1 and closed == created
    assert_safe_events(context)


@pytest.mark.parametrize('expiry', ['before_warning', 'after_warning', 'after_wait'])
def test_late_hop_retry_feasibility_uses_same_budget_before_after_warning_and_wait(
        monkeypatch, model, tmp_path, context, expiry):
    module, clock, waits = model
    redirect = wire_reply(b'unread budget transition', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    original_close = redirect.close

    def close():
        original_close()
        clock[0] = 34 if expiry == 'before_warning' else 32

    redirect.close = close
    if expiry == 'after_warning':
        original_retry = module._retry_event

        def warning(*args, **kwargs):
            result = original_retry(*args, **kwargs)
            clock[0] = 35
            return result

        monkeypatch.setattr(module, '_retry_event', warning)
    elif expiry == 'after_wait':
        original_wait = module._retry_wait

        def wait(delay):
            original_wait(delay)
            clock[0] = 35

        monkeypatch.setattr(module, '_retry_wait', wait)
    busy = wire_reply(b'unread budget-limited failure', status=503)
    trace = transport(monkeypatch, module, [(OVERVIEW, redirect), (SLASH_OVERVIEW, busy)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(TimeoutError, match='DOHOD_BUDGET_EXHAUSTED') as caught:
        module.get_main(OVERVIEW, logging_context=context)
    assert isinstance(caught.value.__cause__, requests.HTTPError)
    assert caught.value.__cause__.response is busy
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW] and written == []
    assert waits == ([1] if expiry == 'after_wait' else [])
    assert_unread_closed(redirect)
    assert_unread_closed(busy)
    values = assert_safe_events(context)
    assert sum(event['event'] == 'dohod_request_retry' for event in values) == (
        0 if expiry == 'before_warning' else 1)


def test_discovery_rejections_are_aggregated_by_fixed_reason_without_raw_href(
        monkeypatch, model, tmp_path, context, capsys):
    module, _, waits = model
    page = wire_reply(discovery(
        'https://foreign.invalid/' + URL_SECRET,
        'https://foreign.invalid/ik/analytics/dividend/a',
        'https://another.invalid/ik/analytics/dividend/b',
        AAA + '?' + URL_SECRET, BBB + '?other=fixture',
        '/ordinary-navigation', '', '#local', '/ik/analytics/dividend/aaa'))
    detail = wire_reply(detail_html())
    trace = transport(monkeypatch, module, [(OVERVIEW, page), (AAA, detail)])
    written = writers(monkeypatch, module, tmp_path)
    assert_detail(module.get_page_info(OVERVIEW, logging_context=context))
    assert sent_urls(trace) == [OVERVIEW, AAA] and waits == [] and written == []
    warnings = [event for event in assert_safe_events(context)
                if event['event'] == 'dohod_url_rejected']
    assert len(warnings) == 2
    assert all(event['level'] == 'WARNING' for event in warnings)
    assert sum(event['message'].endswith('URL_HOST_NOT_ALLOWED') for event in warnings) == 1
    assert sum(event['message'].endswith('URL_QUERY_NOT_ALLOWED') for event in warnings) == 1
    captured = capsys.readouterr()
    assert URL_SECRET not in captured.out + captured.err


def test_accepted_redirect_receipt_follows_cleanup_and_precedes_next_send(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    redirect = wire_reply(URL_SECRET.encode(), status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    final = wire_reply(overview_html())
    trace = transport(monkeypatch, module, [(OVERVIEW, redirect), (SLASH_OVERVIEW, final)])
    original_send = requests.adapters.HTTPAdapter.send

    def send(adapter, request, **kwargs):
        if request.url == SLASH_OVERVIEW:
            assert_unread_closed(redirect)
            followed = [event for event in assert_safe_events(context)
                        if event['event'] == 'dohod_redirect_followed']
            assert len(followed) == 1 and followed[0]['level'] == 'INFO'
        return original_send(adapter, request, **kwargs)

    monkeypatch.setattr(requests.adapters.HTTPAdapter, 'send', send)
    written = writers(monkeypatch, module, tmp_path)
    assert_overview(module.get_main(OVERVIEW, logging_context=context))
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW]
    assert len(written) == 1 and waits == []


@pytest.mark.parametrize('reverse', [False, True])
def test_same_import_redirect_base_then_slashless_base_and_reverse(
        monkeypatch, model, tmp_path, context, reverse):
    module, clock, waits = model
    originals = tuple(getattr(module, name) for name in
        ('df_overview_full', 'df_years_full', 'df_each_full'))
    redirect = wire_reply(b'unread overview transition', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    slash_page = wire_reply(discovery('aaa', '/ik/analytics/dividend/aaa#duplicate'))
    slashless_page = wire_reply(discovery('dividend/bbb', BBB + '#duplicate'))
    detail_a = wire_reply(detail_html(amount=10))
    detail_b = wire_reply(detail_html(amount=15))
    route_a = [(OVERVIEW, redirect), (SLASH_OVERVIEW, slash_page), (AAA, detail_a)]
    route_b = [(OVERVIEW, slashless_page), (BBB, detail_b)]
    trace = transport(monkeypatch, module, route_b + route_a if reverse else route_a + route_b)
    written = writers(monkeypatch, module, tmp_path)
    first = module.get_page_info(OVERVIEW, logging_context=context)
    snapshots = tuple(frame.copy(deep=True) for frame in first)
    clock[0] += 4
    clock[1] += 4
    second = module.get_page_info(OVERVIEW, logging_context=context)
    if reverse:
        assert_detail(first, amount=15, page_url=BBB, ticker='BBB')
        assert_detail(second, amount=10, page_url=AAA, ticker='AAA')
        assert sent_urls(trace) == [OVERVIEW, BBB, OVERVIEW, SLASH_OVERVIEW, AAA]
    else:
        assert_detail(first, amount=10, page_url=AAA, ticker='AAA')
        assert_detail(second, amount=15, page_url=BBB, ticker='BBB')
        assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW, AAA, OVERVIEW, BBB]
    for retained, snapshot in zip(first, snapshots):
        pd.testing.assert_frame_equal(retained, snapshot, check_exact=True)
    assert_no_accumulation(module, originals)
    assert trace['pending'] == [] and written == [] and waits == []
    assert_unread_closed(redirect)
    for response in (slash_page, slashless_page, detail_a, detail_b):
        assert_consumed(response)
    values = assert_safe_events(context)
    assert sum(value['event'] == 'dohod_request_received' for value in values) == 4
    assert sum(value['event'] == 'dohod_redirect_followed' for value in values) == 1


@pytest.mark.parametrize('forged_url', [OVERVIEW, 'https://foreign.invalid/' + URL_SECRET])
def test_discovery_uses_approved_prepared_url_even_if_response_url_is_forged(
        monkeypatch, model, tmp_path, context, forged_url):
    module, _, waits = model
    redirect = wire_reply(b'unread transition', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    page = wire_reply(discovery('aaa'))
    page.raw.after_read = lambda: setattr(page, 'url', forged_url)
    detail = wire_reply(detail_html())
    trace = transport(monkeypatch, module,
        [(OVERVIEW, redirect), (SLASH_OVERVIEW, page), (AAA, detail)])
    written = writers(monkeypatch, module, tmp_path)
    assert_detail(module.get_page_info(OVERVIEW, logging_context=context))
    assert page.url == forged_url and page.request.url == SLASH_OVERVIEW
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW, AAA]
    assert waits == [] and written == [] and trace['pending'] == []
    assert_unread_closed(redirect)
    assert_consumed(page)
    assert_consumed(detail)
    assert_safe_events(context)


def test_retry_discards_failed_slash_base_before_successful_slashless_discovery(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    redirect = wire_reply(b'unread first attempt', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    unavailable = wire_reply(status=503)
    page = wire_reply(discovery('dividend/bbb', 'aaa'))
    detail = wire_reply(detail_html(amount=15))
    trace = transport(monkeypatch, module, [(OVERVIEW, redirect),
        (SLASH_OVERVIEW, unavailable), (OVERVIEW, page), (BBB, detail)])
    written = writers(monkeypatch, module, tmp_path)
    assert_detail(module.get_page_info(OVERVIEW, logging_context=context),
                  amount=15, page_url=BBB, ticker='BBB')
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW, OVERVIEW, BBB]
    assert waits == [1] and written == [] and trace['pending'] == []
    assert_unread_closed(redirect)
    assert_unread_closed(unavailable)
    assert_consumed(page)
    assert_consumed(detail)
    assert_safe_events(context)


def test_pre_admission_source_expiry_never_exposes_raw_url_in_diagnostics(
        monkeypatch, model, tmp_path, context):
    module, clock, waits = model
    originals = tuple(getattr(module, name) for name in
        ('df_overview_full', 'df_years_full', 'df_each_full'))
    calls = []

    def now():
        calls.append(None)
        clock[0] = 0 if len(calls) == 1 else 600
        return clock[0]

    monkeypatch.setattr(module, '_network_clock', now)
    trace = transport(monkeypatch, module, [])
    written = writers(monkeypatch, module, tmp_path)
    raw = 'https://user:' + URL_SECRET + '@www.dohod.ru/ik/analytics/dividend?'+URL_SECRET
    with pytest.raises(TimeoutError, match='DOHOD_BUDGET_EXHAUSTED') as caught:
        module.get_page_info(raw, logging_context=context)
    assert sent_urls(trace) == [] and waits == [] and written == []
    assert_no_accumulation(module, originals)
    rendered = ''.join(traceback.format_exception(caught.type, caught.value, caught.tb))
    assert URL_SECRET not in rendered
    failures = [event for event in assert_safe_events(context)
                if event['event'] == 'dohod_acquisition_failed']
    assert len(failures) == 1
    error = failures[0]['error']
    assert error['code'] == 'DOHOD_BUDGET_EXHAUSTED' and error['endpoint'] is None
    assert error['null_reasons']['endpoint']


@pytest.mark.parametrize('failure', ['overview_loop', 'detail_identity'])
def test_same_import_redirect_failure_then_b_has_no_url_state_carryover(
        monkeypatch, model, tmp_path, context, failure):
    module, clock, waits = model
    originals = tuple(getattr(module, name) for name in
        ('df_overview_full', 'df_years_full', 'df_each_full'))
    if failure == 'overview_loop':
        first = wire_reply(b'unread O to S', status=302,
            headers=[('Location', SLASH_OVERVIEW)])
        rejected = wire_reply(b'unread S to O', status=302,
            headers=[('Location', OVERVIEW)])
        failing_route = [(OVERVIEW, first), (SLASH_OVERVIEW, rejected)]
        expected_urls = [OVERVIEW, SLASH_OVERVIEW, OVERVIEW, BBB]
        reason = 'REDIRECT_LOOP'
    else:
        first = wire_reply(discovery('dividend/aaa'))
        rejected = wire_reply(b'unread cross-slug A to B', status=302,
            headers=[('Location', BBB)])
        failing_route = [(OVERVIEW, first), (AAA, rejected)]
        expected_urls = [OVERVIEW, AAA, OVERVIEW, BBB]
        reason = 'REDIRECT_DETAIL_IDENTITY_CHANGED'
    page_b = wire_reply(discovery('dividend/bbb', BBB + '#duplicate'))
    detail_b = wire_reply(detail_html(amount=15))
    trace = transport(monkeypatch, module,
        failing_route + [(OVERVIEW, page_b), (BBB, detail_b)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError, match=reason):
        module.get_page_info(OVERVIEW, logging_context=context)
    assert_no_accumulation(module, originals)
    clock[0] += 4
    clock[1] += 4
    frames_b = module.get_page_info(OVERVIEW, logging_context=context)
    assert_detail(frames_b, amount=15, page_url=BBB, ticker='BBB')
    assert sent_urls(trace) == expected_urls
    assert trace['pending'] == [] and waits == [] and written == []
    if failure == 'overview_loop':
        assert_unread_closed(first)
    else:
        assert_consumed(first)
    assert_unread_closed(rejected)
    assert_consumed(page_b)
    assert_consumed(detail_b)
    assert_no_accumulation(module, originals)
    failures = [value for value in assert_safe_events(context)
                if value['event'] == 'dohod_acquisition_failed']
    assert len(failures) == 1 and reason in failures[0]['message']


def test_same_import_detail_parser_failure_then_b_does_not_retain_final_base(
        monkeypatch, model, tmp_path, context):
    module, clock, waits = model
    originals = tuple(getattr(module, name) for name in
        ('df_overview_full', 'df_years_full', 'df_each_full'))
    redirect = wire_reply(b'unread redirect', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    page_a = wire_reply(discovery('aaa'))
    no_tables = wire_reply(b'<html><body><p>no tables in this document</p></body></html>')
    page_b = wire_reply(discovery('dividend/bbb'))
    detail_b = wire_reply(detail_html(amount=15))
    trace = transport(monkeypatch, module, [(OVERVIEW, redirect),
        (SLASH_OVERVIEW, page_a), (AAA, no_tables),
        (OVERVIEW, page_b), (BBB, detail_b)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError, match='DOHOD_DETAIL_PARSE_FAILED') as caught:
        module.get_page_info(OVERVIEW, logging_context=context)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    clock[0] += 4
    clock[1] += 4
    frames_b = module.get_page_info(OVERVIEW, logging_context=context)
    assert_detail(frames_b, amount=15, page_url=BBB, ticker='BBB')
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW, AAA, OVERVIEW, BBB]
    assert trace['pending'] == [] and waits == [] and written == []
    assert_unread_closed(redirect)
    for response in (page_a, no_tables, page_b, detail_b):
        assert_consumed(response)
    assert_no_accumulation(module, originals)
    failures = [event for event in assert_safe_events(context)
                if event['event'] == 'dohod_acquisition_failed']
    assert len(failures) == 1 and failures[0]['error']['code'] == 'DOHOD_DETAIL_PARSE_FAILED'


def test_same_import_canonical_discovery_dedup_is_fresh_for_each_call(
        monkeypatch, model, tmp_path):
    module, clock, waits = model
    pages = [wire_reply(discovery('dividend/aaa', AAA + '#again',
        '//WWW.DOHOD.RU:443/ik/analytics/dividend/%61aa')) for _ in range(2)]
    details = [wire_reply(detail_html(amount=10)), wire_reply(detail_html(amount=15))]
    trace = transport(monkeypatch, module, [(OVERVIEW, pages[0]), (AAA, details[0]),
        (OVERVIEW, pages[1]), (AAA, details[1])])
    written = writers(monkeypatch, module, tmp_path)
    first = module.get_page_info(OVERVIEW)
    snapshots = tuple(frame.copy(deep=True) for frame in first)
    clock[0] += 4
    clock[1] += 4
    second = module.get_page_info(OVERVIEW)
    assert_detail(first, amount=10)
    assert_detail(second, amount=15)
    for retained, snapshot in zip(first, snapshots):
        pd.testing.assert_frame_equal(retained, snapshot, check_exact=True)
    assert sent_urls(trace) == [OVERVIEW, AAA, OVERVIEW, AAA]
    assert trace['pending'] == [] and written == [] and waits == []
    for response in pages + details:
        assert_consumed(response)
    assert_no_accumulation(module)


def test_same_import_two_main_exports_keep_independent_overview_discovery_bases(
        monkeypatch, model, tmp_path, context):
    module, clock, waits = model
    originals = tuple(getattr(module, name) for name in
        ('df_overview_full', 'df_years_full', 'df_each_full'))
    first_redirect = wire_reply(b'unread first overview redirect', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    first_overview = wire_reply(overview_html(amount=10))
    first_discovery = wire_reply(discovery('dividend/aaa', AAA + '#duplicate'))
    first_detail = wire_reply(detail_html(amount=10))
    second_overview = wire_reply(overview_html(amount=15))
    second_redirect = wire_reply(b'unread second discovery redirect', status=302,
        headers=[('Location', SLASH_OVERVIEW)])
    second_discovery = wire_reply(discovery('bbb', BBB + '#duplicate'))
    second_detail = wire_reply(detail_html(amount=15))
    created, closed = sessions(monkeypatch)
    trace = transport(monkeypatch, module, [(OVERVIEW, first_redirect),
        (SLASH_OVERVIEW, first_overview), (OVERVIEW, first_discovery), (AAA, first_detail),
        (OVERVIEW, second_overview), (OVERVIEW, second_redirect),
        (SLASH_OVERVIEW, second_discovery), (BBB, second_detail)])
    written = writers(monkeypatch, module, tmp_path)
    assert module.main(OVERVIEW, logging_context=context) is None
    assert_writers(written)
    retained = [entry[3] for entry in written]
    snapshots = [frame.copy(deep=True) for frame in retained]
    clock[0] += 4
    clock[1] += 4
    assert module.main(OVERVIEW, logging_context=context) is None
    assert_writers(written, FILES + FILES)
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW, OVERVIEW, AAA,
        OVERVIEW, OVERVIEW, SLASH_OVERVIEW, BBB]
    for start, amount, page_url, ticker in ((0, 10, AAA, 'AAA'), (9, 15, BBB, 'BBB')):
        for index in range(start, start + 3):
            assert_overview(written[index][3], amount=amount)
        assert_detail(tuple(written[start + offset][3] for offset in (3, 5, 7)),
            amount=amount, page_url=page_url, ticker=ticker)
        assert_detail(tuple(written[start + offset][3] for offset in (4, 6, 8)),
            amount=amount, page_url=page_url, ticker=ticker)
        for left, right in ((1, 2), (3, 4), (5, 6), (7, 8)):
            pd.testing.assert_frame_equal(written[start + left][3],
                written[start + right][3], check_exact=True)
    for frame, snapshot in zip(retained, snapshots):
        pd.testing.assert_frame_equal(frame, snapshot, check_exact=True)
    assert len(created) == len(closed) == 6
    assert all(sum(session is value for value in closed) == 1 for session in created)
    assert trace['pending'] == [] and waits == []
    for response in (first_redirect, second_redirect):
        assert_unread_closed(response)
    for response in (first_overview, first_discovery, first_detail,
            second_overview, second_discovery, second_detail):
        assert_consumed(response)
    values = assert_safe_events(context)
    assert sum(value['event'] == 'dohod_request_received' for value in values) == 6
    assert sum(value['event'] == 'dohod_redirect_followed' for value in values) == 2
    assert not any(value['event'] == 'dohod_acquisition_failed' for value in values)
    assert_no_accumulation(module, originals)


def test_late_cross_slug_main_failure_preserves_early_three_later_six_hashes(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    targets = tmp_path / 'datasets/dividends/dohodru'
    targets.mkdir(parents=True)
    previous = {name: ('previous-url-fixture:' + name).encode('ascii') for _, name in FILES}
    for name, payload in previous.items():
        (targets / name).write_bytes(payload)
    hashes = {name: hashlib.sha256(payload).hexdigest() for name, payload in previous.items()}
    overview = wire_reply(overview_html())
    page = wire_reply(discovery('dividend/aaa', 'dividend/bbb'))
    first_detail = wire_reply(detail_html())
    rejected = wire_reply(b'unread B content under A identity', status=302,
        headers=[('Location', AAA)])
    trace = transport(monkeypatch, module, [(OVERVIEW, overview), (OVERVIEW, page),
        (AAA, first_detail), (BBB, rejected)])
    written = writers(monkeypatch, module, tmp_path,
        action=lambda number, method, name:
            (targets / name).write_bytes(('updated-url-fixture:' + name).encode('ascii')))
    with pytest.raises(ValueError, match='REDIRECT_DETAIL_IDENTITY_CHANGED'):
        module.main(OVERVIEW, logging_context=context)
    assert_writers(written, FILES[:3])
    for entry in written:
        assert_overview(entry[3])
    for _, name in FILES[:3]:
        assert (targets / name).read_bytes() == ('updated-url-fixture:' + name).encode('ascii')
        assert hashlib.sha256((targets / name).read_bytes()).hexdigest() != hashes[name]
    for _, name in FILES[3:]:
        assert (targets / name).read_bytes() == previous[name]
        assert hashlib.sha256((targets / name).read_bytes()).hexdigest() == hashes[name]
    assert sent_urls(trace) == [OVERVIEW, OVERVIEW, AAA, BBB]
    assert trace['pending'] == [] and waits == []
    for response in (overview, page, first_detail):
        assert_consumed(response)
    assert_unread_closed(rejected)
    failures = [value for value in assert_safe_events(context)
                if value['event'] == 'dohod_acquisition_failed']
    assert len(failures) == 1 and 'REDIRECT_DETAIL_IDENTITY_CHANGED' in failures[0]['message']
    assert_no_accumulation(module)


# Native Requests boundaries; every provider below is an offline fixture.
def _transport_cookie_values(prepared):
    parsed = SimpleCookie()
    parsed.load(prepared.headers.get('Cookie', ''))
    return {name: morsel.value for name, morsel in parsed.items()}


def test_real_cookiejar_applies_addition_path_domain_expiry_and_deletion_on_hop(
        monkeypatch, model, tmp_path):
    module, _, waits = model

    def seed(session, index):
        assert index == 0 and isinstance(session.cookies, CookieJar)
        for name, value in [('retained', 'yes'), ('replace', 'old'), ('delete', 'old')]:
            session.cookies.set(name, value, domain='www.dohod.ru',
                                path='/ik/analytics/dividend')
        session.cookies.set('wrong_path', 'no', domain='www.dohod.ru', path='/elsewhere')
        session.cookies.set('wrong_domain', 'no', domain='foreign.invalid', path='/')
        session.cookies.set('expired_seed', 'no', domain='www.dohod.ru', path='/', expires=1)

    created, closed = sessions(monkeypatch, seed=seed)
    redirect = wire_reply(b'unread cookie redirect', status=302, headers=[
        ('Location', SLASH_OVERVIEW),
        ('Set-Cookie', 'added=yes; Path=/ik/analytics/dividend/; Secure'),
        ('Set-Cookie', 'replace=new; Path=/ik/analytics/dividend; Secure'),
        ('Set-Cookie', 'delete=; Max-Age=0; Path=/ik/analytics/dividend'),
        ('Set-Cookie', 'foreign_response=no; Domain=foreign.invalid; Path=/'),
        ('Set-Cookie', 'other_path=no; Path=/elsewhere'),
        ('Set-Cookie', 'expired_response=no; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Path=/'),
    ])
    final = wire_reply(overview_html())
    trace = transport(monkeypatch, module, [(OVERVIEW, redirect), (SLASH_OVERVIEW, final)])
    written = writers(monkeypatch, module, tmp_path)
    assert_overview(module.get_main(OVERVIEW))
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW] and trace['pending'] == []
    assert _transport_cookie_values(trace['sends'][0][1]) == {
        'retained': 'yes', 'replace': 'old', 'delete': 'old'}
    assert _transport_cookie_values(trace['sends'][1][1]) == {
        'retained': 'yes', 'replace': 'new', 'added': 'yes'}
    assert created == closed and len(created) == 1 and waits == [] and len(written) == 1
    assert created[0].cookies.get('replace', domain='www.dohod.ru',
                                  path='/ik/analytics/dividend') == 'new'
    assert not any(cookie.name in {'delete', 'foreign_response', 'expired_response'}
                   for cookie in created[0].cookies)
    assert_unread_closed(redirect)
    assert_consumed(final)


def test_real_session_cookie_state_is_fresh_after_read_retry_and_next_invocation(
        monkeypatch, model, tmp_path):
    module, _, waits = model
    created, closed = sessions(monkeypatch)
    redirects = [wire_reply(b'unread attempt cookie', status=302, headers=[
        ('Location', SLASH_OVERVIEW), ('Set-Cookie', 'attempt=' + value + '; Path=/')])
        for value in ('first', 'second')]
    failed = wire_reply(overview_html(), error=requests.ReadTimeout('offline read retry'))
    final = wire_reply(overview_html())
    next_call = wire_reply(overview_html())
    trace = transport(monkeypatch, module, [
        (OVERVIEW, redirects[0]), (SLASH_OVERVIEW, failed),
        (OVERVIEW, redirects[1]), (SLASH_OVERVIEW, final), (OVERVIEW, next_call)])
    written = writers(monkeypatch, module, tmp_path)
    assert_overview(module.get_main(OVERVIEW))
    assert_overview(module.get_main(OVERVIEW))
    assert sent_urls(trace) == [OVERVIEW, SLASH_OVERVIEW, OVERVIEW, SLASH_OVERVIEW, OVERVIEW]
    assert [_transport_cookie_values(request) for _, request, _ in trace['sends']] == [
        {}, {'attempt': 'first'}, {}, {'attempt': 'second'}, {}]
    assert len(created) == len(closed) == 3 and created == closed
    assert len({id(session.cookies) for session in created}) == 3
    assert waits == [1] and len(written) == 2 and trace['pending'] == []
    for response in redirects:
        assert_unread_closed(response)
    assert failed.close_calls == 1 and failed.raw.reads == 1
    assert_consumed(final)
    assert_consumed(next_call)
    assert_no_accumulation(module)


@pytest.mark.parametrize('ca_variable', ['REQUESTS_CA_BUNDLE', 'CURL_CA_BUNDLE'])
def test_real_session_preparation_preserves_netrc_default_headers_and_environment_tls(
        monkeypatch, model, tmp_path, ca_variable):
    module, _, waits = model
    ca_bundle = 'C:/offline-c3/' + ca_variable.lower() + '.pem'
    monkeypatch.delenv('REQUESTS_CA_BUNDLE', raising=False)
    monkeypatch.delenv('CURL_CA_BUNDLE', raising=False)
    monkeypatch.setenv(ca_variable, ca_bundle)
    netrc_calls, proxy_calls, merge_calls = [], [], []

    def netrc(url):
        netrc_calls.append(url)
        return ('offline-user', 'offline-password')

    def proxies(url, no_proxy=None):
        proxy_calls.append((url, no_proxy))
        return {'https': 'http://offline-proxy.invalid:8080', 'no_proxy': 'offline.invalid'}

    monkeypatch.setattr(requests.sessions, 'get_netrc_auth', netrc)
    monkeypatch.setattr(requests.sessions, 'get_environ_proxies', proxies)
    real_merge = requests.Session.merge_environment_settings

    def merge(session, url, proxies, stream, verify, cert):
        merge_calls.append((url, dict(proxies), stream, verify, cert))
        return real_merge(session, url, proxies, stream, verify, cert)

    monkeypatch.setattr(requests.Session, 'merge_environment_settings', merge)

    def seed(session, index):
        assert index == 0 and session.trust_env is True
        session.headers['X-Offline-Session'] = 'session-value'
        session.verify = False
        session.cert = ('C:/offline-c3/client.pem', 'C:/offline-c3/client.key')
        session.proxies['http'] = 'http://session-proxy.invalid:8080'

    created, closed = sessions(monkeypatch, seed=seed)
    redirect = wire_reply(b'unread settings redirect', status=302,
                          headers=[('Location', SLASH_OVERVIEW)])
    final = wire_reply(overview_html())
    trace = transport(monkeypatch, module, [(OVERVIEW, redirect), (SLASH_OVERVIEW, final)])
    written = writers(monkeypatch, module, tmp_path)
    assert_overview(module.get_main(OVERVIEW))
    assert netrc_calls == [OVERVIEW, SLASH_OVERVIEW]
    assert proxy_calls == [(OVERVIEW, None), (SLASH_OVERVIEW, None)]
    assert merge_calls == [(OVERVIEW, {}, True, None, None),
                           (SLASH_OVERVIEW, {}, True, None, None)]
    for _, prepared, kwargs in trace['sends']:
        assert prepared.headers['Authorization'] == requests.auth._basic_auth_str(
            'offline-user', 'offline-password')
        assert prepared.headers['X-Offline-Session'] == 'session-value'
        assert prepared.headers['Accept'] == requests.utils.default_headers()['Accept']
        assert prepared.headers['Connection'] == requests.utils.default_headers()['Connection']
        assert prepared.headers['Accept-Encoding'] == requests.utils.default_headers()['Accept-Encoding']
        assert prepared.headers['User-Agent'] == module.headers['User-Agent']
        assert kwargs == {'timeout': (10, 10), 'stream': True, 'verify': ca_bundle,
                          'cert': ('C:/offline-c3/client.pem', 'C:/offline-c3/client.key'),
                          'proxies': {'http': 'http://session-proxy.invalid:8080',
                                      'https': 'http://offline-proxy.invalid:8080',
                                      'no_proxy': 'offline.invalid'}}
    assert created == closed and len(created) == 1 and waits == [] and len(written) == 1
    assert_unread_closed(redirect)
    assert_consumed(final)


@pytest.mark.parametrize('mutated', [
    SLASH_OVERVIEW, 'https://foreign.invalid/ik/analytics/dividend',
    OVERVIEW + '#' + URL_SECRET,
])
def test_prepared_url_mutation_is_refused_before_adapter_selection(
        monkeypatch, model, tmp_path, mutated):
    module, _, waits = model
    created, closed = sessions(monkeypatch)
    monkeypatch.setattr(requests.sessions, 'get_netrc_auth', lambda url: None)
    real_prepare = requests.Session.prepare_request
    prepared_calls, adapter_calls = [], []

    def prepare(session, request):
        prepared = real_prepare(session, request)
        prepared_calls.append(prepared)
        prepared.url = mutated
        return prepared

    def get_adapter(session, url):
        adapter_calls.append(url)
        raise AssertionError('Adapter selected after a refused PreparedRequest mutation')

    monkeypatch.setattr(requests.Session, 'prepare_request', prepare)
    monkeypatch.setattr(requests.Session, 'get_adapter', get_adapter)
    trace = transport(monkeypatch, module, [])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError, match='PREPARED_URL_CHANGED') as caught:
        module.get_main(OVERVIEW)
    assert str(caught.value) == 'PREPARED_URL_CHANGED' and caught.value.__cause__ is None
    assert len(prepared_calls) == 1 and adapter_calls == [] and trace['sends'] == []
    assert created == closed and len(created) == 1 and waits == [] and written == []
    assert_no_accumulation(module)


def test_real_httpadapter_send_uses_unfollowed_unread_zero_retry_pool_requests(
        monkeypatch, model, tmp_path):
    module, _, waits = model
    monkeypatch.setattr(requests.sessions, 'get_netrc_auth', lambda url: None)
    monkeypatch.setattr(requests.sessions, 'get_environ_proxies', lambda url, no_proxy=None: {})
    monkeypatch.delenv('REQUESTS_CA_BUNDLE', raising=False)
    monkeypatch.delenv('CURL_CA_BUNDLE', raising=False)
    created, closed = sessions(monkeypatch)
    real_send = requests.adapters.HTTPAdapter.send
    real_build = requests.adapters.HTTPAdapter.build_response
    redirect_raw = WireBody(b'unread native adapter redirect', [('Location', SLASH_OVERVIEW)])
    redirect_raw.status, redirect_raw.reason = 302, 'offline redirect'
    final_raw = WireBody(overview_html(), [('Content-Type', 'text/html; charset=utf-8')])
    final_raw.status, final_raw.reason = 200, 'offline final'
    pending = [redirect_raw, final_raw]
    pool_calls, connection_calls, tls_calls, responses = [], [], [], []

    class OfflinePool:
        def urlopen(self, **kwargs):
            if pool_calls:
                assert redirect_raw.reads == 0
                assert redirect_raw.closes == redirect_raw.releases == 1
                assert responses[0].close_calls == 1
            pool_calls.append(kwargs)
            assert pending, 'Unexpected urllib3 pool call'
            return pending.pop(0)

    pool = OfflinePool()

    def connection(adapter, prepared, verify, proxies=None, cert=None):
        connection_calls.append((adapter, prepared.url, verify, dict(proxies), cert))
        return pool

    def cert_verify(adapter, connection, url, verify, cert):
        tls_calls.append((connection, url, verify, cert))

    def build(adapter, prepared, raw):
        response = real_build(adapter, prepared, raw)
        response.close_calls = 0
        original_close = response.close

        def close():
            response.close_calls += 1
            original_close()

        response.close = close
        responses.append(response)
        return response

    monkeypatch.setattr(requests.adapters.HTTPAdapter, 'get_connection_with_tls_context', connection)
    monkeypatch.setattr(requests.adapters.HTTPAdapter, 'cert_verify', cert_verify)
    monkeypatch.setattr(requests.adapters.HTTPAdapter, 'build_response', build)
    written = writers(monkeypatch, module, tmp_path)
    assert_overview(module.get_main(OVERVIEW))
    assert requests.adapters.HTTPAdapter.send is real_send
    assert [entry[1] for entry in connection_calls] == [OVERVIEW, SLASH_OVERVIEW]
    assert connection_calls[0][0] is connection_calls[1][0]
    assert tls_calls == [(pool, OVERVIEW, True, None), (pool, SLASH_OVERVIEW, True, None)]
    assert [call['url'] for call in pool_calls] == ['/ik/analytics/dividend',
                                                   '/ik/analytics/dividend/']
    for call in pool_calls:
        assert call['method'] == 'GET' and call['body'] is None and call['chunked'] is False
        assert call['redirect'] is False and call['assert_same_host'] is False
        assert call['preload_content'] is False and call['decode_content'] is False
        assert call['retries'].total == 0 and call['retries'].read is False
        assert call['timeout'].connect_timeout == call['timeout'].read_timeout == 10
    assert len(responses) == 2 and all(isinstance(value, requests.Response) for value in responses)
    assert_unread_closed(responses[0])
    assert_consumed(responses[1])
    assert created == closed and len(created) == 1 and waits == [] and len(written) == 1
    assert pending == []

@pytest.mark.parametrize('error_type', [TimeoutError, requests.Timeout])
def test_cookie_processing_timeout_is_sanitized_terminal_and_cannot_trigger_retry(
        monkeypatch, model, tmp_path, context, capsys, error_type):
    module, _, waits = model
    created, closed = sessions(monkeypatch)
    response = wire_reply(b'unread cookie failure', status=302,
                          headers=[('Location', SLASH_OVERVIEW)])
    trace = transport(monkeypatch, module, [(OVERVIEW, response)])
    written = writers(monkeypatch, module, tmp_path)
    cookie_calls = []

    def fail_cookie_processing(jar, prepared, raw):
        cookie_calls.append((jar, prepared.url, raw))
        raise error_type(URL_SECRET)

    monkeypatch.setattr(requests.cookies, 'extract_cookies_to_jar', fail_cookie_processing)
    with pytest.raises(ValueError, match='COOKIE_PROCESSING_FAILED') as caught:
        module.get_main(OVERVIEW, logging_context=context)
    rendered = ''.join(traceback.format_exception(caught.type, caught.value, caught.tb))
    assert str(caught.value) == 'COOKIE_PROCESSING_FAILED'
    assert caught.value.__cause__ is None and URL_SECRET not in rendered
    assert len(cookie_calls) == 1 and cookie_calls[0][1:] == (OVERVIEW, response.raw)
    assert isinstance(cookie_calls[0][0], CookieJar)
    assert sent_urls(trace) == [OVERVIEW] and trace['pending'] == []
    assert created == closed and len(created) == 1 and waits == [] and written == []
    assert_unread_closed(response)
    values = assert_safe_events(context)
    assert not any(event['event'] == 'dohod_request_retry' for event in values)
    captured = capsys.readouterr()
    assert URL_SECRET not in captured.out + captured.err
    assert_no_accumulation(module)
