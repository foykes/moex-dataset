"""Synthetic real-parser compatibility oracles, not a live dohod source contract."""

from contextlib import contextmanager
import hashlib
from io import BytesIO, IOBase
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pandas.io.html as pandas_html
import pytest
import requests

import run_logging
from tests.shared.c2.test_dohod_compatibility import FILES, assert_writers
from tests.shared.c2.test_dohod_network import (
    BASE, CANARY, DETAIL, DISCOVERY, URL, assert_requests, context, events,
    failed, model, reply, source, writers,
)


RATE_COLUMNS = ['текущая доходность', 'доля от прибыли', 'индекс DSI', 'page_url', 'ticker']
OVERVIEW_COLUMNS = ['Тикер', 'Компания', 'Дивиденд']
BODY_CANARY = 'C3_BODY_MARKER_572'
ENCODINGS = [('utf-8', 'utf-8'), ('cp1251', 'windows-1251')]


def document(tables, codec='utf-8', declaration='utf-8'):
    return ('<!doctype html><html><head><meta charset="' + declaration
            + '"></head><body>' + tables + '<p>' + BODY_CANARY + ' ' + CANARY
            + '</p></body></html>').encode(codec)


def overview_html(codec='utf-8', declaration='utf-8', *, amount=10, noise=False):
    tables = ('<table><tr><th>Тикер</th><th>Компания</th><th>Дивиденд</th>'
        '<th>Unnamed: 0</th><th>Капитализация, sorting</th></tr>'
        '<tr><td>AAA</td><td>Газпром</td><td>' + str(amount)
        + '</td><td>obsolete</td><td>obsolete</td></tr>'
        '<tr><td>AAA</td><td>Газпром</td><td>' + str(amount)
        + '</td><td>obsolete</td><td>obsolete</td></tr></table>')
    if noise:
        # Исходное C3-01 тело остаётся parser oracle, но уже не consumer-positive.
        tables = ('<table><tr><th>Служебное</th></tr><tr><td>Не источник</td></tr></table>'
                  + tables)
    return document(tables, codec, declaration)


def detail_html(table_count=3, codec='utf-8', declaration='utf-8', *, amount=10):
    percentage = '10%' if amount == 10 else '15%'
    tables = ('<table><tr><td>' + percentage + '</td><td>20%</td><td>0.3</td></tr>'
        '<tr><td>текущая доходность</td><td>доля от прибыли</td><td>индекс DSI</td></tr>'
        '<tr><td>' + percentage + '</td><td>20%</td><td>0.3</td></tr></table>'
        '<table><tr><th>Год</th><th>Сумма</th></tr>'
        '<tr><td>2024</td><td>' + str(amount) + '</td></tr>'
        '<tr><td>2024</td><td>' + str(amount) + '</td></tr></table>')
    if table_count == 3:
        tables += ('<table><tr><th>Дата</th><th>Выплата</th></tr>'
            '<tr><td>2024-07-11</td><td>' + str(amount) + '</td></tr></table>')
    return document(tables, codec, declaration)


def expected_overview(amount=10, *, companies=None):
    companies = ['Газпром', 'Газпром'] if companies is None else companies
    return pd.DataFrame([['AAA', companies[0], amount], ['AAA', companies[1], amount]],
        columns=OVERVIEW_COLUMNS).astype({'Тикер': 'object', 'Компания': 'object', 'Дивиденд': 'int64'})


def expected_detail(table_count=3, *, amount=10, page_url=DETAIL, ticker='AAA'):
    percentage = '10%' if amount == 10 else '15%'
    rate = pd.DataFrame([
        [percentage, '20%', '0.3', page_url, ticker],
        [percentage, '20%', '0.3', page_url, ticker],
    ], columns=RATE_COLUMNS, index=[0, 2]).astype('object')
    rate.columns.name = 1
    years = pd.DataFrame([[2024, amount, page_url, ticker], [2024, amount, page_url, ticker]],
        columns=['Год', 'Сумма', 'page_url', 'ticker']).astype({
            'Год': 'int64', 'Сумма': 'int64', 'page_url': 'object', 'ticker': 'object'})
    payments = pd.DataFrame()
    if table_count == 3:
        payments = pd.DataFrame([['2024-07-11', amount, page_url, ticker]],
            columns=['Дата', 'Выплата', 'page_url', 'ticker']).astype({
                'Дата': 'object', 'Выплата': 'int64', 'page_url': 'object', 'ticker': 'object'})
    return rate, years, payments


def assert_overview(frame, amount=10, *, companies=None):
    pd.testing.assert_frame_equal(frame, expected_overview(amount, companies=companies),
                                  check_exact=True)
    assert frame.index.tolist() == [0, 1]
    assert frame.index.name is None and frame.columns.name is None
    assert frame.duplicated().tolist() == ([False, True] if companies is None else [False, False])


def assert_detail(frames, table_count=3, *, amount=10, page_url=DETAIL, ticker='AAA'):
    assert type(frames) is tuple and len(frames) == 3
    for actual, expected in zip(frames, expected_detail(
            table_count, amount=amount, page_url=page_url, ticker=ticker)):
        pd.testing.assert_frame_equal(actual, expected, check_exact=True)
        assert actual.index.name is None
    assert frames[0].index.tolist() == [0, 2] and frames[0].columns.name == 1
    assert frames[1].index.tolist() == [0, 1] and frames[1].columns.name is None
    assert frames[0].duplicated().tolist() == [False, True]
    assert frames[1].duplicated().tolist() == [False, True]


def assert_empty_frames(frames):
    assert type(frames) is tuple and len(frames) == 3
    for frame in frames:
        pd.testing.assert_frame_equal(frame, pd.DataFrame(), check_exact=True)


def assert_closed(responses):
    assert all(response.close_calls == 1 and response.raw.reads == 1
               and response.raw.closes == 0 and response.raw.releases == 1 for response in responses)
    assert len({id(response) for response in responses}) == len(responses)


def assert_no_accumulation(module, original_globals=None):
    assert all(getattr(module, name).empty for name in
               ('df_overview_full', 'df_years_full', 'df_each_full'))
    if original_globals is not None:
        assert all(getattr(module, name) is original for name, original in zip(
            ('df_overview_full', 'df_years_full', 'df_each_full'), original_globals))


def two_page_discovery():
    links = ['/ik/analytics/dividend/aaa', '/ik/analytics/dividend/bbb']
    html = ''.join('<a href="' + link + '">fixture</a>' for link in links).encode('ascii')
    # C3-02 preserves the first DOM appearance of each canonical detail URL.
    urls = [URL + '/aaa', URL + '/bbb']
    return html, urls


def partial_frames_observer(monkeypatch, module):
    real_concat = pd.concat
    assembled = []
    def concat(*args, **kwargs):
        frame = real_concat(*args, **kwargs)
        assembled.append(frame)
        return frame
    monkeypatch.setattr(module.pd, 'concat', concat)
    return assembled


def assert_private_messages(session, *, received, failed_count=0):
    values = events(session)
    assert sum(event['event'] == 'dohod_request_received' for event in values) == received
    assert sum(event['event'] == 'dohod_acquisition_failed' for event in values) == failed_count
    assert not any(event['event'] == 'dohod_request_retry' for event in values)
    assert BODY_CANARY not in json.dumps(values, ensure_ascii=False)
    assert all('request_phase=DECODE' not in event['message'] for event in values
               if event['event'] == 'dohod_acquisition_failed')
    return values


@contextmanager
def fresh_context(tmp_path, name):
    session = run_logging.configure_logging(tmp_path / 'logs' / name, run_id='c3-' + name,
        environment='offline', producer_sha=BASE,
        config={'console_level': 'ERROR'}, sensitive_values=(CANARY,))
    try:
        yield session
    finally:
        run_logging.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)


def real_parser_observer(monkeypatch, module, *, fallback=None, after_parse=None):
    """Observe byte identity and rewind; only the first backend may be faulted."""
    real_read = pd.read_html
    real_lxml = pandas_html._LxmlFrameParser._build_doc
    real_bs4 = pandas_html._BeautifulSoupHtml5LibFrameParser._build_doc
    trace = {'bodies': [], 'table_counts': [], 'buffers': [], 'lxml': [], 'bs4': []}

    def body(reader):
        assert type(reader) is SimpleNamespace
        assert set(vars(reader)) == {'read', 'seek', 'seekable', 'tell'}
        assert not isinstance(reader, IOBase)
        assert reader.seekable() and reader.tell() == 0
        buffer = reader.read.__self__
        assert isinstance(buffer, BytesIO)
        assert all(getattr(reader, name).__self__ is buffer for name in
                   ('read', 'seek', 'seekable', 'tell'))
        raw = buffer.getvalue()  # Test observation only; production facade omits getvalue.
        assert type(reader.read(0)) is bytes and reader.tell() == 0
        return raw, buffer

    def read(reader, **kwargs):
        assert kwargs == {'encoding': None}
        raw, buffer = body(reader)
        trace['bodies'].append(raw)
        trace['buffers'].append(buffer)
        frames = real_read(reader, **kwargs)
        trace['table_counts'].append(len(frames))
        if after_parse:
            after_parse(raw)
        return frames

    def lxml(parser):
        raw, buffer = body(parser.io)
        trace['lxml'].append((parser.io, raw, buffer))
        if fallback is not None:
            count = 11 if fallback == 'prefix' else len(raw)
            assert parser.io.read(count) == raw[:count]
            assert parser.io.tell() == count
            raise ValueError('C3_FORCED_FIRST_BACKEND_FAILURE; ' + CANARY)
        return real_lxml(parser)

    def bs4(parser):
        raw, buffer = body(parser.io)  # Assert rewind before any observer read/reset.
        trace['bs4'].append((parser.io, raw, buffer))
        assert parser.io.read() == raw
        parser.io.seek(0)
        return real_bs4(parser)

    monkeypatch.setattr(module.pd, 'read_html', read)
    monkeypatch.setattr(pandas_html._LxmlFrameParser, '_build_doc', lxml)
    monkeypatch.setattr(pandas_html._BeautifulSoupHtml5LibFrameParser, '_build_doc', bs4)
    return trace


def assert_parser_trace(trace, bodies, counts, *, fallback=None):
    assert trace['bodies'] == bodies and trace['table_counts'] == counts
    assert len(trace['lxml']) == len(bodies)
    assert len(trace['bs4']) == (len(bodies) if fallback is not None else 0)
    if fallback is not None:
        for first, second in zip(trace['lxml'], trace['bs4']):
            assert first[0] is second[0] and first[1] == second[1] and first[2] is second[2]
    assert len({id(buffer) for buffer in trace['buffers']}) == len(bodies)
    assert all(buffer.closed for buffer in trace['buffers'])


@pytest.mark.parametrize('codec,declaration', ENCODINGS)
@pytest.mark.parametrize('fallback', [None, 'prefix', 'full'])
def test_real_overview_cyrillic_schema_duplicates_and_backend_fallback(
        monkeypatch, model, tmp_path, context, codec, declaration, fallback):
    module, _, waits = model
    html = overview_html(codec, declaration)
    response = reply(html)
    calls = source(monkeypatch, module, [(URL, response)])
    written = writers(monkeypatch, module, tmp_path)
    trace = real_parser_observer(monkeypatch, module, fallback=fallback)
    frame = module.get_main(URL, logging_context=context)
    assert_overview(frame)
    assert_writers(written, FILES[:1])
    assert_overview(written[0][3])
    assert_parser_trace(trace, [html], [1], fallback=fallback)
    assert_requests(calls)
    assert len(calls) == 1 and waits == []
    assert_closed([response])
    assert_private_messages(context, received=1)
    assert_no_accumulation(module)


@pytest.mark.parametrize('codec,declaration', ENCODINGS)
@pytest.mark.parametrize('fallback', [None, 'prefix', 'full'])
def test_original_noisy_overview_remains_real_adapter_oracle_but_consumer_rejects(
        monkeypatch, model, tmp_path, context, codec, declaration, fallback):
    module, _, waits = model
    html = overview_html(codec, declaration, noise=True)
    trace = real_parser_observer(monkeypatch, module, fallback=fallback)
    tables = module._read_dohod_html(html)
    pd.testing.assert_frame_equal(tables[0], pd.DataFrame(
        [['Не источник']], columns=['Служебное']), check_exact=True)
    raw_overview = pd.DataFrame([
        ['AAA', 'Газпром', 10, 'obsolete', 'obsolete'],
        ['AAA', 'Газпром', 10, 'obsolete', 'obsolete'],
    ], columns=OVERVIEW_COLUMNS + ['Unnamed: 0', 'Капитализация, sorting'])
    pd.testing.assert_frame_equal(tables[1], raw_overview, check_exact=True)

    response = reply(html)
    calls = source(monkeypatch, module, [(URL, response)])
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError, match='DOHOD_UNKNOWN_NONEMPTY_TABLE') as caught:
        module.get_main(URL, logging_context=context)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    assert_parser_trace(trace, [html, html], [2, 2], fallback=fallback)
    assert_requests(calls)
    assert len(calls) == 1 and written == waits == []
    assert_closed([response])
    values = assert_private_messages(context, received=1, failed_count=1)
    error = values[-1]['error']
    assert error['code'] == 'DOHOD_UNKNOWN_NONEMPTY_TABLE' and error['endpoint'] is None
    failed(context)
    assert_no_accumulation(module)


@pytest.mark.parametrize('codec,declaration', ENCODINGS)
@pytest.mark.parametrize('table_count', [2, 3])
@pytest.mark.parametrize('fallback', [None, 'prefix', 'full'])
def test_real_detail_cyrillic_two_three_tables_and_backend_fallback(
        monkeypatch, model, tmp_path, context, codec, declaration, table_count, fallback):
    module, _, waits = model
    html = detail_html(table_count, codec, declaration)
    responses = [reply(DISCOVERY), reply(html)]
    calls = source(monkeypatch, module, list(zip([URL, DETAIL], responses)))
    written = writers(monkeypatch, module, tmp_path)
    trace = real_parser_observer(monkeypatch, module, fallback=fallback)
    frames = module.get_page_info(URL, logging_context=context)
    # Literal frames проверяются снаружи parser handler: observer faults не дают PASS.
    assert_detail(frames, table_count)
    assert_parser_trace(trace, [html], [table_count], fallback=fallback)
    assert_requests(calls)
    assert len(calls) == 2 and written == waits == []
    assert_closed(responses)
    assert_private_messages(context, received=2)
    assert_no_accumulation(module)


@pytest.mark.parametrize('stage', ['overview', 'detail'])
def test_http_body_that_names_local_html_is_document_content_only(
        monkeypatch, model, tmp_path, context, stage):
    module, _, waits = model
    path = tmp_path / 'local-oracle.html'
    html = overview_html() if stage == 'overview' else detail_html()
    path.write_bytes(html)
    original_hash = hashlib.sha256(html).hexdigest()
    body = path.resolve().as_posix().encode('ascii')
    responses = [reply(body)] if stage == 'overview' else [reply(DISCOVERY), reply(body)]
    urls = [URL] if stage == 'overview' else [URL, DETAIL]
    calls = source(monkeypatch, module, list(zip(urls, responses)))
    written = writers(monkeypatch, module, tmp_path)
    # Genuine read_html: raw HTTP bytes must not be interpreted as this local file path.
    if stage == 'overview':
        with pytest.raises(ValueError):
            module.get_main(URL, logging_context=context)
        assert_private_messages(context, received=1, failed_count=1)
        failed(context)
    else:
        with pytest.raises(ValueError, match='DOHOD_DETAIL_PARSE_FAILED') as caught:
            module.get_page_info(URL, logging_context=context)
        assert caught.value.__context__ is None and caught.value.__cause__ is None
        assert_private_messages(context, received=2, failed_count=1)
        failed(context)
    assert_requests(calls)
    assert len(calls) == len(responses) and written == waits == []
    assert_closed(responses)
    assert path.read_bytes() == html
    assert hashlib.sha256(path.read_bytes()).hexdigest() == original_hash
    assert_no_accumulation(module)


@pytest.mark.parametrize('stage', ['overview', 'detail'])
@pytest.mark.parametrize('codec,declaration', ENCODINGS)
def test_real_recoverable_malformed_html_preserves_literal_rows(
        monkeypatch, model, tmp_path, context, stage, codec, declaration):
    module, _, waits = model
    html = (overview_html(codec, declaration) if stage == 'overview'
            else detail_html(3, codec, declaration)).replace(b'</tr>', b'', 1)
    responses = [reply(html)] if stage == 'overview' else [reply(DISCOVERY), reply(html)]
    urls = [URL] if stage == 'overview' else [URL, DETAIL]
    calls = source(monkeypatch, module, list(zip(urls, responses)))
    written = writers(monkeypatch, module, tmp_path)
    trace = real_parser_observer(monkeypatch, module, fallback='full')
    if stage == 'overview':
        assert_overview(module.get_main(URL, logging_context=context))
        assert_writers(written, FILES[:1])
    else:
        assert_detail(module.get_page_info(URL, logging_context=context))
        assert written == []
    assert_parser_trace(trace, [html], [1 if stage == 'overview' else 3], fallback='full')
    assert_requests(calls)
    assert len(calls) == len(responses) and waits == []
    assert_closed(responses)
    assert_private_messages(context, received=len(responses))


def test_invalid_declared_utf8_byte_retains_real_html5lib_recovery(
        monkeypatch, model, tmp_path, context):
    module, _, waits = model
    html = overview_html().replace('Газпром'.encode('utf-8'), b'\xff', 1)
    response = reply(html)
    calls = source(monkeypatch, module, [(URL, response)])
    written = writers(monkeypatch, module, tmp_path)
    trace = real_parser_observer(monkeypatch, module, fallback='full')
    frame = module.get_main(URL, logging_context=context)
    assert_overview(frame, companies=['\ufffd', 'Газпром'])
    assert_writers(written, FILES[:1])
    assert_parser_trace(trace, [html], [1], fallback='full')
    assert_requests(calls)
    assert len(calls) == 1 and waits == []
    assert_closed([response])
    assert_private_messages(context, received=1)


@pytest.mark.parametrize('stage', ['overview', 'detail'])
def test_real_no_tables_fallback_keeps_overview_error_and_fixed_detail_failure(
        monkeypatch, model, tmp_path, context, stage):
    module, _, waits = model
    html = document('<p>Нет таблиц</p>')
    responses = [reply(html)] if stage == 'overview' else [reply(DISCOVERY), reply(html)]
    urls = [URL] if stage == 'overview' else [URL, DETAIL]
    calls = source(monkeypatch, module, list(zip(urls, responses)))
    written = writers(monkeypatch, module, tmp_path)
    trace = real_parser_observer(monkeypatch, module, fallback='full')
    if stage == 'overview':
        with pytest.raises(ValueError):
            module.get_main(URL, logging_context=context)
        assert_private_messages(context, received=1, failed_count=1)
        failed(context)
    else:
        with pytest.raises(ValueError, match='DOHOD_DETAIL_PARSE_FAILED') as caught:
            module.get_page_info(URL, logging_context=context)
        assert caught.value.__context__ is None and caught.value.__cause__ is None
        assert_private_messages(context, received=2, failed_count=1)
        failed(context)
    assert trace['bodies'] == [html] and trace['table_counts'] == []
    assert len(trace['lxml']) == len(trace['bs4']) == 1
    assert trace['lxml'][0][0] is trace['bs4'][0][0]
    assert all(buffer.closed for buffer in trace['buffers'])
    assert_requests(calls)
    assert len(calls) == len(responses) and written == waits == []
    assert_closed(responses)
    assert_no_accumulation(module)


@pytest.mark.parametrize('stage', ['overview', 'detail'])
def test_real_fallback_completion_cannot_hide_expired_operation_budget(
        monkeypatch, model, tmp_path, context, stage):
    module, clock, waits = model
    html = overview_html() if stage == 'overview' else detail_html()
    responses = [reply(html)] if stage == 'overview' else [reply(DISCOVERY), reply(html)]
    urls = [URL] if stage == 'overview' else [URL, DETAIL]
    calls = source(monkeypatch, module, list(zip(urls, responses)))
    written = writers(monkeypatch, module, tmp_path)
    trace = real_parser_observer(monkeypatch, module, fallback='full',
        after_parse=lambda raw: clock.__setitem__(0, 35))
    entry = module.get_main if stage == 'overview' else module.get_page_info
    with pytest.raises(TimeoutError, match='DOHOD_BUDGET_EXHAUSTED'):
        entry(URL, logging_context=context)
    assert_parser_trace(trace, [html], [1 if stage == 'overview' else 3], fallback='full')
    assert_requests(calls)
    assert len(calls) == len(responses) and written == waits == []
    assert_closed(responses)
    values = assert_private_messages(context, received=len(responses), failed_count=1)
    assert values[-1]['error']['code'] == 'DOHOD_BUDGET_EXHAUSTED'
    failed(context)


@pytest.mark.parametrize('middle', ['failure', 'parser-failure'])
def test_same_import_a_then_network_or_parser_failure_then_b_has_no_accumulated_rows(
        monkeypatch, model, tmp_path, middle):
    module, clock, waits = model
    original_globals = tuple(getattr(module, name) for name in
        ('df_overview_full', 'df_years_full', 'df_each_full'))
    assert_no_accumulation(module, original_globals)
    trace = real_parser_observer(monkeypatch, module, fallback='prefix')
    saved_a = None
    all_responses = []
    for number, label in enumerate(('a', middle, 'b')):
        if number:
            clock[0] += 50
        amount = 10 if label == 'a' else 20
        if label == 'failure':
            error = requests.ConnectionError(ConnectionRefusedError(111, CANARY))
            discovery, detail_urls = two_page_discovery()
            responses = [reply(discovery), reply(detail_html(amount=20))]
            sequence = [(URL, responses[0]), (detail_urls[0], responses[1]),
                        (detail_urls[1], error)]
        else:
            html = (document('<p>Нет таблиц</p>') if label == 'parser-failure'
                    else detail_html(3 if label == 'a' else 2, amount=amount))
            responses = [reply(DISCOVERY), reply(html)]
            sequence = list(zip([URL, DETAIL], responses))
        all_responses.extend(responses)
        calls = source(monkeypatch, module, sequence)
        written = writers(monkeypatch, module, tmp_path)
        with fresh_context(tmp_path, label) as session:
            if label == 'failure':
                with monkeypatch.context() as observation:
                    assembled = partial_frames_observer(observation, module)
                    with pytest.raises(requests.ConnectionError) as caught:
                        module.get_page_info(URL, logging_context=session)
                assert caught.value is error
                expected_ticker = {URL + '/aaa': 'AAA', URL + '/bbb': 'BBB'}[detail_urls[0]]
                assert_detail(tuple(assembled), amount=20,
                    page_url=detail_urls[0], ticker=expected_ticker)
                assert [call[0] for call in calls] == [URL] + detail_urls
                assert_private_messages(session, received=2, failed_count=1)
                failed(session)
            elif label == 'parser-failure':
                with pytest.raises(ValueError, match='DOHOD_DETAIL_PARSE_FAILED') as caught:
                    module.get_page_info(URL, logging_context=session)
                assert caught.value.__context__ is None and caught.value.__cause__ is None
                assert_private_messages(session, received=2, failed_count=1)
                failed(session)
            else:
                frames = module.get_page_info(URL, logging_context=session)
                assert_detail(frames, 3 if label == 'a' else 2, amount=amount)
                if label == 'a':
                    saved_a = frames  # Retain returned originals to detect later mutation.
                assert_private_messages(session, received=2)
            assert_detail(saved_a, amount=10)
        assert_requests(calls)
        assert len(calls) == (3 if label == 'failure' else 2) and written == []
        assert_closed(responses)
        assert_no_accumulation(module, original_globals)
    assert_detail(saved_a, amount=10)
    assert waits == [] and clock[0] == 100
    assert_closed(all_responses)
    assert_no_accumulation(module, original_globals)
    assert all(buffer.closed for buffer in trace['buffers'])
    assert len({id(buffer) for buffer in trace['buffers']}) == len(trace['buffers'])


def test_same_import_get_main_a_real_failure_b_retains_returned_a_and_single_writer_group(
        monkeypatch, model, tmp_path):
    module, clock, waits = model
    original_globals = tuple(getattr(module, name) for name in
        ('df_overview_full', 'df_years_full', 'df_each_full'))
    assert_no_accumulation(module, original_globals)
    trace = real_parser_observer(monkeypatch, module, fallback='full')
    saved_a = None
    responses = []
    for number, label in enumerate(('a', 'no-tables', 'b')):
        if number:
            clock[0] += 50
        amount = 10 if label == 'a' else 20
        html = document('<p>Нет таблиц</p>') if label == 'no-tables' else overview_html(amount=amount)
        response = reply(html)
        responses.append(response)
        calls = source(monkeypatch, module, [(URL, response)])
        written = writers(monkeypatch, module, tmp_path)
        with fresh_context(tmp_path, label) as session:
            if label == 'no-tables':
                with pytest.raises(ValueError):
                    module.get_main(URL, logging_context=session)
                assert written == []
                assert_private_messages(session, received=1, failed_count=1)
                failed(session)
            else:
                frame = module.get_main(URL, logging_context=session)
                assert_overview(frame, amount)
                assert_writers(written, FILES[:1])
                assert_overview(written[0][3], amount)
                if label == 'a':
                    saved_a = frame
                else:
                    assert frame is not saved_a
                assert_private_messages(session, received=1)
            assert_overview(saved_a, 10)
        assert_requests(calls)
        assert len(calls) == 1 and waits == []
        assert_closed([response])
        assert_no_accumulation(module, original_globals)
    assert_overview(saved_a, 10)
    assert trace['table_counts'] == [1, 1]
    assert len(trace['bodies']) == len(trace['lxml']) == len(trace['bs4']) == 3
    assert all(buffer.closed for buffer in trace['buffers'])
    assert len({id(buffer) for buffer in trace['buffers']}) == 3
    assert_closed(responses)
    assert_no_accumulation(module, original_globals)
    assert clock[0] == 100 and waits == []


def assert_main_frames(written, amount):
    assert_writers(written)
    for record in written[:3]:
        assert_overview(record[3], amount)
    assert_detail(tuple(written[index][3] for index in (3, 5, 7)), amount=amount)
    assert_detail(tuple(written[index][3] for index in (4, 6, 8)), amount=amount)


def test_same_import_main_a_failure_b_success_preserves_admitted_groups_and_later_six(
        monkeypatch, model, tmp_path):
    module, clock, waits = model
    original_globals = tuple(getattr(module, name) for name in
        ('df_overview_full', 'df_years_full', 'df_each_full'))
    assert_no_accumulation(module, original_globals)
    targets = tmp_path / 'datasets' / 'dividends' / 'dohodru'
    targets.mkdir(parents=True)
    trace = real_parser_observer(monkeypatch, module, fallback='full')
    all_responses = []
    saved_a = None
    previous = None
    for number, label in enumerate(('a', 'failure-b', 'b')):
        if number:
            clock[0] += 100
        amount = 10 if label == 'a' else 20
        if label == 'failure-b':
            error = requests.ConnectionError(ConnectionRefusedError(111, CANARY))
            discovery, detail_urls = two_page_discovery()
            responses = [reply(overview_html(amount=amount)), reply(discovery),
                         reply(detail_html(amount=amount))]
            sequence = [(URL, responses[0]), (URL, responses[1]),
                        (detail_urls[0], responses[2]), (detail_urls[1], error)]
        else:
            responses = [reply(overview_html(amount=amount)), reply(DISCOVERY),
                         reply(detail_html(amount=amount))]
            sequence = list(zip([URL, URL, DETAIL], responses))
        all_responses.extend(responses)
        calls = source(monkeypatch, module, sequence)
        written = writers(monkeypatch, module, tmp_path,
            action=lambda number, method, name, label=label:
                (targets / name).write_bytes((label + ':' + name).encode('ascii')))
        with fresh_context(tmp_path, label) as session:
            if label == 'failure-b':
                with monkeypatch.context() as observation:
                    assembled = partial_frames_observer(observation, module)
                    with pytest.raises(requests.ConnectionError) as caught:
                        module.main(URL, logging_context=session)
                assert caught.value is error
                expected_ticker = {URL + '/aaa': 'AAA', URL + '/bbb': 'BBB'}[detail_urls[0]]
                assert_detail(tuple(assembled), amount=20,
                    page_url=detail_urls[0], ticker=expected_ticker)
                assert [call[0] for call in calls] == [URL, URL] + detail_urls
                assert_writers(written, FILES[:3])
                for record in written:
                    assert_overview(record[3], 20)
                for _, name in FILES[:3]:
                    assert (targets / name).read_bytes() == ('failure-b:' + name).encode('ascii')
                for _, name in FILES[3:]:
                    assert (targets / name).read_bytes() == previous[name][0]
                    assert hashlib.sha256((targets / name).read_bytes()).hexdigest() == previous[name][1]
                assert_private_messages(session, received=3, failed_count=1)
                failed(session)
            else:
                assert module.main(URL, logging_context=session) is None
                assert_main_frames(written, amount)
                for _, name in FILES:
                    assert (targets / name).read_bytes() == (label + ':' + name).encode('ascii')
                if label == 'a':
                    saved_a = [record[3] for record in written]
                    previous = {name: ((targets / name).read_bytes(),
                        hashlib.sha256((targets / name).read_bytes()).hexdigest()) for _, name in FILES}
                else:
                    for frame in saved_a[:3]:
                        assert_overview(frame, 10)
                    assert_detail(tuple(saved_a[index] for index in (3, 5, 7)), amount=10)
                assert_private_messages(session, received=3)
        assert_requests(calls)
        assert len(calls) == (4 if label == 'failure-b' else 3) and waits == []
        assert_closed(responses)
        assert_no_accumulation(module, original_globals)
    assert clock[0] == 200 and waits == []
    assert_closed(all_responses)
    assert_no_accumulation(module, original_globals)
    assert all(buffer.closed for buffer in trace['buffers'])
    assert len({id(buffer) for buffer in trace['buffers']}) == len(trace['buffers'])
