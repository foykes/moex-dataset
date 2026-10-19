"""Literal parser-output and writer timeline oracles, not real HTML quality."""

import hashlib
import ast
import datetime
from pathlib import Path
import sys
import time
import types

import pandas as pd
import pytest
import requests

from tests.shared.c2.test_dohod_network import (
    URL, DETAIL, DISCOVERY, model, context, reply, source, parser, writers, events, failed,
    assert_requests, ROOT, _html_bytes,
)


FILES = [
    ('to_excel', 'obzor_vsex_divs_tickerov.xlsx'),
    ('to_excel', 'main_page.xlsx'), ('to_csv', 'main_page.csv'),
    ('to_excel', 'rate_for_each_ticker.xlsx'), ('to_csv', 'rate_for_each_ticker.csv'),
    ('to_excel', 'sum_each_year_divs.xlsx'), ('to_csv', 'sum_each_year_divs.csv'),
    ('to_excel', 'all_payments.xlsx'), ('to_csv', 'all_payments.csv'),
]


def healthy(monkeypatch, model, tmp_path, *, detail_count=3, action=None, after_parse=None,
            overview_empty=False):
    module, _, _ = model
    replies = [reply(), reply(DISCOVERY), reply(b'detail')]
    calls = source(monkeypatch, module, list(zip([URL, URL, DETAIL], replies)))
    parsed = parser(monkeypatch, module, detail_count=detail_count,
                    after_parse=after_parse, overview_empty=overview_empty)
    written = writers(monkeypatch, module, tmp_path, action=action)
    return calls, parsed, written, replies


def assert_writers(written, expected=FILES):
    assert [(w[0], w[1]) for w in written] == expected
    assert all(w[2] == {} for w in written)  # Historical pandas default index=True.


def assert_frames(frames, *, detail_count=3):
    rate, years, payments = frames
    assert rate.columns.tolist() == ['текущая доходность', 'доля от прибыли', 'индекс DSI', 'page_url', 'ticker']
    assert rate.index.tolist() == [0, 2]
    assert rate.values.tolist() == [[0.1, 0.2, 0.3, DETAIL, 'AAA'], [0.4, 0.5, 0.6, DETAIL, 'AAA']]
    assert years.columns.tolist() == ['Год', 'Сумма', 'page_url', 'ticker']
    assert years.index.tolist() == [4, 4]
    assert years.values.tolist() == [[2024, 10, DETAIL, 'AAA'], [2024, 10, DETAIL, 'AAA']]
    if detail_count == 3:
        assert payments.columns.tolist() == ['Дата', 'Выплата', 'page_url', 'ticker']
        assert payments.index.tolist() == [9]
        assert payments.values.tolist() == [['2024-07-11', 10, DETAIL, 'AAA']]
    else:
        assert payments.empty and payments.columns.tolist() == []


def test_healthy_main_nine_historical_writers(monkeypatch, model, tmp_path, context):
    module, _, waits = model
    calls, parsed, written, replies = healthy(monkeypatch, model, tmp_path)
    assert module.main(URL, logging_context=context) is None
    assert [c[0] for c in calls] == [URL, URL, DETAIL] and waits == []
    assert parsed == [b'overview', b'detail']
    assert_requests(calls)
    assert_writers(written)
    for w in written[:3]:
        assert w[3].columns.tolist() == ['Тикер', 'Дивиденд']
        assert w[3].index.tolist() == [0] and w[3].values.tolist() == [['AAA', 10]]
    assert_frames([written[i][3] for i in (3, 5, 7)])
    assert_frames([written[i][3] for i in (4, 6, 8)])
    for left, right in ((1, 2), (3, 4), (5, 6), (7, 8)):
        pd.testing.assert_frame_equal(written[left][3], written[right][3])
    assert all(r.close_calls == 1 and r.raw.closes == 0 and r.raw.releases == 1 for r in replies)
    assert [e['event'] for e in events(context)] == ['dohod_request_received'] * 3 + [
        'dohod_page_coverage', 'dohod_coverage_summary']
    assert all(getattr(module, name).empty for name in ('df_overview_full', 'df_years_full', 'df_each_full'))


@pytest.mark.parametrize('detail_count', [2, 3])
def test_direct_get_page_info_tuple_no_writers(monkeypatch, model, tmp_path, detail_count):
    module, _, _ = model
    calls = source(monkeypatch, module, [(URL, reply(DISCOVERY)), (DETAIL, reply(b'detail'))])
    parsed = parser(monkeypatch, module, detail_count=detail_count)
    written = writers(monkeypatch, module, tmp_path)
    frames = module.get_page_info(URL)
    assert type(frames) is tuple and len(frames) == 3
    assert_frames(frames, detail_count=detail_count)
    assert len(calls) == 2 and parsed == [b'detail'] and written == []
    assert_requests(calls)


def test_empty_overview_is_unverified_before_all_nine_writers(monkeypatch, model, tmp_path):
    module, _, _ = model
    _, _, written, _ = healthy(monkeypatch, model, tmp_path, overview_empty=True)
    with pytest.raises(ValueError, match='DOHOD_EMPTY_ROLE_NOT_VERIFIED'):
        module.main(URL)
    assert written == []


def test_url_filter_preserves_first_dom_order_and_rejects_foreign(monkeypatch, model, tmp_path):
    module, _, _ = model
    links = ['/ik/analytics/dividend/bbb', '/ik/analytics/dividend/aaa',
        '/ik/analytics/dividend/aaa', '/ik/analytics/dividend/report.pdf',
        '/other', 'https://foreign.invalid/ik/analytics/dividend/weird']
    expected = [URL + '/bbb', URL + '/aaa']
    html = ''.join('<a href="' + value + '">fixture</a>' for value in links).encode()
    calls = source(monkeypatch, module, [(URL, reply(html))] + [(u, reply(b'detail')) for u in expected])
    parser(monkeypatch, module)
    writers(monkeypatch, module, tmp_path)
    result = module.get_page_info(URL)
    assert [c[0] for c in calls] == [URL] + expected
    assert result[0]['page_url'].tolist() == [u for u in expected for _ in range(2)]
    assert result[0]['ticker'].tolist() == [u.split('/')[-1].upper() for u in expected for _ in range(2)]
    assert result[0].index.tolist() == [0, 2, 0, 2]


@pytest.mark.parametrize('cross_call', [4, 6, 8])
def test_final_group_admitted_once_xlsx_crosses_source_deadline(monkeypatch, model, tmp_path, context, cross_call):
    module, clock, waits = model
    def cross(number, method, name):
        if number == cross_call:
            assert method == 'to_excel' and clock[0] < 600
            clock[0] = 601
    calls, _, written, _ = healthy(monkeypatch, model, tmp_path, action=cross)
    assert module.main(URL, logging_context=context) is None
    assert_writers(written)
    assert len(calls) == 3 and waits == [] and clock[0] == 601
    assert not any(e['event'] == 'dohod_acquisition_failed' for e in events(context))


def test_direct_overview_writer_crosses_deadline_returns_frame(monkeypatch, model, tmp_path):
    module, clock, _ = model
    source(monkeypatch, module, [(URL, reply())])
    parser(monkeypatch, module)
    written = writers(monkeypatch, module, tmp_path, action=lambda *args: clock.__setitem__(0, 601))
    frame = module.get_main(URL)
    assert isinstance(frame, pd.DataFrame) and frame.values.tolist() == [['AAA', 10]]
    assert_writers(written, FILES[:1])


@pytest.mark.parametrize('duration', [36, 601])
def test_early_group_finishes_then_original_source_deadline(monkeypatch, model, tmp_path, context, duration):
    module, clock, _ = model
    def cross(number, method, name):
        if number == 1:
            clock[0] = duration
    if duration == 36:
        calls, _, written, _ = healthy(monkeypatch, model, tmp_path, action=cross)
        assert module.main(URL, logging_context=context) is None
        assert_writers(written)
        assert len(calls) == 3
    else:
        calls = source(monkeypatch, module, [(URL, reply())])
        parser(monkeypatch, module)
        written = writers(monkeypatch, module, tmp_path, action=cross)
        with pytest.raises(TimeoutError, match='DOHOD_BUDGET_EXHAUSTED'):
            module.main(URL, logging_context=context)
        assert_writers(written, FILES[:3])
        assert len(calls) == 1
        assert events(context)[-1]['error']['code'] == 'DOHOD_BUDGET_EXHAUSTED'
        failed(context)


@pytest.mark.parametrize('writer_number', [1, 2, 4, 5, 6, 8, 9])
@pytest.mark.parametrize('error_type', [OSError, TimeoutError])
def test_writer_exception_identity_no_transport_retry_or_budget_reclassification(
        monkeypatch, model, tmp_path, context, writer_number, error_type):
    module, clock, waits = model
    error = error_type('fixture writer failure')
    def fail(number, method, name):
        if number == writer_number:
            clock[0] = 601
            raise error
    calls, _, written, _ = healthy(monkeypatch, model, tmp_path, action=fail)
    with pytest.raises(error_type) as caught:
        module.main(URL, logging_context=context)
    assert caught.value is error and caught.value.args == ('fixture writer failure',)
    assert_writers(written, FILES[:writer_number])
    assert len(calls) == (1 if writer_number < 4 else 3) and waits == []
    assert not any(e['error'] and e['error']['code'] == 'DOHOD_BUDGET_EXHAUSTED' for e in events(context))
    failed(context)


def test_late_detail_failure_three_early_six_previous_untouched(monkeypatch, model, tmp_path, context):
    module, _, waits = model
    targets = tmp_path / 'datasets/dividends/dohodru'
    targets.mkdir(parents=True)
    old = {name: ('previous:' + name).encode() for _, name in FILES}
    for name, payload in old.items():
        (targets / name).write_bytes(payload)
    previous_hashes = {name: hashlib.sha256(payload).hexdigest() for name, payload in old.items()}
    error = requests.ConnectionError(ConnectionRefusedError(111, 'fixture'))
    links = ['/ik/analytics/dividend/aaa', '/ik/analytics/dividend/bbb']
    detail_urls = [URL + '/aaa', URL + '/bbb']
    html = ''.join('<a href="' + u + '">fixture</a>' for u in links).encode()
    calls = source(monkeypatch, module, [(URL, reply()), (URL, reply(html)),
        (detail_urls[0], reply(b'detail')), (detail_urls[1], error)])
    parser(monkeypatch, module)
    written = writers(monkeypatch, module, tmp_path,
        action=lambda number, method, name: (targets / name).write_bytes(('updated:' + name).encode()))
    with pytest.raises(requests.ConnectionError) as caught:
        module.main(URL, logging_context=context)
    assert caught.value is error and len(calls) == 4 and waits == []
    assert [c[0] for c in calls[2:]] == detail_urls
    assert_writers(written, FILES[:3])
    for _, name in FILES[:3]:
        assert (targets / name).read_bytes() == ('updated:' + name).encode()
    for _, name in FILES[3:]:
        assert (targets / name).read_bytes() == old[name]
        assert hashlib.sha256((targets / name).read_bytes()).hexdigest() == previous_hashes[name]
    assert len([e for e in events(context) if e['event'] == 'dohod_acquisition_failed']) == 1
    failed(context)


def test_expired_last_detail_parser_no_later_six(monkeypatch, model, tmp_path, context):
    module, clock, _ = model
    def expire(raw):
        if raw == b'detail':
            clock[0] = 600
    _, _, written, _ = healthy(monkeypatch, model, tmp_path, after_parse=expire)
    with pytest.raises(TimeoutError, match='DOHOD_BUDGET_EXHAUSTED'):
        module.main(URL, logging_context=context)
    assert_writers(written, FILES[:3])
    assert 'request_phase=DECODE' not in events(context)[-1]['message']
    failed(context)


def test_detail_parser_failure_cannot_hide_expired_budget(monkeypatch, model, tmp_path):
    module, clock, _ = model
    source(monkeypatch, module, [(URL, reply(DISCOVERY)), (DETAIL, reply(b'bad'))])
    written = writers(monkeypatch, module, tmp_path)
    def read(reader, *, encoding=None):
        assert _html_bytes(reader, encoding=encoding) == b'bad'
        clock[0] = 35
        raise ValueError('fixture no tables')
    monkeypatch.setattr(module.pd, 'read_html', read)
    with pytest.raises(TimeoutError, match='DOHOD_BUDGET_EXHAUSTED'):
        module.get_page_info(URL)
    assert written == []


def test_200_overview_parser_error_zero_writers_no_transport_retry(monkeypatch, model, tmp_path, context):
    module, _, waits = model
    calls = source(monkeypatch, module, [(URL, reply(b'malformed'))])
    written = writers(monkeypatch, module, tmp_path)
    error = ValueError('fixture missing tables')
    def read(reader, *, encoding=None):
        assert _html_bytes(reader, encoding=encoding) == b'malformed'
        raise error
    monkeypatch.setattr(module.pd, 'read_html', read)
    with pytest.raises(ValueError) as caught:
        module.main(URL, logging_context=context)
    assert caught.value is error and len(calls) == 1 and waits == written == []
    assert 'request_phase=DECODE' not in events(context)[-1]['message']
    failed(context)


def test_one_source_deadline_across_multiple_fresh_operations(monkeypatch, model, tmp_path, context):
    module, clock, _ = model
    links = ['/ik/analytics/dividend/aaa', '/ik/analytics/dividend/bbb']
    detail_urls = [URL + '/aaa', URL + '/bbb']
    html = ''.join('<a href="' + u + '">fixture</a>' for u in links).encode()
    def advance():
        clock[0] += 20
    calls = source(monkeypatch, module, [(URL, reply()),
        (URL, reply(html, after_read=advance)),
        (detail_urls[0], reply(b'detail', after_read=advance)),
        (detail_urls[1], reply(b'detail', after_read=advance))])
    parser(monkeypatch, module)
    written = writers(monkeypatch, module, tmp_path,
        action=lambda number, method, name: clock.__setitem__(0, 550) if number == 1 else None)
    with pytest.raises(TimeoutError, match='DOHOD_BUDGET_EXHAUSTED'):
        module.main(URL, logging_context=context)
    assert clock[0] == 610 and len(calls) == 4
    assert [c[1] for c in calls] == [(10, 10)] * 4
    assert_writers(written, FILES[:3])
    assert events(context)[-1]['error']['code'] == 'DOHOD_BUDGET_EXHAUSTED'
    failed(context)


@pytest.mark.parametrize('table_count', [0, 1, 4])
def test_healthy_budget_unrecognized_detail_is_terminal_without_partial_return(
        monkeypatch, model, tmp_path, context, table_count):
    module, _, waits = model
    calls = source(monkeypatch, module, [(URL, reply(DISCOVERY)), (DETAIL, reply(b'unknown'))])
    def read(reader, *, encoding=None):
        assert _html_bytes(reader, encoding=encoding) == b'unknown'
        if table_count == 0:
            raise ValueError('fixture no tables')
        return [pd.DataFrame([['irrelevant']])] * table_count
    monkeypatch.setattr(module.pd, 'read_html', read)
    written = writers(monkeypatch, module, tmp_path)
    reason = 'DOHOD_DETAIL_PARSE_FAILED' if table_count == 0 else 'DOHOD_UNKNOWN_NONEMPTY_TABLE'
    with pytest.raises(ValueError, match=reason) as caught:
        module.get_page_info(URL, logging_context=context)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    assert len(calls) == 2 and written == waits == []
    assert [e['event'] for e in events(context)] == ['dohod_request_received'] * 2 + [
        'dohod_page_coverage', 'dohod_coverage_summary', 'dohod_acquisition_failed']
    assert events(context)[-1]['error']['code'] == reason
    failed(context)


def test_parent_stage_propagates_dohod_failure_upload_and_success_unreachable(
        monkeypatch, model, tmp_path, context, capsys):
    # Compile only the inspected function, never import or execute main.py CLI.
    from contextlib import contextmanager
    import run_logging
    source_text = (ROOT / 'main.py').read_bytes().replace(b'\r\n', b'\n')
    tree = ast.parse(source_text.decode('utf-8'))
    functions = {name: [node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name]
        for name in ('main', '_run_cli')}
    markers = [node for node in tree.body if (
        isinstance(node, ast.Assign)
        and any(isinstance(part, ast.Name) and part.id == 'D2_SOURCE_PROFILE'
                for target in node.targets for part in ast.walk(target))) or (
        isinstance(node, (ast.AnnAssign, ast.AugAssign))
        and any(isinstance(part, ast.Name) and part.id == 'D2_SOURCE_PROFILE'
                for part in ast.walk(node.target)))]
    modern = (len(markers) == 1 and isinstance(markers[0], ast.Assign)
        and len(markers[0].targets) == 1
        and isinstance(markers[0].targets[0], ast.Name)
        and isinstance(markers[0].value, ast.Constant)
        and markers[0].value.value == 'linux-stage-b-v1'
        and all(len(value) == 1 and isinstance(value[0], ast.FunctionDef)
                for value in functions.values()))
    if modern:
        main_args = functions['main'][0].args
        cli_args = functions['_run_cli'][0].args
        modern = (not functions['main'][0].decorator_list
            and not functions['_run_cli'][0].decorator_list
            and not main_args.posonlyargs and not main_args.args
            and main_args.vararg is None and main_args.kwarg is None
            and [arg.arg for arg in main_args.kwonlyargs] == ['logging_context', 'result_sink']
            and all(isinstance(value, ast.Constant) and value.value is None
                    for value in main_args.kw_defaults)
            and not cli_args.posonlyargs and not cli_args.args
            and cli_args.vararg is None and cli_args.kwarg is None
            and not cli_args.kwonlyargs)
    legacy = hashlib.sha256(source_text).hexdigest() == (
        'c035f4439c060391e38c270f6f7adf257b356e6aae2104b135cf53e770509c3a')
    assert legacy or modern, 'D2_FIXTURE_SOURCE_PROFILE_UNSUPPORTED'
    runtime_events = []
    class Runtime:
        def bind_context(self, value):
            assert runtime_events in (['entered'], ['entered', 'bound']) and value is context
            if runtime_events == ['entered']:
                runtime_events.append('bound')
    @contextmanager
    def runtime_scope(*, logging_context=None):
        assert logging_context is None or logging_context is context
        assert runtime_events == []
        runtime_events.append('entered')
        try:
            yield Runtime()
        finally:
            assert runtime_events == ['entered', 'bound']
            runtime_events.append('closed')
    def forbidden_upload(*args, **kwargs):
        pytest.fail('Upload was reached after a required dohod failure')
    module, _, _ = model
    error = requests.ConnectTimeout('fixture final network failure')
    source(monkeypatch, module, [(URL, error)] * 3)
    writers(monkeypatch, module, tmp_path)
    called = []
    for name in ('data_gathering', 'dividends', 'tech', 'upload'):
        stub = types.ModuleType(name)
        stub.main = lambda *a, stage=name, **kw: called.append(stage)
        if name == 'upload':
            stub.main = forbidden_upload
            stub.runtime_scope = runtime_scope
            stub._d2_completion_allowed = lambda result_sink: False
        monkeypatch.setitem(sys.modules, name, stub)
    monkeypatch.setitem(sys.modules, 'dohodru_data', module)
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
    namespace = {'time': time, 'datetime': datetime, 'Path': Path, '__file__': str(ROOT / 'main.py')}
    if modern:
        namespace['D2_SOURCE_PROFILE'] = 'linux-stage-b-v1'
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(ROOT / 'main.py'), 'exec'), namespace)
    with pytest.raises(requests.ConnectTimeout) as caught:
        namespace['main'](logging_context=context)
    assert caught.value is error and called == ['data_gathering', 'dividends', 'tech']
    values = events(context)
    stage_failed = [e for e in values if e['event'] == 'stage_failed']
    assert len(stage_failed) == 1 and stage_failed[0]['stage'] == 'dohodru_data'
    assert stage_failed[0]['outcome'] == 'FAILED'
    assert stage_failed[0]['error']['category'] == 'UNKNOWN'  # Parent hookup remains D/F.
    assert 'Скрипт полностью закончил работу' not in capsys.readouterr().out
    assert runtime_events == (['entered', 'bound', 'closed'] if modern else [])
    failed(context)
