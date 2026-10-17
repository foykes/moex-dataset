"""Offline consumer-profile/coverage oracles, never a live HTML contract."""

import hashlib
import json
import traceback

import pandas as pd
import pytest
import requests

import run_logging
from tests.shared.c2.test_dohod_compatibility import FILES, assert_writers
from tests.shared.c2.test_dohod_network import (
    CANARY, DETAIL, DISCOVERY, URL, _html_bytes, assert_requests,
    context, events, failed, model, reply, source, writers,
)
from tests.shared.c3.test_dohod_html import (
    RATE_COLUMNS, assert_detail, assert_overview, overview_html,
)


PRIVATE_MARKER = 'C3_SEMANTIC_PRIVATE_BODY_804'
PATH_MARKER = 'C:/private-c3-marker/source.html'
UNKNOWN = pd.DataFrame([['unexpected source row']], columns=['Changed without anchors'])


def overview_frame():
    return pd.DataFrame([['AAA', 'Газпром', 10], ['AAA', 'Газпром', 10]],
        columns=['Тикер', 'Компания', 'Дивиденд'])


def detail_frames(*, amount=10, payments=True):
    # Literal source frames, including the historical header-as-data row.
    result = [
        pd.DataFrame([['10%', '20%', '0.3'],
            ['текущая доходность', 'доля от прибыли', 'индекс DSI'],
            ['10%', '20%', '0.3']]),
        pd.DataFrame([[2024, amount], [2024, amount]], columns=['Год', 'Сумма']),
    ]
    if payments:
        result.append(pd.DataFrame([['2024-07-11', amount]], columns=['Дата', 'Выплата']))
    return result


def parse_map(monkeypatch, module, values):
    calls = []
    def read(reader, *, encoding=None):
        raw = _html_bytes(reader, encoding=encoding)
        calls.append(raw)
        answer = values[raw]
        if isinstance(answer, BaseException):
            raise answer
        return [frame.copy(deep=True) for frame in answer]
    monkeypatch.setattr(module.pd, 'read_html', read)
    return calls


def clean_overview_html():
    # C3 adapter suite отдельно сохраняет старое тело с непроверенной noise table.
    return overview_html()


def links(urls):
    return ''.join('<a href="' + target + '">fixture</a>' for target in urls).encode('ascii')


def public_detail(monkeypatch, model, tmp_path, tables, *, logging_context=None):
    module, _, waits = model
    calls = source(monkeypatch, module, [(URL, reply(DISCOVERY)), (DETAIL, reply(b'detail'))])
    parsed = parse_map(monkeypatch, module, {b'detail': tables})
    written = writers(monkeypatch, module, tmp_path)
    return module, calls, parsed, written, waits


def test_public_overview_selects_only_semantic_table(monkeypatch, model, tmp_path):
    module, _, waits = model
    source(monkeypatch, module, [(URL, reply(clean_overview_html()))])
    written = writers(monkeypatch, module, tmp_path)
    assert_overview(module.get_main(URL))
    assert_writers(written, FILES[:1])
    assert waits == []


@pytest.mark.parametrize('case', [
    'unknown-before', 'unknown-after', 'duplicate', 'renamed', 'reordered',
    'empty', 'all-null', 'zero-tables',
])
def test_public_overview_rejects_unverified_tables_before_any_writer(
        monkeypatch, model, tmp_path, case):
    module, _, waits = model
    good = overview_frame()
    values = {
        'unknown-before': [UNKNOWN, good], 'unknown-after': [good, UNKNOWN],
        'duplicate': [good, good],
        'renamed': [good.rename(columns={'Дивиденд': 'Сменившееся поле'})],
        'reordered': [good[['Компания', 'Тикер', 'Дивиденд']]],
        'empty': [good.iloc[:0]],
        'all-null': [pd.DataFrame([[None, None, None]], columns=good.columns)],
        'zero-tables': [],
    }
    calls = source(monkeypatch, module, [(URL, reply(b'overview'))])
    parse_map(monkeypatch, module, {b'overview': values[case]})
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError):
        module.get_main(URL)
    assert written == waits == [] and len(calls) == 1


@pytest.mark.parametrize('order', [(2, 0, 1), (1, 2, 0), (1, 0, 2)])
def test_public_detail_roles_follow_signatures_not_table_positions(
        monkeypatch, model, tmp_path, order):
    tables = detail_frames()
    module, calls, parsed, written, waits = public_detail(
        monkeypatch, model, tmp_path, [tables[index] for index in order])
    assert_detail(module.get_page_info(URL))
    assert [call[0] for call in calls] == [URL, DETAIL]
    assert parsed == [b'detail'] and written == waits == []


@pytest.mark.parametrize('case', [
    'unknown-fourth', 'unknown-replaces-payments', 'duplicate-metrics',
    'missing-history', 'missing-metrics', 'zero-tables', 'empty-history',
    'empty-payments', 'header-only-metrics',
])
def test_public_detail_unknown_or_incomplete_never_returns_partial_tuple(
        monkeypatch, model, tmp_path, case):
    rate, history, payment = detail_frames()
    values = {
        'unknown-fourth': [rate, history, payment, UNKNOWN],
        'unknown-replaces-payments': [rate, history, UNKNOWN],
        'duplicate-metrics': [rate, rate, history, payment],
        'missing-history': [rate, payment], 'missing-metrics': [history, payment],
        'zero-tables': [], 'empty-history': [rate, history.iloc[:0], payment],
        'empty-payments': [rate, history, payment.iloc[:0]],
        'header-only-metrics': [pd.DataFrame([[None] * 3, list(RATE_COLUMNS[:3]),
                                             [None] * 3]), history, payment],
    }
    module, calls, _, written, waits = public_detail(
        monkeypatch, model, tmp_path, values[case])
    with pytest.raises(ValueError):
        module.get_page_info(URL)
    assert len(calls) == 2 and written == waits == []


def test_public_zero_discovery_blocks_later_six(monkeypatch, model, tmp_path):
    module, _, waits = model
    source(monkeypatch, module, [(URL, reply(b'overview')), (URL, reply(b'<html></html>'))])
    parse_map(monkeypatch, module, {b'overview': [overview_frame()]})
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError, match='DOHOD_DISCOVERY_EMPTY_NOT_VERIFIED'):
        module.main(URL)
    assert_writers(written, FILES[:3])
    assert waits == []


@pytest.mark.parametrize('missing_page', ['aaa', 'bbb', 'both'])
def test_public_missing_payments_collects_but_blocks_all_later_six(
        monkeypatch, model, tmp_path, missing_page):
    module, _, waits = model
    urls = [DETAIL, URL + '/bbb']
    sequence = [(URL, reply(b'overview')), (URL, reply(links(urls)))]
    sequence += [(target, reply(target.rsplit('/', 1)[1].encode())) for target in urls]
    calls = source(monkeypatch, module, sequence)
    parse_map(monkeypatch, module, {b'overview': [overview_frame()],
        b'aaa': detail_frames(payments=missing_page not in ('aaa', 'both')),
        b'bbb': detail_frames(payments=missing_page not in ('bbb', 'both'))})
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError, match='DOHOD_PAYMENTS_COVERAGE_NOT_VERIFIED'):
        module.main(URL)
    assert_writers(written, FILES[:3])
    assert [call[0] for call in calls] == [URL, URL] + urls and waits == []


def test_public_middle_parser_failure_stops_before_next_page_and_later_six(
        monkeypatch, model, tmp_path):
    module, _, waits = model
    urls = [URL + '/' + name for name in ('aaa', 'bbb', 'ccc', 'ddd')]
    calls = source(monkeypatch, module, [(URL, reply(b'overview')), (URL, reply(links(urls)))]
        + [(target, reply(name.encode())) for target, name in zip(urls[:3], ('aaa', 'bbb', 'ccc'))])
    parse_map(monkeypatch, module, {b'overview': [overview_frame()],
        b'aaa': detail_frames(), b'bbb': detail_frames(),
        b'ccc': ValueError(PRIVATE_MARKER + ' ' + PATH_MARKER)})
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError, match='DOHOD_DETAIL_PARSE_FAILED') as caught:
        module.main(URL)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    assert_writers(written, FILES[:3])
    assert [call[0] for call in calls] == [URL, URL] + urls[:3] and waits == []


def install_pages(monkeypatch, module, answers, *, urls=None):
    urls = list(answers) if urls is None else urls
    sequence = [(URL, reply(links(urls)))]
    parser_answers = {}
    for index, (target, answer) in enumerate(answers.items()):
        if isinstance(answer, requests.RequestException):
            sequence.append((target, answer))
        else:
            marker = ('page-' + str(index)).encode('ascii')
            sequence.append((target, reply(marker)))
            parser_answers[marker] = answer
    calls = source(monkeypatch, module, sequence)
    parsed = parse_map(monkeypatch, module, parser_answers)
    return calls, parsed


def private_result(module, sink, logging_context=None):
    return module._prepare_pages_result(URL, 600, logging_context, coverage_sink=sink)


def assert_finished_report(report):
    assert report['version'] == 1 and report['scope'] == 'ADMITTED_DISCOVERY'
    assert report['source_coverage'] == 'NOT_VERIFIED'
    assert report['finalized'] is True
    assert report['execution_outcome'] in ('RETURNED', 'FAILED', 'INTERRUPTED')


def test_private_sink_success_same_object_and_no_logger(monkeypatch, model, tmp_path):
    module, _, waits = model
    calls, parsed = install_pages(monkeypatch, module, {DETAIL: detail_frames()})
    written = writers(monkeypatch, module, tmp_path)
    sink = {}
    frames, report = private_result(module, sink)
    assert sink['report'] is report
    assert_detail(frames)
    assert_finished_report(report)
    assert report['expected'] == [DETAIL] and report['missing'] == []
    assert report['counts'] == dict(expected=1, requested=1, received=1, parsed=1,
        validated=1, accepted=1, failed=0, not_run=0, missing=0)
    assert report['observed_coverage'] == 'COMPLETE_OBSERVED'
    assert report['payments_coverage'] == 'PRESENT_FOR_ALL'
    assert report['late_group_input_status'] == 'PASS'
    assert report['diagnostics_status'] == 'NOT_REQUESTED'
    assert [page['url'] for page in report['pages']] == [DETAIL]
    page = report['pages'][0]
    assert page['stage'] == page['outcome'] == 'ACCEPTED'
    assert page['ref'] == 'dohod-page-' + hashlib.sha256(DETAIL.encode('ascii')).hexdigest()
    assert page['sends'] == page['attempts'] == 1
    assert len(calls) == 2 and parsed == [b'page-0'] and written == waits == []


def test_private_sink_middle_semantic_failure_literal_coverage_without_logger(
        monkeypatch, model, tmp_path):
    module, _, waits = model
    a, b, c, d = [URL + '/' + slug for slug in ('aaa', 'bbb', 'ccc', 'ddd')]
    calls, parsed = install_pages(monkeypatch, module,
        {a: detail_frames(), b: detail_frames(), c: detail_frames() + [UNKNOWN]},
        urls=[a, b, c, d])
    written = writers(monkeypatch, module, tmp_path)
    sink = {}
    with pytest.raises(ValueError, match='DOHOD_UNKNOWN_NONEMPTY_TABLE') as caught:
        private_result(module, sink)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    report = sink['report']
    assert_finished_report(report)
    assert report['expected'] == [a, b, c, d] and report['missing'] == [c, d]
    assert report['counts'] == dict(expected=4, requested=3, received=3, parsed=3,
        validated=2, accepted=2, failed=1, not_run=1, missing=2)
    assert [(page['stage'], page['outcome'], page['reason']) for page in report['pages']] == [
        ('ACCEPTED', 'ACCEPTED', None), ('ACCEPTED', 'ACCEPTED', None),
        ('PARSED', 'NOT_VERIFIED', 'DOHOD_UNKNOWN_NONEMPTY_TABLE'),
        ('DISCOVERED', 'NOT_RUN', 'PRIOR_PAGE_FAILED')]
    assert report['observed_coverage'] == 'INCOMPLETE'
    assert report['late_group_input_status'] == 'BLOCKED_FAILED'
    assert report['execution_outcome'] == 'FAILED'
    assert report['diagnostics_status'] == 'NOT_REQUESTED'
    assert [call[0] for call in calls] == [URL, a, b, c]
    assert parsed == [b'page-0', b'page-1', b'page-2'] and written == waits == []


def test_private_sink_network_identity_and_stages_survive_exception(
        monkeypatch, model, tmp_path):
    module, _, waits = model
    a, b, c, d = [URL + '/' + slug for slug in ('aaa', 'bbb', 'ccc', 'ddd')]
    error = requests.exceptions.SSLError('fixture permanent TLS error')
    calls, parsed = install_pages(monkeypatch, module,
        {a: detail_frames(), b: detail_frames(), c: error}, urls=[a, b, c, d])
    written = writers(monkeypatch, module, tmp_path)
    sink = {}
    with pytest.raises(requests.exceptions.SSLError) as caught:
        private_result(module, sink)
    assert caught.value is error and caught.value.args == ('fixture permanent TLS error',)
    report = sink['report']
    assert_finished_report(report)
    assert report['counts'] == dict(expected=4, requested=3, received=2, parsed=2,
        validated=2, accepted=2, failed=1, not_run=1, missing=2)
    assert report['pages'][2]['reason'] == 'TLS_FAILURE'
    assert report['pages'][2]['stage'] == 'REQUESTED'
    assert report['pages'][3]['outcome'] == 'NOT_RUN'
    assert report['missing'] == [c, d]
    assert [call[0] for call in calls] == [URL, a, b, c]
    assert parsed == [b'page-0', b'page-1'] and written == waits == []


@pytest.mark.parametrize('error_type', [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize('with_context', [False, True])
def test_private_sink_interruption_is_finalized_without_error_replacement(
        monkeypatch, model, tmp_path, context, error_type, with_context):
    module, _, waits = model
    error = error_type('fixture interruption')
    calls, _ = install_pages(monkeypatch, module, {DETAIL: error},
        urls=[DETAIL, URL + '/bbb'])
    written = writers(monkeypatch, module, tmp_path)
    sink = {}
    with pytest.raises(error_type) as caught:
        private_result(module, sink, context if with_context else None)
    assert caught.value is error and caught.value.args == ('fixture interruption',)
    report = sink['report']
    assert_finished_report(report)
    assert report['execution_outcome'] == 'INTERRUPTED'
    assert report['pages'][0]['outcome'] == 'INTERRUPTED'
    assert report['pages'][1]['outcome'] == 'NOT_RUN'
    assert report['diagnostics_status'] == ('CONFIRMED' if with_context else 'NOT_REQUESTED')
    assert not any(event['event'] == 'dohod_acquisition_failed' for event in events(context))
    assert len(calls) == 2 and written == waits == []


@pytest.mark.parametrize('kind', ['invalid-type', 'nonempty'])
def test_private_sink_requires_fresh_builtin_dict_before_http(monkeypatch, model, kind):
    module, _, waits = model
    calls = source(monkeypatch, module, [])
    sink = [] if kind == 'invalid-type' else {'prior': 'report'}
    with pytest.raises((TypeError, ValueError)):
        private_result(module, sink)
    assert calls == waits == []


@pytest.mark.parametrize('source_value', [0, False])
def test_metrics_zero_or_false_is_source_payload_and_keeps_null_rows(
        monkeypatch, model, tmp_path, source_value):
    rate, history, payment = detail_frames()
    rate = pd.DataFrame([[source_value, None, None], list(RATE_COLUMNS[:3]), [None] * 3])
    module, _, _, written, _ = public_detail(monkeypatch, model, tmp_path, [rate, history, payment])
    frames = module.get_page_info(URL)
    expected = pd.DataFrame([[source_value, None, None, DETAIL, 'AAA'],
                             [None, None, None, DETAIL, 'AAA']],
        columns=RATE_COLUMNS, index=[0, 2]).astype('object')
    expected.columns.name = 1
    pd.testing.assert_frame_equal(frames[0], expected, check_exact=True)
    assert written == []


@pytest.mark.parametrize('role', ['metrics', 'history', 'payments'])
def test_derived_provenance_and_metrics_header_do_not_make_payload_nonempty(
        monkeypatch, model, tmp_path, role):
    rate, history, payment = detail_frames()
    if role == 'metrics':
        rate = pd.DataFrame([[None] * 3, list(RATE_COLUMNS[:3]), [None] * 3])
    elif role == 'history':
        history = pd.DataFrame([[None, None]], columns=['Год', 'Сумма'])
    else:
        payment = pd.DataFrame([[None, None]], columns=['Дата', 'Выплата'])
    module, calls, _, written, waits = public_detail(
        monkeypatch, model, tmp_path, [rate, history, payment])
    with pytest.raises(ValueError, match='DOHOD_EMPTY_ROLE_NOT_VERIFIED'):
        module.get_page_info(URL)
    assert len(calls) == 2 and written == waits == []


def test_empty_unknown_table_is_not_approved_noise(monkeypatch, model, tmp_path):
    module, _, _, written, _ = public_detail(monkeypatch, model, tmp_path,
        detail_frames() + [UNKNOWN.iloc[:0]])
    with pytest.raises(ValueError, match='DOHOD_UNKNOWN_EMPTY_TABLE'):
        module.get_page_info(URL)
    assert written == []


@pytest.mark.parametrize('role', ['history', 'payments'])
def test_cross_page_profile_change_refuses_schema_union(monkeypatch, model, role):
    module, _, waits = model
    first = detail_frames()
    second = detail_frames()
    if role == 'history':
        second[1] = pd.DataFrame([[2024, 10, 0]],
            columns=['Год', 'Дивиденд (руб.)', 'Изм. к пред. году'])
    else:
        second[2] = pd.DataFrame([['2024-01-01', '2024-07-11', 2024, 10]], columns=[
            'Дата объявления дивиденда', 'Дата закрытия реестра',
            'Год для учета дивиденда', 'Дивиденд'])
    install_pages(monkeypatch, module, {DETAIL: first, URL + '/bbb': second})
    sink = {}
    with pytest.raises(ValueError, match='DOHOD_TABLE_PROFILE_CHANGED'):
        private_result(module, sink)
    assert sink['report']['counts']['accepted'] == 1
    assert sink['report']['pages'][1]['outcome'] == 'NOT_VERIFIED'
    assert waits == []


def test_private_optional_payments_reports_collection_separately_from_writer_gate(
        monkeypatch, model, tmp_path):
    module, _, waits = model
    install_pages(monkeypatch, module, {DETAIL: detail_frames(payments=False)})
    written = writers(monkeypatch, module, tmp_path)
    sink = {}
    frames, report = private_result(module, sink)
    assert_detail(frames, 2)
    assert report is sink['report']
    assert report['observed_coverage'] == 'COMPLETE_OBSERVED'
    assert report['payments_coverage'] == 'NOT_VERIFIED'
    assert report['late_group_input_status'] == 'BLOCKED_NOT_VERIFIED'
    assert report['execution_outcome'] == 'RETURNED'
    assert written == waits == []


def test_same_import_success_failure_success_reports_and_retained_frames_are_isolated(
        monkeypatch, model, tmp_path):
    module, clock, waits = model
    written = writers(monkeypatch, module, tmp_path)
    install_pages(monkeypatch, module, {DETAIL: detail_frames()})
    sink_a = {}
    frames_a, report_a = private_result(module, sink_a)
    retained = [frame.copy(deep=True) for frame in frames_a]
    snapshot = json.dumps(report_a, ensure_ascii=False, sort_keys=True)
    clock[0] += 10
    install_pages(monkeypatch, module, {URL + '/bad': detail_frames() + [UNKNOWN]})
    sink_failure = {}
    with pytest.raises(ValueError, match='DOHOD_UNKNOWN_NONEMPTY_TABLE'):
        private_result(module, sink_failure)
    clock[0] += 10
    target_b = URL + '/bbb'
    install_pages(monkeypatch, module, {target_b: detail_frames(amount=15)})
    sink_b = {}
    frames_b, report_b = private_result(module, sink_b)
    assert report_a is sink_a['report'] and report_b is sink_b['report']
    assert len({id(report_a), id(report_b), id(sink_failure['report'])}) == 3
    assert json.dumps(report_a, ensure_ascii=False, sort_keys=True) == snapshot
    assert report_b['expected'] == [target_b] and report_b['missing'] == []
    assert report_b['counts']['accepted'] == 1
    assert all(set(frame['page_url']) == {target_b} for frame in frames_b)
    assert frames_b[1]['Сумма'].tolist() == [15, 15]
    assert frames_b[2]['Выплата'].tolist() == [15]
    for original, expected in zip(frames_a, retained):
        pd.testing.assert_frame_equal(original, expected, check_exact=True)
    assert written == waits == [] and clock[0] == 20


def test_historical_overview_profile_is_consumer_policy_not_live_html(
        monkeypatch, model, tmp_path):
    module, _, waits = model
    columns = ['Акция', 'Сектор', 'Период', 'Выплата на акцию (прогноз)',
        'Доходность выплаты', 'Цена расчета доходности',
        'Дата закрытия реестра (оценка)', 'Капитализация, млн. руб.', 'DSI']
    expected = pd.DataFrame([['Газпром', 'Нефть', '2025 (прогноз)', 10, '5%',
        200, None, 1000, 0.3]], columns=columns, index=[7])
    source(monkeypatch, module, [(URL, reply(b'overview'))])
    parse_map(monkeypatch, module, {b'overview': [expected]})
    written = writers(monkeypatch, module, tmp_path)
    actual = module.get_main(URL)
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)
    pd.testing.assert_frame_equal(written[0][3], expected, check_exact=True)
    assert_writers(written, FILES[:1])
    assert waits == []


def test_historical_detail_profiles_keep_forecast_strings_dates_nulls_and_indices(
        monkeypatch, model, tmp_path):
    rate = detail_frames()[0]
    history = pd.DataFrame([['2025 (прогноз)', 10, None], ['2025 (прогноз)', 10, None]],
        columns=['Год', 'Дивиденд (руб.)', 'Изм. к пред. году'], index=[4, 4])
    payments = pd.DataFrame([[None, '2024-07-11', '2024', 10]], columns=[
        'Дата объявления дивиденда', 'Дата закрытия реестра',
        'Год для учета дивиденда', 'Дивиденд'], index=[9])
    module, _, _, written, _ = public_detail(monkeypatch, model, tmp_path, [rate, history, payments])
    actual_rate, actual_history, actual_payments = module.get_page_info(URL)
    expected_history = history.copy(deep=True)
    expected_history['page_url'] = DETAIL
    expected_history['ticker'] = 'AAA'
    expected_payments = payments.copy(deep=True)
    expected_payments['page_url'] = DETAIL
    expected_payments['ticker'] = 'AAA'
    pd.testing.assert_frame_equal(actual_history, expected_history, check_exact=True)
    pd.testing.assert_frame_equal(actual_payments, expected_payments, check_exact=True)
    assert actual_rate.index.tolist() == [0, 2] and actual_rate.columns.name == 1
    assert actual_history.index.tolist() == [4, 4]
    assert actual_history.duplicated().tolist() == [False, True]
    assert actual_payments.iloc[0, 0] is None
    assert written == []


@pytest.mark.parametrize('case', ['multiindex', 'duplicate-labels', 'metrics-header-moved'])
def test_unsupported_source_shape_is_not_guessed(monkeypatch, model, tmp_path, case):
    rate, history, payments = detail_frames()
    if case == 'multiindex':
        history.columns = pd.MultiIndex.from_tuples([('Период', 'Год'), ('Платеж', 'Сумма')])
    elif case == 'duplicate-labels':
        history.columns = ['Год', 'Год']
    else:
        rate = rate.iloc[[1, 0, 2]].reset_index(drop=True)
    module, _, _, written, _ = public_detail(monkeypatch, model, tmp_path, [rate, history, payments])
    with pytest.raises(ValueError, match='DOHOD_TABLE_SCHEMA_UNSUPPORTED'):
        module.get_page_info(URL)
    assert written == []


@pytest.mark.parametrize('public_name', ['get_main', 'main'])
@pytest.mark.parametrize('with_context', [False, True])
def test_new_overview_semantic_error_uses_safe_route_once(
        monkeypatch, model, tmp_path, context, public_name, with_context, capsys):
    module, _, waits = model
    body = b'overview'
    source(monkeypatch, module, [(URL, reply(body))])
    unknown = pd.DataFrame([[PRIVATE_MARKER + ' ' + CANARY + ' ' + PATH_MARKER]],
        columns=['Changed schema without anchors'])
    parse_map(monkeypatch, module, {body: [overview_frame(), unknown]})
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError, match='DOHOD_UNKNOWN_NONEMPTY_TABLE') as caught:
        getattr(module, public_name)(URL, logging_context=context if with_context else None)
    assert caught.value.args == ('DOHOD_UNKNOWN_NONEMPTY_TABLE',)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    rendered = ''.join(traceback.format_exception(caught.value))
    output = capsys.readouterr()
    values = events(context)
    terminal = [event for event in values if event['event'] == 'dohod_acquisition_failed']
    assert len(terminal) == (1 if with_context else 0)
    if terminal:
        record = terminal[0]['error']
        assert record['code'] == 'DOHOD_UNKNOWN_NONEMPTY_TABLE'
        assert record['endpoint'] is None
        assert record['null_reasons']['endpoint'] == 'RAW_ENDPOINT_NOT_EMITTED'
        assert record['final'] and not record['retryable']
    private_projection = json.dumps(terminal, ensure_ascii=False) + rendered + output.out + output.err
    for marker in (PRIVATE_MARKER, CANARY, PATH_MARKER, URL):
        assert marker not in private_projection
    assert written == waits == []


@pytest.mark.parametrize('with_context', [False, True])
def test_legacy_overview_parser_error_identity_and_existing_chain_unchanged(
        monkeypatch, model, tmp_path, context, with_context):
    module, _, waits = model
    cause = LookupError('fixture original cause')
    error = ValueError('fixture overview parser failure')
    error.__cause__ = cause
    calls = source(monkeypatch, module, [(URL, reply(b'overview'))])
    parse_map(monkeypatch, module, {b'overview': error})
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError) as caught:
        module.get_main(URL, logging_context=context if with_context else None)
    assert caught.value is error and error.args == ('fixture overview parser failure',)
    assert error.__cause__ is cause and error.__context__ is None
    assert len(calls) == 1 and written == waits == []
    values = events(context)
    assert sum(event['event'] == 'dohod_acquisition_failed' for event in values) == int(with_context)


def seed_previous(tmp_path):
    targets = tmp_path / 'datasets/dividends/dohodru'
    targets.mkdir(parents=True)
    originals = {name: ('previous:' + name).encode('utf-8') for _, name in FILES}
    for name, value in originals.items():
        (targets / name).write_bytes(value)
    return targets, originals, {name: hashlib.sha256(value).hexdigest()
        for name, value in originals.items()}


@pytest.mark.parametrize('writer_number', range(1, 10))
@pytest.mark.parametrize('mutation_phase', ['before', 'after'])
def test_writer_failure_preserves_actual_prefix_not_a_snapshot_transaction(
        monkeypatch, model, tmp_path, writer_number, mutation_phase):
    module, _, waits = model
    targets, originals, hashes = seed_previous(tmp_path)
    calls = source(monkeypatch, module, [(URL, reply(b'overview')),
        (URL, reply(DISCOVERY)), (DETAIL, reply(b'detail'))])
    parse_map(monkeypatch, module, {b'overview': [overview_frame()], b'detail': detail_frames()})
    error = OSError('fixture writer failure')
    def action(number, method, name):
        if number == writer_number and mutation_phase == 'before':
            raise error
        (targets / name).write_bytes(('updated:' + name).encode('utf-8'))
        if number == writer_number:
            raise error
    written = writers(monkeypatch, module, tmp_path, action=action)
    with pytest.raises(OSError) as caught:
        module.main(URL)
    assert caught.value is error and caught.value.args == ('fixture writer failure',)
    assert_writers(written, FILES[:writer_number])
    changed = writer_number if mutation_phase == 'after' else writer_number - 1
    for index, (_, name) in enumerate(FILES):
        actual = (targets / name).read_bytes()
        if index < changed:
            assert actual == ('updated:' + name).encode('utf-8')
            assert hashlib.sha256(actual).hexdigest() != hashes[name]
        else:
            assert actual == originals[name]
            assert hashlib.sha256(actual).hexdigest() == hashes[name]
    assert len(calls) == (1 if writer_number <= 3 else 3) and waits == []


@pytest.mark.parametrize('failure', ['unknown-table', 'missing-payments', 'network'])
def test_late_failure_preserves_six_previous_hashes_while_first_three_changed(
        monkeypatch, model, tmp_path, failure):
    module, _, waits = model
    targets, originals, hashes = seed_previous(tmp_path)
    error = requests.exceptions.SSLError('fixture late TLS failure')
    terminal = error if failure == 'network' else reply(b'detail')
    calls = source(monkeypatch, module, [(URL, reply(b'overview')),
        (URL, reply(DISCOVERY)), (DETAIL, terminal)])
    detail = detail_frames(payments=failure != 'missing-payments')
    if failure == 'unknown-table':
        detail += [UNKNOWN]
    parse_map(monkeypatch, module, {b'overview': [overview_frame()], b'detail': detail})
    written = writers(monkeypatch, module, tmp_path, action=lambda number, method, name:
        (targets / name).write_bytes(('updated:' + name).encode('utf-8')))
    with pytest.raises(requests.exceptions.SSLError if failure == 'network' else ValueError) as caught:
        module.main(URL)
    if failure == 'network':
        assert caught.value is error
    assert_writers(written, FILES[:3])
    for _, name in FILES[:3]:
        assert (targets / name).read_bytes() == ('updated:' + name).encode('utf-8')
    for _, name in FILES[3:]:
        actual = (targets / name).read_bytes()
        assert actual == originals[name] and hashlib.sha256(actual).hexdigest() == hashes[name]
    assert len(calls) == 3 and waits == []


def test_overview_failure_before_early_admission_preserves_all_previous_hashes(
        monkeypatch, model, tmp_path):
    module, _, waits = model
    targets, originals, hashes = seed_previous(tmp_path)
    source(monkeypatch, module, [(URL, reply(b'overview'))])
    parse_map(monkeypatch, module, {b'overview': [overview_frame().iloc[:0]]})
    written = writers(monkeypatch, module, tmp_path)
    with pytest.raises(ValueError, match='DOHOD_EMPTY_ROLE_NOT_VERIFIED'):
        module.main(URL)
    assert written == waits == []
    for _, name in FILES:
        actual = (targets / name).read_bytes()
        assert actual == originals[name] and hashlib.sha256(actual).hexdigest() == hashes[name]


def assert_coverage_event_shape(value):
    assert value['stage'] == 'dohodru_data' and value['fields'] == {}
    assert value['error'] is None
    assert value['null_reasons']['error'] in ('NO_ERROR', 'PRIMARY_ERROR_REPORTED_SEPARATELY')
    for key in run_logging.EVENT_NULLABLE:
        assert key in value
        if value[key] is None:
            assert value['null_reasons'][key]
    counts = value['counts']
    assert set(counts) == {'rows_received', 'rows_accepted', 'rows_quarantined',
        'pages', 'instruments', 'null_reasons'}
    assert counts['rows_received'] is counts['rows_accepted'] is counts['rows_quarantined'] is None
    assert counts['instruments'] is None and type(counts['pages']) is int
    assert counts['null_reasons'] == {
        'rows_received': 'HETEROGENEOUS_TABLE_ROWS_NOT_AGGREGATED',
        'rows_accepted': 'HETEROGENEOUS_TABLE_ROWS_NOT_AGGREGATED',
        'rows_quarantined': 'NOT_EVALUATED', 'instruments': 'SOURCE_UNIVERSE_NOT_VERIFIED'}
    assert value['elapsed_ms'] >= 0 and value['duration_ms'] >= 0


def test_coverage_events_exact_literal_failure_sequence_and_safe_mapper(
        monkeypatch, model, tmp_path, context, capsys):
    module, _, waits = model
    a, b, c, d = [URL + '/' + slug for slug in ('aaa', 'bbb', 'ccc', 'ddd')]
    unknown = pd.DataFrame([[PRIVATE_MARKER + CANARY + PATH_MARKER]], columns=['New schema'])
    install_pages(monkeypatch, module,
        {a: detail_frames(), b: detail_frames(), c: detail_frames() + [unknown]}, urls=[a, b, c, d])
    written = writers(monkeypatch, module, tmp_path)
    sink = {}
    with pytest.raises(ValueError, match='DOHOD_UNKNOWN_NONEMPTY_TABLE') as caught:
        private_result(module, sink, context)
    values = events(context)
    assert [value['event'] for value in values] == ['dohod_request_received'] * 4 + [
        'dohod_page_coverage'] * 4 + ['dohod_coverage_summary', 'dohod_acquisition_failed']
    coverage = [value for value in values if value['event'] in
        ('dohod_page_coverage', 'dohod_coverage_summary')]
    for value in coverage:
        assert_coverage_event_shape(value)
    assert [value['outcome'] for value in coverage] == [
        'ACCEPTED', 'ACCEPTED', 'NOT_VERIFIED', 'NOT_RUN', 'INCOMPLETE']
    assert [value['counts']['pages'] for value in coverage] == [1, 1, 1, 0, 3]
    for value, target in zip(coverage[:4], [a, b, c, d]):
        assert value['page'] == 'dohod-page-' + hashlib.sha256(target.encode('ascii')).hexdigest()
    summary = coverage[-1]
    assert summary['page'] is None and summary['null_reasons']['page'] == 'AGGREGATE_COVERAGE'
    assert summary['retries'] is None and summary['null_reasons']['retries'] == 'MULTIPLE_LOGICAL_OPERATIONS'
    error = values[-1]['error']
    assert error['code'] == 'DOHOD_UNKNOWN_NONEMPTY_TABLE'
    assert error['endpoint'] is None and error['null_reasons']['endpoint'] == 'RAW_ENDPOINT_NOT_EMITTED'
    for key in run_logging.ERROR_NULLABLE:
        assert key in error
        if error[key] is None:
            assert error['null_reasons'][key]
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    output = capsys.readouterr()
    safe = json.dumps(coverage + [values[-1]], ensure_ascii=False)
    safe += ''.join(traceback.format_exception(caught.value)) + output.out + output.err
    for marker in (PRIVATE_MARKER, CANARY, PATH_MARKER, a, b, c, d):
        assert marker not in safe
    assert sink['report']['diagnostics_status'] == 'CONFIRMED'
    assert written == waits == []


def test_success_coverage_batch_has_one_page_and_summary_and_no_terminal_error(
        monkeypatch, model, tmp_path, context):
    module, _, _ = model
    install_pages(monkeypatch, module, {DETAIL: detail_frames()})
    writers(monkeypatch, module, tmp_path)
    sink = {}
    private_result(module, sink, context)
    values = events(context)
    assert [value['event'] for value in values] == ['dohod_request_received'] * 2 + [
        'dohod_page_coverage', 'dohod_coverage_summary']
    for value in values[-2:]:
        assert_coverage_event_shape(value)
        assert value['null_reasons']['error'] == 'NO_ERROR'
    assert values[-2]['retries'] == {'attempt': 1, 'limit': 3}
    assert values[-1]['outcome'] == 'COMPLETE_OBSERVED'
    assert sink['report']['diagnostics_status'] == 'CONFIRMED'


@pytest.mark.parametrize('primary_failure', [False, True])
def test_coverage_batch_refusal_never_replays_and_preserves_primary_error(
        monkeypatch, model, tmp_path, context, primary_failure):
    module, _, waits = model
    tls_error = requests.exceptions.SSLError('fixture preserved TLS failure')
    answers = {DETAIL: detail_frames(), URL + '/bbb': tls_error if primary_failure else detail_frames()}
    calls, _ = install_pages(monkeypatch, module, answers)
    written = writers(monkeypatch, module, tmp_path)
    real_emit = run_logging.emit_event
    attempts = []
    def emit(session, level, event, message, fields=None):
        if event in ('dohod_page_coverage', 'dohod_coverage_summary', 'dohod_acquisition_failed'):
            attempts.append(event)
            if event == 'dohod_page_coverage' and attempts.count(event) == 2:
                return {'accepted': False, 'confirmed': False}
        return real_emit(session, level, event, message, fields)
    monkeypatch.setattr(run_logging, 'emit_event', emit)
    sink = {}
    error_type = requests.exceptions.SSLError if primary_failure else RuntimeError
    with pytest.raises(error_type) as caught:
        private_result(module, sink, context)
    if primary_failure:
        assert caught.value is tls_error and caught.value.args == ('fixture preserved TLS failure',)
    else:
        assert caught.value.args == ('LOGGING_INCOMPLETE',)
        assert caught.value.__context__ is None and caught.value.__cause__ is None
    assert attempts == ['dohod_page_coverage', 'dohod_page_coverage', 'dohod_acquisition_failed']
    assert sink['report']['finalized'] is True
    assert sink['report']['coverage_delivery_attempted'] is True
    assert sink['report']['diagnostics_status'] == 'INCOMPLETE'
    assert len(calls) == 3 and written == waits == []
    values = events(context)
    assert sum(value['event'] == 'dohod_page_coverage' for value in values) == 1
    assert sum(value['event'] == 'dohod_acquisition_failed' for value in values) == 1


def test_summary_flush_failure_does_not_resend_coverage(monkeypatch, model, tmp_path, context):
    module, _, waits = model
    install_pages(monkeypatch, module, {DETAIL: detail_frames()})
    written = writers(monkeypatch, module, tmp_path)
    real_emit, real_flush = run_logging.emit_event, run_logging.flush_logging
    attempts = []
    def emit(session, level, event, message, fields=None):
        if event in ('dohod_page_coverage', 'dohod_coverage_summary', 'dohod_acquisition_failed'):
            attempts.append(event)
        return real_emit(session, level, event, message, fields)
    def flush(session):
        if 'dohod_coverage_summary' in attempts:
            return {'confirmed': False}
        return real_flush(session)
    monkeypatch.setattr(run_logging, 'emit_event', emit)
    monkeypatch.setattr(run_logging, 'flush_logging', flush)
    sink = {}
    with pytest.raises(RuntimeError, match='LOGGING_INCOMPLETE') as caught:
        private_result(module, sink, context)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    assert attempts == ['dohod_page_coverage', 'dohod_coverage_summary', 'dohod_acquisition_failed']
    assert sink['report']['diagnostics_status'] == 'INCOMPLETE'
    monkeypatch.setattr(run_logging, 'flush_logging', real_flush)
    values = events(context)
    assert sum(value['event'] == 'dohod_page_coverage' for value in values) == 1
    assert sum(value['event'] == 'dohod_coverage_summary' for value in values) == 1
    assert sum(value['event'] == 'dohod_acquisition_failed' for value in values) == 1
    assert written == waits == []


def test_metrics_source_column_labels_do_not_replace_physical_header_contract(
        monkeypatch, model, tmp_path):
    rate, history, payment = detail_frames()
    rate.columns = ['Доходность-источник', 'Доля-источник', 'DSI-источник']
    module, calls, _, written, waits = public_detail(
        monkeypatch, model, tmp_path, [rate, history, payment])
    frames = module.get_page_info(URL)
    assert_detail(frames)
    assert [call[0] for call in calls] == [URL, DETAIL] and written == waits == []


def test_repeated_metrics_triad_in_source_rows_is_preserved_without_dedup(
        monkeypatch, model, tmp_path):
    _, history, payment = detail_frames()
    triad = ['текущая доходность', 'доля от прибыли', 'индекс DSI']
    rate = pd.DataFrame([triad, triad, ['10%', '20%', '0.3'], triad])
    module, calls, _, written, waits = public_detail(
        monkeypatch, model, tmp_path, [rate, history, payment])
    frames = module.get_page_info(URL)
    expected = pd.DataFrame([
        ['текущая доходность', 'доля от прибыли', 'индекс DSI', DETAIL, 'AAA'],
        ['10%', '20%', '0.3', DETAIL, 'AAA'],
        ['текущая доходность', 'доля от прибыли', 'индекс DSI', DETAIL, 'AAA'],
    ], columns=RATE_COLUMNS, index=[0, 2, 3]).astype('object')
    expected.columns.name = 1
    pd.testing.assert_frame_equal(frames[0], expected, check_exact=True)
    assert frames[0].duplicated().tolist() == [False, False, True]
    assert [call[0] for call in calls] == [URL, DETAIL] and written == waits == []


def test_single_source_table_matching_history_and_metrics_is_role_conflict(
        monkeypatch, model, tmp_path):
    rate, history, payment = detail_frames()
    rate.columns = ['Год', 'Дивиденд (руб.)', 'Изм. к пред. году']
    module, calls, _, written, waits = public_detail(
        monkeypatch, model, tmp_path, [rate, history, payment])
    with pytest.raises(ValueError, match='DOHOD_TABLE_ROLE_CONFLICT') as caught:
        module.get_page_info(URL)
    assert caught.value.args == ('DOHOD_TABLE_ROLE_CONFLICT',)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    assert [call[0] for call in calls] == [URL, DETAIL] and written == waits == []


@pytest.mark.parametrize('detail_number', [1, 2])
@pytest.mark.parametrize('error_type', [RuntimeError, KeyboardInterrupt])
def test_clock_failure_during_detail_state_initialization_keeps_finalized_sink(
        monkeypatch, model, tmp_path, detail_number, error_type):
    module, _, waits = model
    urls = [DETAIL] if detail_number == 1 else [DETAIL, URL + '/bbb']
    calls, parsed = install_pages(monkeypatch, module,
        {target: detail_frames() for target in urls})
    written = writers(monkeypatch, module, tmp_path)
    real_operation_state, real_clock = module._operation_state, module._network_clock
    error = error_type('fixture clock failed during detail initialization')
    armed, fired = [False], []
    def clock():
        if armed[0]:
            armed[0] = False
            fired.append(urls[-1])
            raise error
        return real_clock()
    def operation_state(target):
        if target == urls[-1]:
            armed[0] = True
        return real_operation_state(target)
    monkeypatch.setattr(module, '_network_clock', clock)
    monkeypatch.setattr(module, '_operation_state', operation_state)
    sink = {}
    with pytest.raises(error_type) as caught:
        private_result(module, sink)
    assert caught.value is error
    assert caught.value.args == ('fixture clock failed during detail initialization',)
    assert caught.value.__context__ is None and caught.value.__cause__ is None
    assert fired == [urls[-1]]
    report = sink['report']
    assert_finished_report(report)
    assert report['execution_outcome'] == ('FAILED' if error_type is RuntimeError else 'INTERRUPTED')
    assert report['diagnostics_status'] == 'NOT_REQUESTED'
    assert report['expected'] == urls and report['missing'] == [urls[-1]]
    assert report['counts'] == dict(expected=detail_number, requested=detail_number - 1,
        received=detail_number - 1, parsed=detail_number - 1, validated=detail_number - 1,
        accepted=detail_number - 1, failed=0, not_run=1, missing=1)
    pending = report['pages'][-1]
    assert pending['url'] == urls[-1] and pending['stage'] == 'DISCOVERED'
    assert pending['outcome'] == 'NOT_RUN' and pending['reason'] == 'PRIOR_PAGE_FAILED'
    assert pending['requested'] is False and pending['sends'] == 0
    if detail_number == 2:
        retained = report['pages'][0]
        assert retained['stage'] == retained['outcome'] == 'ACCEPTED'
        assert retained['reason'] is None and retained['accepted'] is True
        assert retained['roles']['metrics']['rows'] == 2
    assert [call[0] for call in calls] == [URL] + urls[:-1]
    assert parsed == ([] if detail_number == 1 else [b'page-0'])
    assert written == waits == []
