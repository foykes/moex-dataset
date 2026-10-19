"""Real disposable logging transport and narrow main-context forwarding."""

import importlib.util
from io import StringIO
import json
from pathlib import Path
import sys
import types

import pandas as pd
import pytest

import run_logging


ROOT = Path(__file__).resolve().parents[3]
BASE = 'ebc66fc376a2077ce027cf33689aae31728709c2'
SEARCH_URL = 'https://iss.moex.com/iss/securities.json?q=RU0009029540&iss.meta=off'
DETAIL_URL = 'https://iss.moex.com/iss/securities/SBER/dividends.json'
SEARCH = {'securities': {'columns': ['secid', 'shortname', 'isin'],
    'data': [['MOEX', 'МосБиржа', 'RU000A0JR4A1'],
             ['SBER', 'Сбербанк', 'RU0009029540'],
             ['SBER', 'Другое имя', 'RU0009029540']]}}
PAYOUTS = {'dividends': {'columns': ['secid', 'isin', 'registryclosedate', 'value', 'currencyid'],
    'data': [['SBER', 'RU0009029540', '2024-07-11', 33.3, 'RUB'],
             ['SBER', 'RU0009029540', '2024-07-11', 1.5, 'USD'],
             ['SBER', 'RU0009029540', '2024-07-11', 7.0, 'RUB']]}}
CANARY = 'C1_SYNTHETIC_SECRET_713'
EVENT_KEYS = set('timestamp_utc level event stage message run_id producer_sha process worker snapshot_id release_id dataset_id instrument interval page file artifact_id target_id outcome elapsed_ms duration_ms counts retries error fields null_reasons'.split())
ERROR_KEYS = set('category code stage retryable final message run_id snapshot_id dataset_id instrument interval page file artifact_id target_id endpoint attempt exception_type null_reasons'.split())


def load(name):
    spec = importlib.util.spec_from_file_location('c1_' + name, ROOT / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def context(tmp_path):
    # The real queue/listener only writes inside this guard-issued disposable root.
    session = run_logging.configure_logging(tmp_path, run_id='c1-disposable',
        environment='offline', producer_sha=BASE,
        config={'console_level': 'ERROR'}, sensitive_values=(CANARY,))
    yield session
    run_logging.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)


def source(monkeypatch, module, detail=PAYOUTS):
    calls = []
    def opened(url, *, timeout=None):
        calls.append(url)
        return StringIO(json.dumps(SEARCH if url == SEARCH_URL else detail, ensure_ascii=False))
    monkeypatch.setattr(module.urllib.request, 'urlopen', opened)
    return calls


def records(context):
    assert run_logging.flush_logging(context)['confirmed']
    values = [json.loads(line) for line in
              (context['_run_root'] / 'events.jsonl').read_text(encoding='utf-8').splitlines()]
    for event in values:
        assert set(event) == EVENT_KEYS
        assert event['fields'] == {}
        assert 'endpoint' not in event
        if event['error'] is not None:
            assert set(event['error']) == ERROR_KEYS
    return values


@pytest.mark.parametrize('isin', [None, float('nan'), pd.NA, '', 42, 'ru0009029540'])
@pytest.mark.parametrize('ticker', ['SBER', float('nan')])
def test_invalid_identity_with_real_logging_context(monkeypatch, context, isin, ticker):
    module = load('dividends')
    calls = source(monkeypatch, module)
    with pytest.raises(ValueError, match='^DIVIDEND_ISIN_INVALID:'):
        module.div_loader(isin, ticker, logging_context=context)
    assert calls == [] and module.divs_all == []
    events = records(context)
    assert [event['event'] for event in events] == ['dividend_loader_failed']
    error = events[0]['error']
    assert error['category'] == 'QUALITY' and error['code'] == 'DIVIDEND_ISIN_INVALID'
    assert error['endpoint'] is None and error['null_reasons']['endpoint']
    assert error['instrument'] == ('SBER' if type(ticker) is str else None)
    report = run_logging.finalize_logging(context, execution_outcome='FAILED', exit_status=1)
    assert report['exit_status'] == 1 and report['delivery_outcome'] == 'COMPLETE'
    assert report['errors'][0] == error


@pytest.mark.parametrize('fault', ['make_error', 'emit_event', 'unconfirmed', 'stderr'])
def test_primary_identity_error_survives_secondary_logger_failure(monkeypatch, context, fault):
    module = load('dividends')
    calls = source(monkeypatch, module)
    primary = ValueError('DIVIDEND_ISIN_INVALID: original fixture')
    def invalid(*args):
        raise primary
    monkeypatch.setattr(module, 're', types.SimpleNamespace(fullmatch=invalid))
    def failed(*args, **kwargs):
        raise ValueError('secondary ' + CANARY)
    if fault == 'make_error':
        monkeypatch.setattr(run_logging, 'make_error', failed)
    elif fault == 'unconfirmed':
        monkeypatch.setattr(run_logging, 'emit_event', lambda *a: {'accepted': True, 'confirmed': False})
    else:
        monkeypatch.setattr(run_logging, 'emit_event', failed)
    if fault == 'stderr':
        monkeypatch.setattr(module, 'print', failed, raising=False)
    with pytest.raises(ValueError) as caught:
        module.div_loader('RU0009029540', 'SBER', logging_context=context)
    assert caught.value is primary
    assert calls == [] and module.divs_all == []
    run_logging.finalize_logging(context, execution_outcome='FAILED', exit_status=1)


def test_named_identity_events_and_canonical_counts(monkeypatch, context):
    module = load('dividends')
    calls = source(monkeypatch, module)
    assert module.div_loader('RU0009029540', 'SBER', logging_context=context) is None
    assert calls == [SEARCH_URL, DETAIL_URL]
    events = records(context)
    assert [event['event'] for event in events] == [
        'dividend_identity_search_started', 'dividend_identity_selected',
        'dividend_request_started', 'dividend_loader_returned']
    assert events[1]['counts'] == {'rows_received': 3, 'rows_accepted': 2,
        'rows_quarantined': None, 'pages': 1, 'instruments': 1,
        'null_reasons': {'rows_quarantined': 'NOT_EVALUATED'}}
    assert events[3]['counts']['rows_received'] == events[3]['counts']['rows_accepted'] == 3
    assert events[3]['duration_ms'] >= 0
    assert all(event['stage'] == 'dividends' and event['error'] is None for event in events)
    report = run_logging.finalize_logging(context, execution_outcome='COMPLETED', exit_status=0)
    assert report['delivery_outcome'] == 'COMPLETE' and report['storage_sealed']
    assert report['exit_status'] == 0


def test_missing_dividends_is_separate_source_error(monkeypatch, context):
    module = load('dividends')
    calls = source(monkeypatch, module, {'description': {}, 'boards': {}})
    with pytest.raises(KeyError) as caught:
        module.div_loader('RU0009029540', 'SBER', logging_context=context)
    assert caught.value.args == ('dividends',)
    assert calls == [SEARCH_URL, DETAIL_URL] and module.divs_all == []
    error = records(context)[-1]['error']
    assert error['category'] == 'SOURCE' and error['code'] == 'DIVIDEND_SOURCE_SCHEMA_INVALID'
    assert error['endpoint'] == DETAIL_URL and error['instrument'] == 'SBER'
    assert 'endpoint' not in error['null_reasons']
    run_logging.finalize_logging(context, execution_outcome='FAILED', exit_status=1)


def test_source_exception_identity_and_redaction(monkeypatch, context, capsys):
    module = load('dividends')
    primary = OSError('https://user:' + CANARY + '@example.invalid/data?token=' + CANARY)
    calls = []
    def failed(url, *, timeout=None):
        calls.append(url)
        raise primary
    monkeypatch.setattr(module.urllib.request, 'urlopen', failed)
    with pytest.raises(OSError) as caught:
        module.div_loader('RU0009029540', 'SBER', logging_context=context)
    assert caught.value is primary and calls == [SEARCH_URL]
    error = records(context)[-1]['error']
    # The existing logger strips all URL queries, including this public ISIN.
    assert error['endpoint'] == 'https://iss.moex.com/iss/securities.json'
    assert error['code'] == 'DIVIDEND_SEARCH_FAILED'
    report = run_logging.finalize_logging(context, execution_outcome='FAILED', exit_status=1)
    assert report['exit_status'] == 1 and report['delivery_outcome'] == 'COMPLETE'
    output = capsys.readouterr()
    stored = ''.join(path.read_text(encoding='utf-8') for path in
                     context['_run_root'].iterdir() if path.suffix in ('.json', '.jsonl'))
    assert CANARY not in output.out + output.err + stored
    assert 'user:' not in stored and '?token=' not in stored


@pytest.mark.parametrize('fault', ['rejected_info', 'unhealthy', 'unconfirmed_flush'])
def test_failed_logging_cannot_report_success(monkeypatch, context, fault):
    module = load('dividends')
    calls = source(monkeypatch, module)
    if fault == 'rejected_info':
        original = run_logging.emit_event
        monkeypatch.setattr(run_logging, 'emit_event', lambda *a:
            {'accepted': False, 'confirmed': False} if a[1] == 'INFO' else original(*a))
    elif fault == 'unhealthy':
        monkeypatch.setattr(run_logging, 'check_logging_health', lambda *a: {'healthy': False})
    else:
        monkeypatch.setattr(run_logging, 'flush_logging', lambda *a: {'confirmed': False})
    with pytest.raises(RuntimeError, match='^LOGGING_INCOMPLETE$'):
        module.div_loader('RU0009029540', 'SBER', logging_context=context)
    assert calls == ([SEARCH_URL, DETAIL_URL] if fault == 'unconfirmed_flush' else [])
    run_logging.finalize_logging(context, execution_outcome='FAILED', exit_status=1)


def test_logging_preserves_main_export_input(monkeypatch, context, tmp_path):
    module = load('dividends')
    module.current_path = str(tmp_path)
    monkeypatch.setattr(pd, 'read_excel', lambda path:
        pd.DataFrame({'TRADE_CODE': ['SBER'], 'ISIN': ['RU0009029540']}))
    exports = []
    for method in ('to_excel', 'to_csv'):
        monkeypatch.setattr(pd.DataFrame, method,
            lambda frame, path, index: exports.append((path, index, frame.copy())))
    calls = source(monkeypatch, module)
    assert module.main(logging_context=context) is None
    assert calls == [SEARCH_URL, DETAIL_URL]
    expected = pd.DataFrame([
        ['RU0009029540', 'SBER', '2024-07-11', 33.3, 'RUB'],
        ['RU0009029540', 'SBER', '2024-07-11', 1.5, 'USD'],
        ['RU0009029540', 'SBER', '2024-07-11', 7.0, 'RUB']],
        columns=['ISIN', 'TRADE_CODE', 'dt', 'value', 'currency'])
    assert [(Path(path).name, index) for path, index, frame in exports] == [
        ('all.xlsx', False), ('all.csv', False)]
    for path, index, frame in exports:
        pd.testing.assert_frame_equal(frame, expected)
    events = records(context)
    export = next(event for event in events if event['event'] == 'dividend_export_input')
    assert export['counts']['rows_accepted'] == 3 and export['outcome'] == 'INPUT_PREPARED'
    assert export['counts']['rows_received'] is None and export['counts']['pages'] is None
    # No serialization, publication or previous-file integrity is asserted.
    assert not (tmp_path / 'datasets').exists()


@pytest.mark.parametrize('scenario,isin', [
    ('missing_isin', None), ('nan_isin', float('nan')),
    ('invalid_isin', 'ru0009029540'), ('missing_dividends', 'RU0009029540'),
])
@pytest.mark.parametrize('with_logging', [False, True], ids=['without_context', 'real_context'])
def test_main_failure_stops_before_export(monkeypatch, tmp_path, request,
                                         scenario, isin, with_logging):
    module = load('dividends')
    module.current_path = str(tmp_path)
    session = request.getfixturevalue('context') if with_logging else None
    reads, emitted = [], []
    exports = {'to_excel': [], 'to_csv': []}
    def read_fixture(path):
        reads.append(path)
        # Keep the row when ISIN is NaN: main drops only entirely empty rows.
        return pd.DataFrame({'TRADE_CODE': ['SBER'], 'ISIN': [isin]})
    monkeypatch.setattr(pd, 'read_excel', read_fixture)
    for method in exports:
        monkeypatch.setattr(pd.DataFrame, method,
            lambda *args, kind=method, **kwargs: exports[kind].append((args, kwargs)))
    calls = source(monkeypatch, module, {'description': {}, 'boards': {}})
    emitter = run_logging.emit_event
    def observed(*args, **kwargs):
        receipt = emitter(*args, **kwargs)
        emitted.append((args[2], receipt))
        return receipt
    monkeypatch.setattr(run_logging, 'emit_event', observed)

    missing_block = scenario == 'missing_dividends'
    expected_type = KeyError if missing_block else ValueError
    normal_returns = []
    with pytest.raises(expected_type) as caught:
        normal_returns.append(module.main(logging_context=session) if with_logging else module.main())
    assert type(caught.value) is expected_type
    assert caught.value.args == (('dividends',) if missing_block else
        ('DIVIDEND_ISIN_INVALID: требуется ISIN из 12 символов',))
    assert normal_returns == []
    assert reads == [str(tmp_path) + '/datasets/ticker_lists/moex_full.xlsx']
    assert calls == ([SEARCH_URL, DETAIL_URL] if missing_block else [])
    assert exports['to_excel'] == [] and exports['to_csv'] == []
    forbidden = {'dividend_export_input', 'dividend_collection_returned'}
    assert forbidden.isdisjoint(event for event, receipt in emitted)
    if not with_logging:
        assert emitted == []
        return

    events = records(session)
    assert forbidden.isdisjoint(event['event'] for event in events)
    failures = [event for event in events if event['event'] == 'dividend_loader_failed']
    assert len(failures) == 1
    failure = failures[0]
    assert failure['level'] == 'ERROR' and failure['outcome'] == 'FAILED'
    error = failure['error']
    assert error['stage'] == 'dividends' and error['instrument'] == 'SBER'
    assert error['exception_type'] == expected_type.__name__
    assert error['category'] == ('SOURCE' if missing_block else 'QUALITY')
    assert error['code'] == ('DIVIDEND_SOURCE_SCHEMA_INVALID' if missing_block else 'DIVIDEND_ISIN_INVALID')
    assert error['endpoint'] == (DETAIL_URL if missing_block else None)
    receipts = [receipt for event, receipt in emitted if event == 'dividend_loader_failed']
    assert len(receipts) == 1 and receipts[0]['accepted'] and receipts[0]['confirmed']
    report = run_logging.finalize_logging(session, execution_outcome='FAILED', exit_status=1)
    assert report['execution_outcome'] == 'FAILED' and report['exit_status'] == 1
    assert report['delivery_outcome'] == 'COMPLETE' and report['storage_sealed']
    assert report['evidence_incomplete'] is False
    assert report['expected_producers'][0]['final_confirmed']
    assert report['errors'] == [error]


def _d2_main_profile(source):
    """Select a fixture ABI before executing main; this is not runtime acceptance."""
    import ast
    import hashlib

    source = source.replace(b'\r\n', b'\n')
    if hashlib.sha256(source).hexdigest() == 'c035f4439c060391e38c270f6f7adf257b356e6aae2104b135cf53e770509c3a':
        return 'legacy'
    tree = ast.parse(source.decode('utf-8'))
    markers = [node for node in tree.body if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign))
               and any(isinstance(item, ast.Name) and item.id == 'D2_SOURCE_PROFILE'
                       for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
                       for item in ast.walk(target))]
    assert len(markers) == 1, 'Unknown main source profile'
    marker = markers[0]
    assert (isinstance(marker, ast.Assign) and len(marker.targets) == 1
            and isinstance(marker.targets[0], ast.Name)
            and isinstance(marker.value, ast.Constant)
            and marker.value.value == 'linux-stage-b-v1'), 'Unknown main source profile'
    for name, keywords in (('main', ['logging_context', 'result_sink']), ('_run_cli', [])):
        functions = [node for node in tree.body
                     if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name]
        assert len(functions) == 1 and type(functions[0]) is ast.FunctionDef, 'Unknown main callable'
        function = functions[0]
        args = function.args
        assert (not function.decorator_list and not args.posonlyargs and not args.args
                and args.vararg is None and args.kwarg is None and not args.defaults
                and [arg.arg for arg in args.kwonlyargs] == keywords
                and all(isinstance(value, ast.Constant) and value.value is None
                        for value in args.kw_defaults)), 'Unknown main signature'
    return 'modern'


def _d2_upload_dependency(profile, expected_context, record):
    """Unit double without native observations or a fabricated F-LOG seal."""
    observed = {'runtime': None, 'sink': None, 'returned': False, 'closed': False}

    class UnitRuntime:
        def __init__(self, context):
            self.context = context
            self.bound = False
            self.active = False

        def __enter__(self):
            assert observed['runtime'] is None, 'Duplicate fixture runtime'
            observed['runtime'] = self
            self.active = True
            return self

        def bind_context(self, context):
            assert self.active and (context is None or type(context) is dict), 'Invalid fixture context'
            assert ((not self.bound and self.context is None) or self.context is context), 'Fixture context was replaced'
            self.context = context
            self.bound = True

        def __exit__(self, *error):
            assert self.active, 'Fixture runtime already closed'
            self.active = False
            observed['closed'] = True
            return False

    def runtime_scope(*, logging_context=None):
        assert profile == 'modern' and logging_context is expected_context
        return UnitRuntime(logging_context)

    def main(root, target, *, logging_context=None, result_sink=None, _runtime=None):
        if profile == 'legacy':
            assert logging_context is None and result_sink is None and _runtime is None
            kwargs = {}
        else:
            assert (_runtime is observed['runtime'] and _runtime is not None
                    and _runtime.active and _runtime.bound)
            assert logging_context is expected_context and _runtime.context is expected_context
            assert type(result_sink) is dict and not observed['returned']
            observed['sink'] = result_sink
            kwargs = dict(logging_context=logging_context, result_sink=result_sink, _runtime=_runtime)
        record('upload', (root, target), kwargs)
        observed['returned'] = True

    def finalization(result_sink, report, *, status=0):
        raise AssertionError('Orchestration fixture has no observed F-LOG finalization')

    module = types.ModuleType('upload')
    module.main = main
    module.runtime_scope = runtime_scope
    module._d2_apply_finalization = finalization
    module._d2_completion_allowed = lambda result_sink: False
    module.fixture_kind = 'UNIT_DOUBLE'
    return module, observed


@pytest.mark.parametrize('with_logging', [False, True])
def test_pipeline_forwards_exact_context_only_to_dividends(monkeypatch, context, with_logging):
    path = ROOT / 'main.py'
    source = path.read_bytes()
    profile = _d2_main_profile(source)
    calls = []
    for name in ('data_gathering', 'dividends', 'tech', 'dohodru_data'):
        stub = types.ModuleType(name)
        def main(*args, stage=name, **kwargs):
            calls.append((stage, args, kwargs))
        stub.main = main
        monkeypatch.setitem(sys.modules, name, stub)
    upload, observed = _d2_upload_dependency(profile, context if with_logging else None,
        lambda stage, args, kwargs: calls.append((stage, args, kwargs)))
    monkeypatch.setitem(sys.modules, 'upload', upload)
    specification = importlib.util.spec_from_file_location('c1_main', path)
    module = importlib.util.module_from_spec(specification)
    exec(compile(source, str(path), 'exec'), module.__dict__)
    result_sink = {}
    if profile == 'legacy':
        assert (module.main(logging_context=context) if with_logging else module.main()) is None
    else:
        assert module.main(logging_context=context if with_logging else None,
                           result_sink=result_sink) is None
    assert [stage for stage, args, kwargs in calls] == [
        'data_gathering', 'dividends', 'tech', 'dohodru_data', 'upload']
    assert calls[1][1] == ()
    assert calls[1][2] == ({'logging_context': context} if with_logging else {})
    if with_logging:
        assert calls[1][2]['logging_context'] is context
    if profile == 'legacy':
        assert all(kwargs == {} for stage, args, kwargs in calls if stage != 'dividends')
        assert observed['runtime'] is None and observed['sink'] is None
    else:
        assert all(kwargs == {} for stage, args, kwargs in calls if stage not in {'dividends', 'upload'})
        assert calls[4][2] == dict(logging_context=context if with_logging else None,
                                  result_sink=result_sink, _runtime=observed['runtime'])
        assert calls[4][2]['logging_context'] is (context if with_logging else None)
        assert observed['sink'] is result_sink and observed['closed']
    assert observed['returned']
    assert calls[0][1] == calls[2][1] == (str(ROOT),)
    assert calls[3][1] == ('https://www.dohod.ru/ik/analytics/dividend',)
    assert calls[4][1][0] == str(ROOT) and len(calls[4][1]) == 2
