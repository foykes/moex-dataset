"""Real orchestration/update regressions with literal domain oracles."""

import inspect
import hashlib
import json
from types import SimpleNamespace

import pytest


def call_update(module, config, root, catalogue, snapshot, **kwargs):
    # Baseline reproduction keeps the existing signature valid. GREEN requires
    # the snapshot argument; absence of the new signature is not a RED oracle.
    if 'tickers_dates' in inspect.signature(module.data_update).parameters:
        return module.data_update(config, root, catalogue, snapshot, **kwargs)
    return module.data_update(config, root, catalogue, **kwargs)


def test_empty_root_forwards_same_snapshot(gathering, catalogue, snapshot,
                                         dataset_config, fixture_root, exact_reads,
                                         writers, monkeypatch):
    _, root = fixture_root
    calls, builds = [], []
    monkeypatch.setattr(gathering, 'config', dataset_config)
    monkeypatch.setattr(gathering, 'moex_tickerlists', lambda *args, **kwargs: catalogue)

    def build(*args, **kwargs):
        builds.append((args, kwargs))
        return snapshot

    def reload(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(gathering, 'build_tickers_dates', build)
    monkeypatch.setattr(gathering, 'full_reload', reload)
    gathering.main(root)
    assert len(builds) == 1
    assert len(calls) == 8
    for (args, kwargs), specification in zip(calls, dataset_config):
        assert len(args) == 7, 'cold-start lost the required metadata snapshot'
        assert args[1:6] == (specification['interval'], specification['years'],
                             specification['filename'], specification['word'], root)
        assert args[6] is snapshot
    assert writers == []


def test_clean_catalogue_does_not_require_blank(gathering, catalogue, snapshot,
                                              candle_frame, fixture_root, exact_reads,
                                              writers, monkeypatch, save_dataset):
    root_path, root = fixture_root
    specification = [{'interval': 24, 'years': 10, 'filename': 'fixture_daily', 'word': 'часа'}]
    target = root_path / 'datasets/fixture_daily.csv'
    target.parent.mkdir(parents=True)
    previous = save_dataset(target, candle_frame)
    exact_reads.declared[target] = candle_frame
    candle_calls = []

    def candles(*args):
        candle_calls.append(args)
        return candle_frame.iloc[:0].copy()

    monkeypatch.setattr(gathering, 'moex_query', candles)
    call_update(gathering, specification, root, catalogue, snapshot)
    assert candle_calls == [
        ('SBER', 'Акции', '2024-03-01', '2024-03-02', 24),
        ('MOEX', 'Акции', '2023-03-03', '2024-03-02', 24),
    ]
    assert len(writers) == 2
    assert [record[0] for record in writers] == ['xlsx', 'csv']
    assert all(record[3:] == ((), {'index': False}) for record in writers)
    assert target.read_bytes() == previous


@pytest.mark.parametrize('missing', range(8))
def test_each_missing_dataset(gathering, catalogue, snapshot, candle_frame,
                              dataset_config, fixture_root, exact_reads, writers,
                              monkeypatch, missing, save_dataset):
    root_path, root = fixture_root
    previous, queries, reloads, builds = {}, [], [], []
    for position, spec in enumerate(dataset_config):
        if position == missing:
            continue
        for suffix in ('csv', 'xlsx'):
            target = root_path / ('datasets/' + spec['filename'] + '.' + suffix)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(('PREVIOUS_' + str(position) + '_' + suffix).encode())
            previous[target] = target.read_bytes()
        frame = candle_frame.copy(deep=True)
        frame['RSI14'] = 50.0
        target = root_path / ('datasets/' + spec['filename'] + '.csv')
        previous[target] = save_dataset(target, frame)
        exact_reads.declared[target] = frame
    monkeypatch.setattr(gathering, 'config', dataset_config)
    monkeypatch.setattr(gathering, 'moex_tickerlists', lambda *args, **kwargs: catalogue)

    def build(*args):
        builds.append(args)
        return snapshot

    def query(*args):
        queries.append(args)
        return candle_frame.iloc[:0].copy()

    monkeypatch.setattr(gathering, 'build_tickers_dates', build)
    monkeypatch.setattr(gathering, 'moex_query', query)
    monkeypatch.setattr(gathering, 'full_reload', lambda *args, **kwargs: reloads.append((args, kwargs)))
    gathering.main(root)
    assert len(builds) == 1 and len(reloads) == 1
    args, kwargs = reloads[0]
    spec = dataset_config[missing]
    assert args[1:6] == (spec['interval'], spec['years'], spec['filename'], spec['word'], root)
    assert args[6] is snapshot
    assert kwargs == {'logging_context': None}
    expected_queries = []
    for position, spec in enumerate(dataset_config):
        if position != missing:
            expected_queries += [('SBER', 'Акции', '2024-03-01', '2024-03-02', spec['interval']),
                                 ('MOEX', 'Акции', '2023-03-03', '2024-03-02', spec['interval'])]
    assert queries == expected_queries
    assert len(writers) == 14
    assert [item[0] for item in writers] == ['xlsx', 'csv'] * 7
    assert [item[2] for item in writers] == [
        root + '/datasets/' + spec['filename'] + '.' + suffix
        for position, spec in enumerate(dataset_config) if position != missing
        for suffix in ('xlsx', 'csv')]
    for _, frame, _, args, kwargs in writers:
        assert frame.columns.tolist() == ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']
        assert frame.values.tolist() == candle_frame.values.tolist()
        assert args == () and kwargs == {'index': False}
    assert all(target.read_bytes() == content for target, content in previous.items())


def test_force_reload_same_snapshot(gathering, catalogue, snapshot, dataset_config,
                                    fixture_root, writers, monkeypatch):
    root_path, root = fixture_root
    previous = {}
    for spec in dataset_config:
        for suffix in ('csv', 'xlsx'):
            path = root_path / ('datasets/' + spec['filename'] + '.' + suffix)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'PREVIOUS_FORCE_CONTROL')
            previous[path] = path.read_bytes()
    builds, calls = [], []
    monkeypatch.setattr(gathering, 'config', dataset_config)
    monkeypatch.setattr(gathering, 'moex_tickerlists', lambda *args, **kwargs: catalogue)
    monkeypatch.setattr(gathering, 'build_tickers_dates', lambda *args: builds.append(args) or snapshot)
    monkeypatch.setattr(gathering, 'full_reload', lambda *args, **kwargs: calls.append((args, kwargs)))
    monkeypatch.setattr(gathering, 'data_update', lambda *args, **kwargs: pytest.fail('force must not update'))
    gathering.main(root, force_reload=True)
    assert len(builds) == 1 and len(calls) == 8
    for (args, kwargs), spec in zip(calls, dataset_config):
        assert args[0]['TRADE_CODE'].tolist() == ['MOEX', 'SBER']
        assert args[1:6] == (spec['interval'], spec['years'], spec['filename'], spec['word'], root)
        assert args[6] is snapshot
        assert kwargs == {'logging_context': None}
    assert writers == []
    assert all(path.read_bytes() == content for path, content in previous.items())


@pytest.mark.parametrize('interval', [24, 60, 10, 1])
@pytest.mark.parametrize('years,sber_begin', [(10, '2014-03-05'), (30, '2000-01-03')])
def test_real_full_reload(gathering, snapshot, candle_frame, fixture_root,
                          writers, monkeypatch, interval, years, sber_begin):
    import pandas as pd
    _, root = fixture_root
    source = pd.DataFrame({'TRADE_CODE': [' MOEX ', '', None, 'SBER', 'MOEX'],
                           'SUPERTYPE': ['Акции'] * 5}, index=[4, 8, 12, 17, 25])
    before = source.copy(deep=True)
    calls = []

    def candles(*args):
        calls.append(args)
        result = candle_frame.copy(deep=True)
        result['ticker'] = args[0]
        return result

    monkeypatch.setattr(gathering, 'moex_query', candles)
    gathering.full_reload(source, interval, years, 'fixture_reload', 'минут', root, snapshot)
    assert calls == [('MOEX', 'Акции', '2020-02-29', '2024-03-01', interval),
                     ('SBER', 'Акции', sber_begin, '2024-03-02', interval)]
    assert len(writers) == 2 and [item[0] for item in writers] == ['xlsx', 'csv']
    expected_rows = [
        [100., 101., 102., 99., 202000., 2000, '2024-03-01 00:00:00', '2024-03-01 23:59:59', 'MOEX'],
        [100., 101., 102., 99., 202000., 2000, '2024-03-01 00:00:00', '2024-03-01 23:59:59', 'SBER'],
    ]
    for kind, frame, path, args, kwargs in writers:
        assert frame.columns.tolist() == ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']
        assert frame.values.tolist() == expected_rows
        assert frame.index.tolist() == [0, 0]  # Preserve legacy concat; exports index=False.
        assert frame.dtypes.astype(str).tolist() == ['float64'] * 5 + ['int64', 'object', 'object', 'object']
        assert path.endswith('fixture_reload.' + kind) and args == () and kwargs == {'index': False}
    pd.testing.assert_frame_equal(source, before)


def test_real_update_rows_schema_and_delta_order(gathering, catalogue, snapshot,
                                                candle_frame, fixture_root, exact_reads,
                                                writers, monkeypatch, save_dataset):
    import pandas as pd
    root_path, root = fixture_root
    target = root_path / 'datasets/fixture_daily.csv'
    target.parent.mkdir(parents=True)
    february = [99., 100., 101., 98., 100000., 1000,
                '2024-02-29 00:00:00', '2024-02-29 23:59:59', 'SBER']
    old = pd.DataFrame([february, candle_frame.values.tolist()[0]], columns=candle_frame.columns)
    old['RSI14'] = 50.0
    previous = save_dataset(target, old)
    exact_reads.declared[target] = old
    queries = []
    new_sber = [101., 103., 104., 100., 309000., 3000, '2024-03-02 00:00:00', '2024-03-02 23:59:59', 'SBER']
    new_moex = [10., 11., 12., 9., 11000., 1000, '2024-03-02 00:00:00', '2024-03-02 23:59:59', 'MOEX']

    def candles(*args):
        queries.append(args)
        if args[0] == 'SBER':
            return pd.DataFrame([candle_frame.values.tolist()[0], new_sber], columns=candle_frame.columns)
        return pd.DataFrame([new_moex], columns=candle_frame.columns)

    monkeypatch.setattr(gathering, 'moex_query', candles)
    gathering.data_update([{'interval': 24, 'years': 10, 'filename': 'fixture_daily', 'word': 'часа'}],
                          root, catalogue, snapshot)
    assert queries == [('SBER', 'Акции', '2024-03-01', '2024-03-02', 24),
                       ('MOEX', 'Акции', '2023-03-03', '2024-03-02', 24)]
    expected = [new_moex, february, [100., 101., 102., 99., 202000., 2000,
                           '2024-03-01 00:00:00', '2024-03-01 23:59:59', 'SBER'], new_sber]
    assert len(writers) == 2
    for _, frame, _, args, kwargs in writers:
        assert frame.values.tolist() == expected
        assert frame.index.tolist() == [0, 1, 2, 3]
        assert frame.columns.tolist() == ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']
        assert frame.dtypes.astype(str).tolist() == ['float64'] * 5 + ['int64', 'object', 'object', 'object']
        assert list(zip(frame.ticker, [24] * 4, frame.begin)) == [
            ('MOEX', 24, '2024-03-02 00:00:00'), ('SBER', 24, '2024-02-29 00:00:00'),
            ('SBER', 24, '2024-03-01 00:00:00'),
            ('SBER', 24, '2024-03-02 00:00:00')]
        assert args == () and kwargs == {'index': False}
    assert target.read_bytes() == previous


@pytest.mark.parametrize('case', ['fresh', 'old', 'both', 'lookup-only'])
def test_stored_identity_refusal_preserves_previous(gathering, catalogue, snapshot,
                                                   candle_frame, fixture_root,
                                                   exact_reads, writers, monkeypatch, case, save_dataset):
    import pandas as pd
    root_path, root = fixture_root
    target = root_path / 'datasets/fixture_daily.csv'
    target.parent.mkdir(parents=True)
    previous = {}
    for suffix in ('csv', 'xlsx'):
        path = target.with_suffix('.' + suffix)
        path.write_bytes(('PREVIOUS_' + suffix).encode())
        previous[path] = (path.read_bytes(), hashlib.sha256(path.read_bytes()).hexdigest())
    frame = candle_frame.copy(deep=True)
    frame.loc[0, 'ticker'] = ' SBER ' if case != 'lookup-only' else ' MOEX '
    if case == 'old':
        frame.loc[0, 'end'] = '2024-01-01 23:59:59'
    if case == 'both':
        frame = pd.concat([frame, candle_frame], ignore_index=True)
    frame['RSI14'] = 50.0
    before = frame.copy(deep=True)
    content = save_dataset(target, frame)
    previous[target] = (content, hashlib.sha256(content).hexdigest())
    exact_reads.declared[target] = frame
    if case == 'lookup-only':
        catalogue = catalogue.iloc[[1]].copy()
    queries = []
    monkeypatch.setattr(gathering, 'moex_query', lambda *args: queries.append(args) or pytest.fail('candle called'))
    with pytest.raises(ValueError, match='^A1_STORED_TICKER_INCOMPATIBLE$'):
        gathering.data_update([{'interval': 24, 'years': 10, 'filename': 'fixture_daily', 'word': 'часа'}],
                              root, catalogue, snapshot)
    assert queries == [] and writers == []
    pd.testing.assert_frame_equal(frame, before)
    assert all((path.read_bytes(), hashlib.sha256(path.read_bytes()).hexdigest()) == value
               for path, value in previous.items())


@pytest.mark.parametrize('accepted,healthy', [(False, True), (True, False)])
def test_diagnostics_without_primary_stop_domain(gathering, fixture_root,
                                                 event_log, monkeypatch, accepted, healthy):
    _, root = fixture_root
    event_log.accepted, event_log.healthy = accepted, healthy
    calls = []
    monkeypatch.setattr(gathering, 'moex_tickerlists', lambda *args, **kwargs: calls.append(args))
    with pytest.raises(RuntimeError, match='^LOGGING_INCOMPLETE$') as captured:
        gathering.main(root, logging_context=event_log.context)
    assert captured.value.args == ('LOGGING_INCOMPLETE',)
    assert captured.value.__cause__ is None and captured.value.__suppress_context__
    assert calls == []


def test_no_context_has_no_logger_calls(gathering, catalogue, snapshot, fixture_root,
                                      writers, monkeypatch):
    import builtins
    _, root = fixture_root
    original = builtins.__import__
    imports, queries = [], []

    def importing(name, *args, **kwargs):
        if name == 'run_logging':
            imports.append(name)
            raise AssertionError('no-context imported logger')
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', importing)
    monkeypatch.setattr(gathering, 'moex_query', lambda *args: queries.append(args) or gathering.pd.DataFrame())
    gathering.full_reload(catalogue, 24, 10, 'fixture', 'часа', root, snapshot)
    assert imports == [] and len(queries) == 2 and writers == []


def test_snapshot_parameters_remain_required(gathering):
    assert inspect.signature(gathering.data_update).parameters['tickers_dates'].default is inspect.Parameter.empty
    assert inspect.signature(gathering.full_reload).parameters['tickers_dates'].default is inspect.Parameter.empty
    for function in (gathering.main, gathering.moex_tickerlists, gathering.full_reload, gathering.data_update):
        parameter = inspect.signature(function).parameters['logging_context']
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY and parameter.default is None


def test_real_reload_thirty_year_cutoff(gathering, catalogue, snapshot, candle_frame,
                                       fixture_root, writers, monkeypatch):
    import pandas as pd
    _, root = fixture_root
    dates = snapshot.copy(deep=True)
    dates.loc[1, 'issue_date'] = pd.Timestamp('1980-01-01')
    calls = []
    monkeypatch.setattr(gathering, 'moex_query', lambda *args: calls.append(args) or candle_frame.iloc[:0].copy())
    gathering.full_reload(catalogue, 24, 30, 'fixture', 'часа', root, dates)
    assert calls == [('MOEX', 'Акции', '2020-02-29', '2024-03-01', 24),
                     ('SBER', 'Акции', '1994-03-10', '2024-03-02', 24)]
    assert writers == []


def test_context_and_snapshot_forward_once(gathering, catalogue, snapshot, dataset_config,
                                          fixture_root, exact_reads, writers, monkeypatch,
                                          event_log, capsys):
    _, root = fixture_root
    builds, producers, reloads = [], [], []
    monkeypatch.setattr(gathering, 'config', dataset_config)
    monkeypatch.setattr(gathering, 'moex_tickerlists',
                        lambda *args, **kwargs: producers.append((args, kwargs)) or catalogue)
    monkeypatch.setattr(gathering, 'build_tickers_dates', lambda *args: builds.append(args) or snapshot)
    monkeypatch.setattr(gathering, 'full_reload', lambda *args, **kwargs: reloads.append((args, kwargs)))
    gathering.main(root, logging_context=event_log.context)
    assert len(builds) == 1 and len(producers) == 1 and len(reloads) == 8
    assert producers[0] == ((root,), {'logging_context': event_log.context})
    assert all(args[6] is snapshot and kwargs['logging_context'] is event_log.context
               for args, kwargs in reloads)
    assert exact_reads.calls[0][0].name == 'moex_full.csv'
    assert len(exact_reads.calls) == 1 and writers == []
    output = capsys.readouterr()
    assert root not in output.out and output.err == ''
    assert [item['event'] for item in event_log.events].count('a1_metadata_returned') == 1
    assert len([item for item in event_log.events if item['event'] == 'a1_cold_start']) == 8
    assert event_log.events[-1]['event'] == 'a1_main_returned'


def test_stable_three_ticker_delta_and_30day_boundary(gathering, snapshot, candle_frame,
                                                    fixture_root, exact_reads, writers, monkeypatch, save_dataset):
    import pandas as pd
    root_path, root = fixture_root
    source = pd.DataFrame({'TRADE_CODE': [' MOEX ', '', None, 'SBER', 'MOEX', 'BOND1'],
                           'SUPERTYPE': ['Акции'] * 5 + ['Облигации']}, index=[2, 8, 11, 14, 18, 21])
    exact_reads.catalogue = source
    frame = candle_frame.copy(deep=True)
    frame.loc[0, 'end'] = '2024-02-01 12:00:00'  # Inclusive fixed 30-day cutoff.
    target = root_path / 'datasets/fixture.csv'
    target.parent.mkdir(parents=True)
    save_dataset(target, frame)
    exact_reads.declared[target] = frame
    queries = []
    monkeypatch.setattr(gathering, 'moex_query', lambda *args: queries.append(args) or frame.iloc[:0].copy())
    gathering.data_update([{'interval': 60, 'years': 30, 'filename': 'fixture', 'word': 'минут'}],
                          root, source, snapshot)
    assert queries == [('SBER', 'Акции', '2024-02-01', '2024-03-02', 60),
                       ('MOEX', 'Акции', '2023-03-03', '2024-03-02', 60),
                       ('BOND1', 'Облигации', '2023-03-03', '2024-03-02', 60)]
    assert len(writers) == 2
    assert all(item[1].values.tolist() == [[100., 101., 102., 99., 202000., 2000,
               '2024-03-01 00:00:00', '2024-02-01 12:00:00', 'SBER']] for item in writers)


def test_clean_old_ticker_has_no_hidden_annual_backfill(gathering, catalogue, snapshot,
                                                      candle_frame, fixture_root,
                                                      exact_reads, writers, monkeypatch, save_dataset):
    root_path, root = fixture_root
    target = root_path / 'datasets/fixture.csv'
    target.parent.mkdir(parents=True)
    frame = candle_frame.copy(deep=True)
    frame.loc[0, 'end'] = '2024-02-01 11:59:59'
    save_dataset(target, frame)
    exact_reads.declared[target] = frame
    queries = []
    monkeypatch.setattr(gathering, 'moex_query', lambda *args: queries.append(args) or frame.iloc[:0].copy())
    gathering.data_update([{'interval': 24, 'years': 10, 'filename': 'fixture', 'word': 'часа'}],
                          root, catalogue, snapshot)
    assert queries == [('MOEX', 'Акции', '2023-03-03', '2024-03-02', 24)]
    assert len(writers) == 2 and all(item[1].ticker.tolist() == ['SBER'] for item in writers)


@pytest.mark.parametrize('secondary', [False, True])
def test_query_primary_stops_reload_before_next_call(gathering, catalogue, snapshot,
                                                    fixture_root, writers, monkeypatch,
                                                    event_log, capsys, secondary):
    _, root = fixture_root
    primary = OSError('A1_SYNTHETIC_CANARY')
    queries = []
    original_emit = event_log.logger.emit_event

    def emit(context, level, *args):
        if level == 'ERROR' and secondary:
            raise OSError('secondary')
        return original_emit(context, level, *args)

    def query(*args):
        queries.append(args)
        raise primary

    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    monkeypatch.setattr(gathering, 'moex_query', query)
    with pytest.raises(OSError) as captured:
        gathering.full_reload(catalogue, 24, 10, 'fixture', 'часа', root, snapshot,
                              logging_context=event_log.context)
    assert captured.value is primary
    assert queries == [('MOEX', 'Акции', '2020-02-29', '2024-03-01', 24)]
    assert writers == [] and not any(item['outcome'] == 'RETURNED' for item in event_log.events)
    if secondary:
        assert primary.__notes__ == ['A1_LOGGING_FAILURE: LOGGING_INCOMPLETE']
        assert capsys.readouterr().err == 'A1_LOGGING_FAILURE: LOGGING_INCOMPLETE\n'
    else:
        event = event_log.events[-1]
        assert (event['dataset_id'], event['interval'], event['instrument'], event['file']) == (
            'fixture', 24, 'MOEX', 'fixture.csv')
        assert event['duration_ms'] >= 0 and event['error']['code'] == 'APPLICATION_ERROR'
        assert not getattr(primary, '__notes__', []) and capsys.readouterr().err == ''


@pytest.mark.parametrize('failed_writer', ['xlsx', 'csv'])
def test_metadata_failure_does_not_guess_artifact(gathering, catalogue, fixture_root,
                                                monkeypatch, event_log, capsys, failed_writer):
    _, root = fixture_root
    primary = OSError({'path': r'C:\private\WIN_PATH_CANARY\metadata',
                       'secret': 'A1_SYNTHETIC_CANARY'})
    original_args = primary.args
    downstream, writes, requests = [], [], []
    monkeypatch.setattr(gathering, 'moex_tickerlists', lambda *args, **kwargs: catalogue)

    def get(*args, **kwargs):
        requests.append((args, kwargs))
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {})

    def write(kind):
        def writer(frame, path, *args, **kwargs):
            assert path.endswith('/ticker_lists/tickers_dates.' + kind)
            writes.append(kind)
            if kind == failed_writer:
                raise primary
        return writer

    # Real unchanged builder and its XLSX -> CSV sequence; HTTP/writers offline.
    monkeypatch.setattr(gathering.requests, 'Session', lambda: SimpleNamespace(get=get))
    monkeypatch.setattr(gathering.pd.DataFrame, 'to_excel', write('xlsx'))
    monkeypatch.setattr(gathering.pd.DataFrame, 'to_csv', write('csv'))
    monkeypatch.setattr(gathering, 'data_update', lambda *args, **kwargs: downstream.append(args))
    monkeypatch.setattr(gathering, 'full_reload', lambda *args, **kwargs: downstream.append(args))
    monkeypatch.setattr(gathering, 'moex_query', lambda *args, **kwargs: downstream.append(args))
    with pytest.raises(OSError) as captured:
        gathering.main(root, logging_context=event_log.context)
    assert captured.value is primary and primary.args == original_args and downstream == []
    assert writes == (['xlsx'] if failed_writer == 'xlsx' else ['xlsx', 'csv'])
    assert len(requests) == 4 and all(kwargs['timeout'] == 5 for _, kwargs in requests)
    event = event_log.events[-1]
    assert event['level'] == 'ERROR' and event['outcome'] == 'FAILED'
    assert not getattr(primary, '__notes__', []) and capsys.readouterr().err == ''
    serialized = json.dumps(event_log.events, ensure_ascii=False)
    assert all(value not in serialized for value in ('A1_SYNTHETIC_CANARY', 'WIN_PATH_CANARY', root))
    assert event['error']['file'] is None
    assert event['file'] is None
    assert event['null_reasons']['file'] == 'NOT_AVAILABLE_OR_NOT_APPLICABLE'


@pytest.mark.parametrize('failure', ['read', 'prepare'])
@pytest.mark.parametrize('delivery', ['ok', 'unconfirmed', 'unhealthy'])
def test_lookup_failure_identifies_catalogue_before_domain(gathering, catalogue, snapshot,
                                                         fixture_root, monkeypatch, event_log,
                                                         writers, capsys, failure, delivery):
    import pandas as pd
    _, root = fixture_root
    primary = OSError({'path': r'C:\private\WIN_PATH_CANARY\lookup',
                       'secret': 'A1_SYNTHETIC_CANARY'})
    original_args = primary.args
    reads, downstream, prepared_errors = [], [], []
    bad_lookup = pd.DataFrame({'TRADE_CODE': [123], 'SUPERTYPE': ['Акции']})
    original_prepare = gathering._prepare_ticker_catalogue
    original_emit = event_log.logger.emit_event
    original_isfile = gathering.os.path.isfile

    def read(path, *args, **kwargs):
        reads.append((path, args, kwargs))
        assert path == root + '/datasets/ticker_lists/moex_full.csv'
        assert args == () and kwargs == {'index_col': 0}
        if failure == 'read':
            raise primary
        return bad_lookup

    def prepare(source):
        try:
            return original_prepare(source)
        except ValueError as error:
            prepared_errors.append((error, error.args))
            raise

    def emit(context, level, *args):
        if level == 'ERROR':
            event_log.confirmed = delivery != 'unconfirmed'
            event_log.healthy = delivery != 'unhealthy'
        return original_emit(context, level, *args)

    def isfile(path):
        if path == root + 'datasets/fixture.csv':
            downstream.append(path)
        return original_isfile(path)

    monkeypatch.setattr(gathering.pd, 'read_csv', read)
    monkeypatch.setattr(gathering, '_prepare_ticker_catalogue', prepare)
    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    monkeypatch.setattr(gathering.os.path, 'isfile', isfile)
    monkeypatch.setattr(gathering, 'full_reload', lambda *args, **kwargs: downstream.append(args))
    monkeypatch.setattr(gathering, 'moex_query', lambda *args, **kwargs: downstream.append(args))
    with pytest.raises(OSError if failure == 'read' else ValueError) as captured:
        gathering.data_update([{'filename': 'fixture', 'interval': 24, 'years': 10, 'word': 'часа'}],
                              root, catalogue, snapshot, logging_context=event_log.context)
    if failure == 'prepare':
        primary, original_args = prepared_errors[0]
        assert original_args == ('A1_TICKER_CATALOGUE_STRUCTURE',)
    assert captured.value is primary and primary.args == original_args
    assert len(reads) == 1 and downstream == [] and writers == []
    event = event_log.events[-1]
    assert event['level'] == 'ERROR' and event['outcome'] == 'FAILED'
    assert not any(item['outcome'] == 'RETURNED' for item in event_log.events)
    output = capsys.readouterr()
    if delivery == 'ok':
        assert not getattr(primary, '__notes__', []) and output.err == ''
    else:
        assert primary.__notes__ == ['A1_LOGGING_FAILURE: LOGGING_INCOMPLETE']
        assert output.err == 'A1_LOGGING_FAILURE: LOGGING_INCOMPLETE\n'
    serialized = json.dumps(event_log.events, ensure_ascii=False) + output.out + output.err
    assert all(value not in serialized for value in ('A1_SYNTHETIC_CANARY', 'WIN_PATH_CANARY', root))
    assert event['file'] == 'ticker_lists/moex_full.csv'


def test_force_reload_reason_event(gathering, catalogue, snapshot, dataset_config,
                                   fixture_root, monkeypatch, event_log):
    _, root = fixture_root
    monkeypatch.setattr(gathering, 'config', dataset_config)
    monkeypatch.setattr(gathering, 'moex_tickerlists', lambda *args, **kwargs: catalogue)
    monkeypatch.setattr(gathering, 'build_tickers_dates', lambda *args: snapshot)
    calls = []
    monkeypatch.setattr(gathering, 'full_reload', lambda *args, **kwargs: calls.append((args, kwargs)))
    gathering.main(root, force_reload=True, logging_context=event_log.context)
    reasons = [item for item in event_log.events if item['event'] == 'a1_force_reload']
    assert len(reasons) == 1 and reasons[0]['outcome'] == 'FORCE_RELOAD'
    assert not any(item['event'] == 'a1_cold_start' for item in event_log.events)
    assert len(calls) == 8 and all(args[6] is snapshot for args, _ in calls)
