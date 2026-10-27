"""Catalogue exports and A1 diagnostics through real domain entrypoints."""

import csv
import io
import json
from types import SimpleNamespace

import pytest


def http_bytes(monkeypatch, gathering, content=None, error=None):
    calls = []

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, *args, **kwargs):
            calls.append((args, kwargs))
            if error is not None:
                raise error
            return SimpleNamespace(content=content)

    monkeypatch.setattr(gathering.requests, 'Session', Session)
    return calls


def encoded(rows):
    stream = io.StringIO(newline='')
    csv.writer(stream).writerows(rows)
    return stream.getvalue().encode('cp1251')


def test_processing_copy_and_literal_text(gathering):
    import pandas as pd
    source = pd.DataFrame({'TRADE_CODE': [' MOEX ', '', None, float('nan'), pd.NA,
                                        'SBER', 'MOEX', 'NaN', 'nan', 'None', 'sber'],
                           'SUPERTYPE': ['Акции'] * 11, 'NAME': list('abcdefghijk')},
                          index=[3, 7, 8, 9, 12, 15, 18, 22, 23, 26, 31])
    source.index.name = 'raw_index'
    before = source.copy(deep=True)
    result = gathering._prepare_ticker_catalogue(source)
    assert result.columns.tolist() == ['TRADE_CODE', 'SUPERTYPE', 'NAME']
    assert result.values.tolist() == [
        ['MOEX', 'Акции', 'a'], ['SBER', 'Акции', 'f'], ['NaN', 'Акции', 'h'],
        ['nan', 'Акции', 'i'], ['None', 'Акции', 'j'], ['sber', 'Акции', 'k']]
    assert result.index.tolist() == list(range(6)) and result.index.name is None
    pd.testing.assert_frame_equal(source, before)
    result.loc[0, 'NAME'] = 'changed copy'
    pd.testing.assert_frame_equal(source, before)


@pytest.mark.parametrize('value', [123, 0.0, True, ['SBER'], {'ticker': 'SBER'}, ('SBER',)])
def test_non_null_non_string_is_structure_error(gathering, value):
    import pandas as pd
    source = pd.DataFrame({'TRADE_CODE': ['SBER', value], 'SUPERTYPE': ['Акции'] * 2})
    with pytest.raises(ValueError, match='^A1_TICKER_CATALOGUE_STRUCTURE$'):
        gathering._prepare_ticker_catalogue(source)


@pytest.mark.parametrize('case', ['empty', 'blank', 'null', 'missing', 'duplicate'])
def test_helper_empty_structure(gathering, case):
    import pandas as pd
    sources = {
        'empty': pd.DataFrame(columns=['TRADE_CODE', 'SUPERTYPE']),
        'blank': pd.DataFrame({'TRADE_CODE': ['', '  '], 'SUPERTYPE': ['Акции'] * 2}),
        'null': pd.DataFrame({'TRADE_CODE': [None, pd.NA], 'SUPERTYPE': ['Акции'] * 2}),
        'missing': pd.DataFrame({'TRADE_CODE': ['SBER']}),
        'duplicate': pd.DataFrame([['SBER', 'SBER', 'Акции']],
                                  columns=['TRADE_CODE', 'TRADE_CODE', 'SUPERTYPE']),
    }
    reason = 'A1_EMPTY_TICKER_CATALOGUE' if case in ('empty', 'blank', 'null') else 'A1_TICKER_CATALOGUE_STRUCTURE'
    with pytest.raises(ValueError, match='^' + reason + '$'):
        gathering._prepare_ticker_catalogue(sources[case])


@pytest.mark.parametrize('routes', [['Акции', 'Облигации'], ['Акции', None], ['Акции', ''],
                                  ['Акции', 'Акции', 'Облигации']])
def test_new_alias_group_checks_all_routes(gathering, routes):
    import pandas as pd
    codes = ['SBER', ' SBER ', 'SBER'][:len(routes)]
    source = pd.DataFrame({'TRADE_CODE': codes, 'SUPERTYPE': routes})
    with pytest.raises(ValueError, match='^A1_AMBIGUOUS_TICKER_ROUTING$'):
        gathering._prepare_ticker_catalogue(source)


@pytest.mark.parametrize('name,values', [('ISIN', ['RU1', 'RU2']), ('INSTRUMENT_ID', [101, 102])])
def test_new_alias_conflicting_identity(gathering, name, values):
    import pandas as pd
    source = pd.DataFrame({'TRADE_CODE': ['SBER', ' SBER '], 'SUPERTYPE': ['Акции'] * 2,
                           name: values})
    with pytest.raises(ValueError, match='^A1_AMBIGUOUS_TICKER_IDENTITY$'):
        gathering._prepare_ticker_catalogue(source)


def test_alias_non_identity_columns_and_missing_hints_are_not_new_validation(gathering):
    import pandas as pd
    source = pd.DataFrame({'TRADE_CODE': [' SBER ', 'SBER'], 'SUPERTYPE': ['Акции'] * 2,
                           'ISIN': [None, 'RU1'], 'INSTRUMENT_ID': [101, 101],
                           'NAME': ['Old', 'New'], 'CURRENCY': ['RUB', '']})
    before = source.copy(deep=True)
    result = gathering._prepare_ticker_catalogue(source)
    assert result.values.tolist() == [['SBER', 'Акции', None, 101, 'Old', 'RUB']]
    pd.testing.assert_frame_equal(source, before)


def test_exact_raw_duplicate_keeps_legacy_first(gathering):
    import pandas as pd
    source = pd.DataFrame({'TRADE_CODE': ['SBER', 'SBER'],
                           'SUPERTYPE': ['Акции', 'Облигации'], 'ISIN': ['RU1', 'RU2']})
    assert gathering._prepare_ticker_catalogue(source).values.tolist() == [['SBER', 'Акции', 'RU1']]


def test_real_http_parser_preserves_raw_exports(gathering, fixture_root, writers,
                                               monkeypatch, event_log):
    import pandas as pd
    _, root = fixture_root
    rows = [['TRADE_CODE', 'SUPERTYPE', 'ISIN'],
            [' MOEX ', 'Акции', 'RU1'], ['SBER', 'Акции', 'RU2'], ['MOEX', 'Акции', 'RU1'],
            ['', 'Акции', ''], ['NaN', 'Облигации', 'RU3'], ['nan', 'Облигации', 'RU4'],
            ['None', 'Облигации', 'RU5']]
    calls = http_bytes(monkeypatch, gathering, encoded(rows))
    result = gathering.moex_tickerlists(root, logging_context=event_log.context)
    assert calls == [(('https://www.moex.com/ru/listing/securities-list-csv.aspx?type=1',),
                      {'headers': gathering.header})]
    assert result.values.tolist() == [['MOEX', 'Акции', 'RU1'], ['SBER', 'Акции', 'RU2'],
                                     ['NaN', 'Облигации', 'RU3'], ['nan', 'Облигации', 'RU4'],
                                     ['None', 'Облигации', 'RU5']]
    assert result.loc[0, 'TRADE_CODE'] == 'MOEX'
    assert result.index.tolist() == [0, 1, 2, 3, 4]
    raw = pd.DataFrame(rows[1:], columns=rows[0], index=range(1, 8))
    raw.columns.name = 0
    assert [item[0] for item in writers] == ['xlsx', 'csv', 'csv']
    for (_, frame, path, args, kwargs), expected, name in zip(
            writers, [raw, raw, raw.iloc[:4].reset_index(drop=True)],
            ['moex_full.xlsx', 'moex_full.csv', 'moex_stocks.csv']):
        pd.testing.assert_frame_equal(frame, expected)
        assert path.endswith('/datasets/ticker_lists/' + name)
        assert args == () and kwargs == {}  # Preserve legacy index export defaults.
    prepared = next(item for item in event_log.events if item['event'] == 'a1_catalogue_prepared')
    assert prepared['counts'] == dict(rows_received=7, rows_accepted=5, rows_quarantined=None,
                                     pages=None, instruments=5,
                                     null_reasons={'rows_quarantined': 'NOT_ASSESSED', 'pages': 'NOT_MEASURED'})
    returned = event_log.events[-1]
    assert returned['event'] == 'a1_catalogue_returned' and returned['outcome'] == 'RETURNED'
    assert returned['file'] == 'ticker_lists/moex_full.csv' and returned['duration_ms'] >= 0
    assert returned['instrument'] is None and 'instrument' in returned['null_reasons']
    assert all(item['level'] == 'INFO' for item in event_log.events)


@pytest.mark.parametrize('rows,reason', [
    ([], 'A1_EMPTY_TICKER_CATALOGUE'),
    ([['TRADE_CODE', 'SUPERTYPE']], 'A1_EMPTY_TICKER_CATALOGUE'),
    ([['TRADE_CODE', 'SUPERTYPE'], ['', 'Акции'], [' ', 'Акции']], 'A1_EMPTY_TICKER_CATALOGUE'),
    ([['TRADE_CODE'], ['SBER']], 'A1_TICKER_CATALOGUE_STRUCTURE'),
    ([['TRADE_CODE', 'TRADE_CODE', 'SUPERTYPE'], ['SBER', 'SBER', 'Акции']], 'A1_TICKER_CATALOGUE_STRUCTURE'),
    ([['TRADE_CODE', 'SUPERTYPE'], ['SBER', 'Акции'], [' SBER ', 'Облигации']], 'A1_AMBIGUOUS_TICKER_ROUTING'),
    ([['TRADE_CODE', 'SUPERTYPE', 'ISIN'], ['SBER', 'Акции', 'RU1'], [' SBER ', 'Акции', 'RU2']], 'A1_AMBIGUOUS_TICKER_IDENTITY'),
])
def test_source_guard_before_first_writer_and_metadata(gathering, fixture_root, writers,
                                                       monkeypatch, rows, reason):
    _, root = fixture_root
    http_bytes(monkeypatch, gathering, encoded(rows))
    downstream = []
    monkeypatch.setattr(gathering, 'build_tickers_dates', lambda *args: downstream.append(args))
    with pytest.raises(ValueError, match='^' + reason + '$'):
        gathering.main(root)
    assert writers == [] and downstream == []


def test_decode_failure_before_writer(gathering, fixture_root, writers, monkeypatch):
    _, root = fixture_root
    http_bytes(monkeypatch, gathering, b'\x98')  # Undefined CP1251 byte; real decode.
    with pytest.raises(UnicodeDecodeError):
        gathering.moex_tickerlists(root)
    assert writers == []


@pytest.mark.parametrize('secondary', ['emit', 'health', 'unconfirmed', 'rejected'])
def test_primary_source_exception_survives_diagnostics_failure(gathering, fixture_root,
                                                              writers, monkeypatch,
                                                              event_log, capsys, secondary):
    _, root = fixture_root
    primary = ValueError({'nested': ['A1_SYNTHETIC_CANARY', 'https://user:pass@moex.com/x?secret=q#frag']})
    calls = http_bytes(monkeypatch, gathering, error=primary)
    original_emit = event_log.logger.emit_event
    original_health = event_log.logger.check_logging_health

    def emit(context, level, *args):
        if level == 'ERROR':
            if secondary == 'emit':
                raise OSError('A1_SYNTHETIC_CANARY')
            if secondary == 'unconfirmed':
                event_log.confirmed = False
            if secondary == 'rejected':
                event_log.accepted = False
        return original_emit(context, level, *args)

    def health(context):
        if secondary == 'health' and event_log.events[-1]['level'] == 'ERROR':
            raise OSError('A1_SYNTHETIC_CANARY')
        return original_health(context)

    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    monkeypatch.setattr(event_log.logger, 'check_logging_health', health)
    with pytest.raises(ValueError) as captured:
        gathering.moex_tickerlists(root, logging_context=event_log.context)
    assert captured.value is primary and captured.value.args == primary.args
    assert primary.__notes__ == ['A1_LOGGING_FAILURE: LOGGING_INCOMPLETE']
    frames, trace = [], primary.__traceback__
    while trace:
        frames.append(trace.tb_frame.f_code.co_name)
        trace = trace.tb_next
    assert frames[-1] == 'get' and '_a1_event' not in frames
    assert len(calls) == 1 and writers == []
    assert not any(item['outcome'] == 'RETURNED' for item in event_log.events)
    output = capsys.readouterr()
    assert output.err == 'A1_LOGGING_FAILURE: LOGGING_INCOMPLETE\n' and output.out == ''


@pytest.mark.parametrize('broken_fallback', ['stderr', 'note'])
def test_diagnostics_fallbacks_are_independently_protected(gathering, fixture_root,
                                                         monkeypatch, event_log,
                                                         capsys, broken_fallback):
    _, root = fixture_root

    class UnassignableNote(ValueError):
        def __setattr__(self, name, value):
            if name == '__notes__':
                raise OSError('note unavailable')
            super().__setattr__(name, value)

    primary = UnassignableNote('primary') if broken_fallback == 'note' else ValueError('primary')
    original_emit = event_log.logger.emit_event

    def emit(context, level, *args):
        if level == 'ERROR':
            raise OSError('secondary')
        return original_emit(context, level, *args)

    monkeypatch.setattr(event_log.logger, 'emit_event', emit)
    http_bytes(monkeypatch, gathering, error=primary)
    if broken_fallback == 'stderr':
        class BrokenStderr:
            def write(self, text):
                raise OSError('stderr unavailable')
        monkeypatch.setattr(gathering.sys, 'stderr', BrokenStderr())
    with pytest.raises(ValueError) as captured:
        gathering.moex_tickerlists(root, logging_context=event_log.context)
    assert captured.value is primary
    if broken_fallback == 'stderr':
        assert primary.__notes__ == ['A1_LOGGING_FAILURE: LOGGING_INCOMPLETE']
    else:
        assert capsys.readouterr().err == 'A1_LOGGING_FAILURE: LOGGING_INCOMPLETE\n'


def test_primary_exception_event_redacts_nested_canaries(gathering, fixture_root,
                                                        writers, monkeypatch, event_log, capsys):
    _, root = fixture_root
    tokens = ['WIN_PATH_CANARY', 'UNC_PATH_CANARY', 'POSIX_PATH_CANARY',
              'URL_USER_CANARY', 'URL_PASS_CANARY', 'URL_QUERY_CANARY', 'URL_FRAGMENT_CANARY',
              'A1_SYNTHETIC_CANARY']
    primary = ValueError({'nested': [r'C:\private\WIN_PATH_CANARY\data.csv',
                                    r'\\server\private\UNC_PATH_CANARY\data.csv',
                                    '/private/POSIX_PATH_CANARY/data.csv',
                                    'https://URL_USER_CANARY:URL_PASS_CANARY@moex.com/x?token=URL_QUERY_CANARY#URL_FRAGMENT_CANARY',
                                    {'password': 'A1_SYNTHETIC_CANARY'}]})
    http_bytes(monkeypatch, gathering, error=primary)
    with pytest.raises(ValueError) as captured:
        gathering.moex_tickerlists(root, logging_context=event_log.context)
    assert captured.value is primary and not getattr(primary, '__notes__', [])
    error_event = event_log.events[-1]
    assert error_event['level'] == 'ERROR' and error_event['outcome'] == 'FAILED'
    assert error_event['error']['code'] == 'APPLICATION_ERROR'
    serialized = json.dumps(event_log.events, ensure_ascii=False)
    assert all(token not in serialized for token in tokens)
    assert '<external>' in serialized and 'https://moex.com/x' in serialized
    assert str(event_log.context['_checkout']) not in serialized
    assert str(root) not in serialized
    assert writers == [] and capsys.readouterr().err == ''


def test_field_paths_urls_use_existing_redactor(gathering, event_log):
    gathering._a1_event(event_log.context, 'INFO', 'a1_fixture',
                        dict(file=r'C:\private\FIELD_PATH_CANARY\dataset.csv',
                             instrument='https://USER_CANARY:PASS_CANARY@moex.com/x?q=QUERY_CANARY#FRAGMENT_CANARY'))
    event = event_log.events[-1]
    assert event['file'] == '<external>' and event['instrument'] == 'https://moex.com/x'
    assert event['stage'] == 'data_gathering' and event['dataset_id'] is None
    assert 'dataset_id' in event['null_reasons']


def test_unusable_primary_notes_do_not_replace_primary(gathering, fixture_root,
                                                      monkeypatch, event_log, writers, capsys):
    _, root = fixture_root
    primary = ValueError('primary')
    primary.__notes__ = None
    http_bytes(monkeypatch, gathering, error=primary)
    with pytest.raises(ValueError) as captured:
        gathering.moex_tickerlists(root, logging_context=event_log.context)
    assert captured.value is primary and primary.__notes__ is None
    assert event_log.events[-1]['outcome'] == 'FAILED' and writers == []
    assert capsys.readouterr().err == ''


@pytest.mark.parametrize('failed_name', ['moex_full.xlsx', 'moex_full.csv', 'moex_stocks.csv'])
def test_source_writer_failure_identifies_actual_file(gathering, fixture_root,
                                                     monkeypatch, event_log, failed_name):
    _, root = fixture_root
    http_bytes(monkeypatch, gathering, encoded([['TRADE_CODE', 'SUPERTYPE'], ['SBER', 'Акции']]))
    primary, written = OSError('writer primary'), []

    def write(frame, path, *args, **kwargs):
        name = path.replace('\\', '/').split('/')[-1]
        written.append(name)
        if name == failed_name:
            raise primary

    monkeypatch.setattr(gathering.pd.DataFrame, 'to_excel', write)
    monkeypatch.setattr(gathering.pd.DataFrame, 'to_csv', write)
    with pytest.raises(OSError) as captured:
        gathering.moex_tickerlists(root, logging_context=event_log.context)
    assert captured.value is primary
    assert written == ['moex_full.xlsx', 'moex_full.csv', 'moex_stocks.csv'][:
        ['moex_full.xlsx', 'moex_full.csv', 'moex_stocks.csv'].index(failed_name) + 1]
    assert event_log.events[-1]['file'] == 'ticker_lists/' + failed_name
    assert event_log.events[-1]['outcome'] == 'FAILED'
