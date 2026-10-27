"""A1 fixtures under the existing F3 boundary; no separate guard or bootstrap."""

import datetime
import csv
import io
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def gathering(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT))
    import data_gathering

    class FixedClock(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2024, 3, 2, 12, 0, 0, tzinfo=tz)

    monkeypatch.setattr(data_gathering, 'datetime',
                        SimpleNamespace(datetime=FixedClock, timedelta=datetime.timedelta))
    monkeypatch.setattr(data_gathering, 'exception_list', [])
    return data_gathering


@pytest.fixture
def catalogue():
    import pandas as pd
    return pd.DataFrame({'TRADE_CODE': ['MOEX', 'SBER'],
                         'SUPERTYPE': ['Акции', 'Акции']})


@pytest.fixture
def snapshot():
    import pandas as pd
    return pd.DataFrame({'TRADE_CODE': ['MOEX', 'SBER'],
                         'issue_date': pd.to_datetime(['2020-02-29', '2000-01-03']),
                         'stopped_date': pd.to_datetime(['2024-03-01', None])})


@pytest.fixture
def dataset_config():
    # Literal eight-name inventory, independent of production config/helper.
    return [
        {'interval': 24, 'years': 10, 'filename': '10years_data_1d_interval', 'word': 'часа'},
        {'interval': 60, 'years': 10, 'filename': '10years_data_1h_interval', 'word': 'минут'},
        {'interval': 10, 'years': 10, 'filename': '10years_data_10m_interval', 'word': 'минут'},
        {'interval': 1, 'years': 10, 'filename': '10years_data_1m_interval', 'word': 'минута'},
        {'interval': 24, 'years': 30, 'filename': '30years_data_1d_interval', 'word': 'часа'},
        {'interval': 60, 'years': 30, 'filename': '30years_data_1h_interval', 'word': 'минут'},
        {'interval': 10, 'years': 30, 'filename': '30years_data_10m_interval', 'word': 'минут'},
        {'interval': 1, 'years': 30, 'filename': '30years_data_1m_interval', 'word': 'минута'},
    ]


@pytest.fixture
def candle_frame():
    import pandas as pd
    return pd.DataFrame([
        [100.0, 101.0, 102.0, 99.0, 202000.0, 2000,
         '2024-03-01 00:00:00', '2024-03-01 23:59:59', 'SBER'],
    ], columns=['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker'])


@pytest.fixture
def writers(monkeypatch):
    import pandas as pd
    records = []

    def record(kind):
        def write(frame, path, *args, **kwargs):
            records.append((kind, frame.copy(deep=True), str(path), args, dict(kwargs)))
        return write

    monkeypatch.setattr(pd.DataFrame, 'to_csv', record('csv'))
    monkeypatch.setattr(pd.DataFrame, 'to_excel', record('xlsx'))
    return records


@pytest.fixture
def fixture_root(tmp_path):
    # Trailing separator isolates the known #4 path defect (owned by F2-02).
    return tmp_path, str(tmp_path) + '/'


@pytest.fixture
def save_dataset():
    # Literal fixture frames become real CSV bytes without production writers.
    def save(path, frame):
        stream = io.StringIO(newline='')
        writer = csv.writer(stream, lineterminator='\n')
        writer.writerow(frame.columns.tolist())
        writer.writerows(frame.values.tolist())
        content = stream.getvalue().encode('utf-8')
        path.write_bytes(content)
        return content
    return save


@pytest.fixture
def exact_reads(monkeypatch, gathering, fixture_root, catalogue):
    root, _ = fixture_root
    declared = {}
    calls = []
    original = gathering.pd.read_csv
    catalogue_path = root / 'datasets/ticker_lists/moex_full.csv'
    state = SimpleNamespace(declared=declared, calls=calls, catalogue=catalogue.copy(deep=True))

    def read(path, *args, **kwargs):
        target = Path(path)
        calls.append((target, args, dict(kwargs)))
        if target == catalogue_path:
            assert args == () and kwargs == {'index_col': 0}
            return state.catalogue.copy(deep=True)
        if target in declared:
            assert args == () and kwargs == {}
            return original(path, *args, **kwargs)
        pytest.fail('Unexpected data read outside declared fixture boundary')

    monkeypatch.setattr(gathering.pd, 'read_csv', read)
    return state


@pytest.fixture
def event_log(monkeypatch):
    # Exercise the real pure envelope/redactor, with transport/health recording
    # doubles. No session, queue, thread, file handler or private live context.
    import run_logging
    context = {'_secrets': ('A1_SYNTHETIC_CANARY',), '_checkout': ROOT,
               'config': run_logging.validate_logging_config(), 'run_id': 'a1-fixture',
               'worker_id': 'parent', 'producer_sha': 'fixture-sha',
               'package': 'A'}
    events = []
    state = SimpleNamespace(context=context, events=events, fail=False, healthy=True,
                            accepted=True, confirmed=True, logger=run_logging)

    def emit(context, level, event, message, fields):
        if state.fail:
            raise OSError('A1_SYNTHETIC_CANARY')
        events.append(run_logging._event(context, level, event, message, fields))
        return {'accepted': state.accepted,
                'confirmed': state.confirmed if level == 'ERROR' else False}

    monkeypatch.setattr(run_logging, 'emit_event', emit)
    monkeypatch.setattr(run_logging, 'check_logging_health', lambda context: {'healthy': state.healthy})
    return state
