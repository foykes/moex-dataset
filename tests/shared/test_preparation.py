"""Literal oracles for existing preparation functions; all IO is offline."""

import hashlib
import importlib.util
import io
from io import StringIO
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest
import mds_offline_guard as guard


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / 'tests/fixtures/shared'


def _hashes():
    return {name: hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest()
            for name in ('candles.csv', 'ticker_dates.json')}


def _forbidden_writer(*args, **kwargs):
    raise AssertionError('Preparation tests forbid real dataset writers')


def _data_gathering(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT))
    import data_gathering
    monkeypatch.setattr(data_gathering, 'headers_full', {'chrome': ['AGENT_A'], 'edge': ['AGENT_B']})
    monkeypatch.setattr(data_gathering, 'header', {'User-Agent': 'AGENT_A'})
    return data_gathering


def test_header_rotation_preserves_fields_and_wraps(monkeypatch):
    module = _data_gathering(monkeypatch)
    original = {'User-Agent': 'AGENT_A', 'Accept': 'text/csv'}
    assert module.get_next_header(None) == {'User-Agent': 'AGENT_A'}
    assert module.get_next_header(original) == {'User-Agent': 'AGENT_B', 'Accept': 'text/csv'}
    assert original == {'User-Agent': 'AGENT_A', 'Accept': 'text/csv'}
    assert module.get_next_header({'User-Agent': 'AGENT_B'}) == {'User-Agent': 'AGENT_A'}
    assert module.get_next_header({'User-Agent': 'UNKNOWN'}) == {'User-Agent': 'AGENT_A'}


def test_candle_retry_503_then_200_has_literal_rows_and_keys(monkeypatch, tmp_path):
    module = _data_gathering(monkeypatch)
    before = _hashes()
    previous = tmp_path / 'previous.csv'
    previous.write_bytes(b'PREVIOUS_ARTIFACT\n')
    previous_hash = hashlib.sha256(previous.read_bytes()).hexdigest()
    content = (FIXTURES / 'candles.csv').read_text(encoding='utf-8')
    responses = iter((SimpleNamespace(status_code=503, text=''),
                      SimpleNamespace(status_code=200, text=content)))
    calls, sleeps = [], []

    def request(url, headers, timeout):
        calls.append((url, dict(headers), timeout))
        return next(responses)

    monkeypatch.setattr(module, 'requests', SimpleNamespace(get=request))
    monkeypatch.setattr(module, 'time', SimpleNamespace(sleep=sleeps.append))
    monkeypatch.setattr(module.pd.DataFrame, 'to_csv', _forbidden_writer)
    monkeypatch.setattr(module.pd.DataFrame, 'to_excel', _forbidden_writer)
    result = module.moex_query('FIXTURE_SHARE', 'Акции', '2024-02-29', '2024-03-01',
                               24, max_retries=2)
    url = ('http://iss.moex.com/iss/engines/stock/markets/shares/securities/'
           'FIXTURE_SHARE/candles.csv?from=2024-02-29&till=2024-03-01&interval=24')
    assert calls == [(url, {'User-Agent': 'AGENT_A'}, 30), (url, {'User-Agent': 'AGENT_B'}, 30)]
    assert sleeps == [3, 3]
    assert result.columns.tolist() == ['open', 'close', 'high', 'low', 'value', 'volume',
                                       'begin', 'end', 'ticker']
    assert result.values.tolist() == [
        [100.0, 101.0, 102.0, 99.0, 202000.0, 2000,
         '2024-02-29 00:00:00', '2024-02-29 23:59:59', 'FIXTURE_SHARE'],
        [101.0, 103.0, 104.0, 100.0, 309000.0, 3000,
         '2024-03-01 00:00:00', '2024-03-01 23:59:59', 'FIXTURE_SHARE'],
    ]
    assert [(ticker, 24, begin) for ticker, begin in zip(result['ticker'], result['begin'])] == [
        ('FIXTURE_SHARE', 24, '2024-02-29 00:00:00'),
        ('FIXTURE_SHARE', 24, '2024-03-01 00:00:00'),
    ]
    assert result.index.tolist() == [0, 1]
    assert _hashes() == before
    assert previous.read_bytes() == b'PREVIOUS_ARTIFACT\n'
    assert hashlib.sha256(previous.read_bytes()).hexdigest() == previous_hash


@pytest.mark.parametrize('bond_payload, expected_rows, expected_errors, expected_sleeps', [
    ('valid_bond', [['FIXTURE_SHARE', '2001-01-01', '2024-02-29'],
                    ['FIXTURE_BOND', '2019-01-01', '2024-03-01']], [], [10, 10]),
    ('malformed_bond', [['FIXTURE_SHARE', '2001-01-01', '2024-02-29']], ['FIXTURE_BOND'], [10]),
])
def test_ticker_dates_valid_and_malformed_structure(monkeypatch, bond_payload,
                                                   expected_rows, expected_errors, expected_sleeps):
    import pandas as pd
    before = _hashes()
    payloads = json.loads((FIXTURES / 'ticker_dates.json').read_text(encoding='utf-8'))
    specification = importlib.util.spec_from_file_location('f3_fixture_ticker_dates', ROOT / 'tests.py')
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    monkeypatch.setattr(module, 'header', {'User-Agent': ''})
    responses = iter((payloads['valid_share'], payloads[bond_payload]))
    calls, sleeps, config_reads = [], [], []

    def settings_open(path, mode='r', encoding=None):
        assert (path, mode, encoding) == ('settings/user_agents.json', 'r', 'utf-8')
        config_reads.append((path, mode, encoding))
        return StringIO('{"chrome": ["AGENT_A"]}')

    def request(url, headers):
        calls.append((url, dict(headers)))
        payload = next(responses)
        return SimpleNamespace(status_code=200, json=lambda: payload)

    monkeypatch.setattr(module, 'requests', SimpleNamespace(get=request))
    monkeypatch.setattr(module, 'time', SimpleNamespace(sleep=sleeps.append))
    monkeypatch.setattr(module, 'open', settings_open, raising=False)
    monkeypatch.setattr(pd.DataFrame, 'to_csv', _forbidden_writer)
    monkeypatch.setattr(pd.DataFrame, 'to_excel', _forbidden_writer)
    frame = pd.DataFrame({'SUPERTYPE': ['Акции', 'Облигации'],
                          'TRADE_CODE': ['FIXTURE_SHARE', 'FIXTURE_BOND']})
    result, errors = module.get_ticker_dates(frame)
    assert calls == [
        ('http://iss.moex.com/iss/history/engines/stock/markets/shares/securities/FIXTURE_SHARE/dates.json', {'User-Agent': 'AGENT_A'}),
        ('http://iss.moex.com/iss/history/engines/stock/markets/bonds/securities/FIXTURE_BOND/dates.json', {'User-Agent': 'AGENT_A'}),
    ]
    assert result.columns.tolist() == ['ticker', 'date_from', 'date_till']
    assert result.values.tolist() == expected_rows
    assert result['ticker'].is_unique
    assert errors == expected_errors
    assert sleeps == expected_sleeps
    assert config_reads == [('settings/user_agents.json', 'r', 'utf-8')]
    assert _hashes() == before


def _review_control(case, root, ticket):
    """Fixed pre-guard source probes; only synthetic issued fixture files."""
    cases = {'source-identity-ftp', 'source-identity-nested',
             'source-identity-alias', 'source-identity-normal'}
    assert case in cases and guard._CONTEXT is None
    temporary = guard.checked_path(ticket['roots'][0], root / '.f3/tmp')
    replica = temporary / 'source-replica'
    replica.mkdir(exist_ok=False)
    ordinary = replica / 'ordinary.py'
    ordinary.write_bytes(b'NORMAL_SOURCE = 1\n')
    (replica / '.gitignore').write_bytes(b'.f3/\n')
    git = shutil.which('git')
    assert git is not None
    environment = {name: value for name, value in os.environ.items()
                   if not name.startswith('GIT_')}
    environment.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1',
                       GIT_TERMINAL_PROMPT='0')
    for arguments in [
        ['init', '--initial-branch=codex/fixture'],
        ['config', '--local', 'user.name', 'F2 disposable fixture'],
        ['config', '--local', 'user.email', 'fixture@example.invalid'],
        ['config', '--local', 'core.autocrlf', 'false'],
        ['add', '--', '.'], ['commit', '-m', 'N0 P0 baseline'],
    ]:
        completed = subprocess.run([git, *arguments], cwd=replica, env=environment,
                                   capture_output=True, timeout=10)
        assert completed.returncode == 0

    secret = b'F3_CODE03_SYNTHETIC_SECRET_VALUE'
    target = ordinary
    if case == 'source-identity-ftp':
        target = replica / 'ftp_credentials.py'
        target.write_bytes(secret)
    elif case == 'source-identity-nested':
        target = replica / 'tools/secrets/canary.py'
        target.parent.mkdir(parents=True)
        target.write_bytes(secret)
    elif case == 'source-identity-alias':
        canary = temporary / 'ftp_credentials.py'
        canary.write_bytes(secret)
        target = replica / 'ordinary_alias.py'
        os.link(canary, target)

    open_calls, leaf_calls = [], []
    original_open = io.open

    class ObservedReader:
        def __init__(self, stream):
            self.stream = stream
        def __enter__(self):
            self.stream.__enter__()
            return self
        def __exit__(self, *arguments):
            return self.stream.__exit__(*arguments)
        def read(self, *arguments):
            leaf_calls.append(1)
            return self.stream.read(*arguments)

    def observe_open(path, *arguments, **keywords):
        mode = arguments[0] if arguments else keywords.get('mode', 'r')
        match = (not isinstance(path, int)
                 and os.path.abspath(os.fsdecode(path)) == str(target)
                 and not any(flag in mode for flag in 'wax+'))
        if match:
            open_calls.append(1)
        stream = original_open(path, *arguments, **keywords)
        return ObservedReader(stream) if match else stream

    io.open = observe_open
    result = {'case': case}
    try:
        if case == 'source-identity-normal':
            first = guard.source_identity(replica)
            ordinary.write_bytes(b'NORMAL_SOURCE = 2\n')
            changed = guard.source_identity(replica)
            ordinary.write_bytes(b'NORMAL_SOURCE = 1\n')
            restored = guard.source_identity(replica)
            extra = replica / 'tools/extra.py'
            extra.parent.mkdir()
            extra.write_bytes(b'EXTRA_SOURCE = 3\n')
            untracked = guard.source_identity(replica)
            assert first[1] == restored[1] == 'commit'
            assert changed[1] == untracked[1] == 'working-tree'
            assert first[0] == changed[0] == restored[0] == untracked[0]
            assert first[2] == restored[2]
            assert first[2] != changed[2] and first[2] != untracked[2]
            assert len(open_calls) == len(leaf_calls) == 4
            result.update(source_modes=[value[1] for value in
                                        (first, changed, restored, untracked)],
                          ordinary_content_hashed=True, manifest_changes=True)
        else:
            names = {'MDS_OFFLINE_TICKET', 'MDS_OFFLINE_ROOT',
                     'MDS_OFFLINE_REPORT_ROOT', 'MDS_ENVIRONMENT_OUTPUT_ROOT',
                     'MDS_OFFLINE_TESTED_SHA', 'MDS_OFFLINE_SOURCE_MODE',
                     'MDS_OFFLINE_SOURCE_DIGEST', 'MDS_OFFLINE_RUN_ID'}
            inherited = {name: os.environ[name] for name in names if name in os.environ}
            for name in names:
                os.environ.pop(name, None)
            try:
                assert 'MDS_OFFLINE_TICKET' not in os.environ
                with pytest.raises(ValueError) as rejected:
                    guard.ensure_context(replica)
                expected = ('OFFLINE_PATH_ALIAS' if case == 'source-identity-alias'
                            else 'OFFLINE_SOURCE_PRIVATE_DATA')
                assert str(rejected.value) == expected
                assert guard._CONTEXT is None
                assert open_calls == leaf_calls == []
                result.update(ticketless=True, preflight_refused=True,
                              rejection=str(rejected.value), context_created=False)
            finally:
                for name in names:
                    os.environ.pop(name, None)
                os.environ.update(inherited)
    finally:
        io.open = original_open
    result.update(open_calls=len(open_calls), leaf_reader_calls=len(leaf_calls))
    assert guard._CONTEXT is None
    context = guard.ensure_context(root)
    assert context.process_id == ticket['ticket_id'] and context.counts == {}
    print(json.dumps(result, sort_keys=True))
    guard.finish(0)
    return 0


def _source_identity_control(case, tmp_path):
    command = [sys.executable, '-I', '-B', '-X', 'utf8',
               str(ROOT / 'tools/offline_guard.py'), 'control', case]
    result = guard.run_child(command, role='f3-control', roots=[tmp_path], timeout=30)
    assert result.actual_exit == result.returncode == 0
    assert result.validated and result.report_errors == []
    assert result.report['counters'] == {}
    canary = b'F3_CODE03_SYNTHETIC_SECRET_VALUE'
    assert canary not in result.stdout + result.stderr
    for report in Path(result.ticket['report_dir']).rglob('*.json'):
        assert canary not in report.read_bytes()
    return json.loads(result.stdout.decode('utf-8').splitlines()[-1])


@pytest.mark.parametrize('case', ['source-identity-ftp', 'source-identity-nested',
                                 'source-identity-alias', 'source-identity-normal'])
def test_source_identity_preflight_checks_before_content_read(case, tmp_path):
    result = _source_identity_control(case, tmp_path)
    assert result['case'] == case
    if case == 'source-identity-normal':
        assert result['ordinary_content_hashed'] and result['manifest_changes']
        assert result['source_modes'] == ['commit', 'working-tree', 'commit', 'working-tree']
        assert result['open_calls'] == result['leaf_reader_calls'] == 4
    else:
        assert result['ticketless'] and result['preflight_refused']
        assert not result['context_created']
        assert result['open_calls'] == result['leaf_reader_calls'] == 0
