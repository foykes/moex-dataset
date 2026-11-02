"""Literal source/projection/target oracles; no live publication clients."""
import base64
import builtins
from contextlib import contextmanager
import hashlib
import io
import json
from pathlib import Path
import sys
import types

import pytest


@pytest.fixture
def source(tmp_path, oracle, upload, monkeypatch):
    root = tmp_path / 'datasets'
    root.mkdir()
    (root / '10years_data_1d_interval.csv').write_bytes(oracle['csv']['text'].encode())
    for name, text in oracle['files'].items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.encode())
    monkeypatch.setattr(upload, 'datasets_path', str(root) + '/')
    return root


def fingerprints(root):
    return {path.relative_to(root).as_posix(): (path.read_bytes(), hashlib.sha256(path.read_bytes()).hexdigest())
            for path in root.rglob('*') if path.is_file()}


def test_original_blob_and_fixture_bytes(oracle):
    original = base64.b64decode(oracle['baseline_upload_base64'], validate=True)
    assert len(original) == oracle['baseline_upload_bytes']
    assert hashlib.sha256(original).hexdigest() == '95945e89d8bc766f934132270a8ee5df75d5382cad86ff44771eb352d5e5acb1'
    assert hashlib.sha1(b'blob ' + str(len(original)).encode() + b'\0' + original).hexdigest() == 'dc1f12bc63eadebe589a666ffcb6a0a2bb908747'
    data = oracle['csv']['text'].encode()
    assert len(data) == oracle['csv']['bytes']
    assert hashlib.sha256(data).hexdigest() == '6c49e66ff43bddee5e0d9c4dfa032c0483c807c996d613a4a66dcf179d3510c9'


def test_inventory_is_shallow_and_preserves_destinations(upload, source):
    before = fingerprints(source)
    plan = upload._d2_build_source_plan()
    targets = {row['destination'] for row in plan['ftp']}
    assert targets == {'README.md', '10years_data_1d_interval.csv',
                       'dividends/dividend.csv', 'ticker_lists/tickers.csv'}
    assert len(plan['google']) == 1
    assert len({row['output_id'] for row in plan['ftp']}) == 4
    assert {row['group'] for row in plan['ftp']} == {'root', 'dividends', 'ticker_lists'}
    upload._d2_recheck_source_plan(plan)
    assert fingerprints(source) == before


@pytest.mark.parametrize('directory', ['root', 'dividends', 'ticker_lists'])
def test_missing_inventory_directory_fails_before_launch(upload, source, directory, monkeypatch):
    if directory == 'root':
        monkeypatch.setattr(upload, 'datasets_path', str(source / 'absent'))
    else:
        # Preserve existing fixture bytes while making the named directory absent.
        (source / directory).rename(source / (directory + '-saved'))
    with pytest.raises(Exception, match='D2_'):
        upload._d2_build_source_plan()


def test_inventory_drift_fails_without_rescan_or_mutation(upload, source):
    plan = upload._d2_build_source_plan()
    (source / 'late.csv').write_bytes(b'LATE\n')
    with pytest.raises(Exception, match='D2_SOURCE_CHANGED'):
        upload._d2_recheck_source_plan(plan)
    assert all(row['destination'] != 'late.csv' for row in plan['ftp'])


def test_casefold_collision_is_rejected(upload, source, monkeypatch):
    # Windows cannot create case-distinct real filenames; enumeration is the fault boundary.
    import os
    original = os.scandir
    @contextmanager
    def listing(path):
        with original(path) as entries:
            names = list(entries)
        if Path(path) == source:
            names.append(types.SimpleNamespace(name='readme.MD',
                stat=lambda **kwargs: (source / 'README.md').stat()))
        yield iter(names)
    monkeypatch.setattr(os, 'scandir', listing)
    with pytest.raises(Exception, match='D2_'):
        upload._d2_build_source_plan()


def test_google_projection_and_write_arguments_are_literal(upload, source, oracle, monkeypatch):
    before = fingerprints(source)
    calls, states = [], []
    class Sheet:
        def clear(self):
            calls.append('clear')
        def set_dataframe(self, frame, start, **kwargs):
            assert list(frame.columns) == oracle['csv']['columns']
            assert frame.values.tolist() == oracle['csv']['rows']
            assert list(frame.index) == [0, 1]
            assert dict(start=start, **kwargs) == oracle['google_arguments']
            calls.append('write')
    class Client:
        def open_by_url(self, url):
            assert url == 'D2_DISPOSABLE_GOOGLE_TARGET'
            return [Sheet()]
    fake = types.ModuleType('pygsheets')
    fake.authorize = lambda **kwargs: Client()
    monkeypatch.setitem(sys.modules, 'pygsheets', fake)
    monkeypatch.setattr(upload, 'pygsheets', fake)
    plan = upload._d2_build_source_plan()
    assert upload.gdoc_upload(str(source.parent), 'D2_DISPOSABLE_GOOGLE_TARGET',
        source_plan=plan['google'], output_observer=lambda token, state: states.append((token, state))) is None
    assert calls == ['clear', 'write']
    assert [state for _, state in states] == ['UNKNOWN', 'ACKNOWLEDGED']
    assert fingerprints(source) == before


@pytest.mark.parametrize('fault', ['clear', 'write'])
def test_google_partial_mutation_stays_unknown(upload, source, monkeypatch, fault):
    calls, states = [], []
    class Sheet:
        def clear(self):
            calls.append('clear')
            if fault == 'clear':
                raise RuntimeError('D2_SYNTHETIC_SECRET_CANARY')
        def set_dataframe(self, *args, **kwargs):
            calls.append('write')
            raise RuntimeError('D2_SYNTHETIC_SECRET_CANARY')
    fake = types.ModuleType('pygsheets')
    fake.authorize = lambda **kwargs: types.SimpleNamespace(open_by_url=lambda url: [Sheet()])
    monkeypatch.setitem(sys.modules, 'pygsheets', fake)
    monkeypatch.setattr(upload, 'pygsheets', fake)
    before = fingerprints(source)
    with pytest.raises(Exception, match='D2_') as error:
        upload.gdoc_upload(str(source.parent), 'D2_DISPOSABLE_GOOGLE_TARGET',
            source_plan=upload._d2_build_source_plan()['google'], output_observer=lambda token, state: states.append(state))
    assert states == ['UNKNOWN']
    assert calls == (['clear'] if fault == 'clear' else ['clear', 'write'])
    assert 'D2_SYNTHETIC_SECRET_CANARY' not in str(error.value)
    assert fingerprints(source) == before


@pytest.mark.parametrize('fail_at', [None, 1, 2, 3, 4])
def test_ftp_first_failure_stops_and_closes_with_exact_targets(upload, source, monkeypatch, fail_at):
    before, calls, states = fingerprints(source), [], []
    class FTP:
        encoding = None
        def storbinary(self, command, stream):
            calls.append((command, stream.read()))
            if fail_at == len(calls):
                raise RuntimeError('D2_SYNTHETIC_SECRET_CANARY')
        def quit(self):
            calls.append(('quit', None))
        def close(self):
            calls.append(('close', None))
    monkeypatch.setattr(upload.ftplib, 'FTP', lambda *a, **kw: FTP())
    original = builtins.open
    def fake_open(path, *a, **kw):
        if str(path).replace('\\', '/').endswith('moex.foykes.com.json'):
            return io.StringIO(json.dumps({'domain':'fixture','username':'fixture','password':'fixture'}))
        return original(path, *a, **kw)
    monkeypatch.setattr(builtins, 'open', fake_open)
    plan = upload._d2_build_source_plan()
    if fail_at is None:
        assert upload.ftp_upload(str(source.parent), source_plan=plan['ftp'],
            output_observer=lambda token, state: states.append((token, state))) is None
    else:
        with pytest.raises(Exception, match='D2_') as error:
            upload.ftp_upload(str(source.parent), source_plan=plan['ftp'],
                output_observer=lambda token, state: states.append((token, state)))
        assert 'D2_SYNTHETIC_SECRET_CANARY' not in str(error.value)
    expected = [('STOR 10years_data_1d_interval.csv', before['10years_data_1d_interval.csv'][0]),
                ('STOR README.md', b'D2 fixture root\n'),
                ('STOR dividends/dividend.csv', b'ticker,dividend\nAAA,1.25\n'),
                ('STOR ticker_lists/tickers.csv', b'ticker\nAAA\nBBB\n')]
    transfers = [row for row in calls if row[0].startswith('STOR ')]
    expected_transfers = expected[:fail_at] if fail_at is not None else expected
    assert transfers == expected_transfers
    assert calls[-2:] == [('quit',None),('close',None)]
    expected_states = ['UNKNOWN','ACKNOWLEDGED'] * (4 if fail_at is None else fail_at-1)
    if fail_at is not None:
        expected_states.append('UNKNOWN')
    assert [state for _, state in states] == expected_states
    if fail_at is not None:
        assert states[-1][1] == 'UNKNOWN'
    assert fingerprints(source) == before


@pytest.mark.parametrize('key', ['max_ftp_outputs','max_checkpoint_bytes','max_result_bytes','max_run_bytes'])
@pytest.mark.parametrize('bad', [True, 0, -1, 1.5, '1'])
def test_diagnostic_limits_are_closed_positive_integers(upload, key, bad):
    with pytest.raises(Exception, match='D2_'):
        upload._d2_validate_limits({key:bad})


def test_diagnostic_limits_cannot_expand_or_change_timeout(upload, oracle):
    assert upload._d2_validate_limits(None) == oracle['diagnostic_profile']
    for key, maximum in oracle['diagnostic_profile'].items():
        with pytest.raises(Exception, match='D2_'):
            upload._d2_validate_limits({key:maximum + 1})
        assert upload._d2_validate_limits({key:maximum - 1})[key] == maximum - 1
    with pytest.raises(Exception, match='D2_'):
        upload._d2_validate_limits({'work_seconds':15})


def test_output_count_512_and_513_never_truncates(upload, tmp_path, oracle, monkeypatch):
    root = tmp_path / 'inventory-boundary'
    root.mkdir()
    (root / 'dividends').mkdir()
    (root / 'ticker_lists').mkdir()
    (root / '10years_data_1d_interval.csv').write_bytes(oracle['csv']['text'].encode())
    for number in range(511):
        (root / ('file-' + str(number).zfill(4) + '.csv')).write_bytes(b'FIXTURE\n')
    monkeypatch.setattr(upload, 'datasets_path', str(root))
    calls = []
    monkeypatch.setattr(upload.ftplib, 'FTP', lambda *args, **kwargs: calls.append('FTP'))
    plan = upload._d2_build_source_plan()
    assert len(plan['ftp']) == 512
    assert len({row['output_id'] for row in plan['ftp']}) == 512
    assert len({row['destination'] for row in plan['ftp']}) == 512
    assert fingerprints(root)['10years_data_1d_interval.csv'][1] == oracle['csv']['sha256']
    (root / 'boundary-plus-one.csv').write_bytes(b'FIXTURE+1\n')
    with pytest.raises(Exception, match='D2_OUTPUT_LIMIT'):
        upload._d2_build_source_plan()
    assert calls == []
    assert len(plan['ftp']) == 512


def test_inventory_os_error_is_safe_and_read_only(upload, source, monkeypatch, capsys):
    import os
    before = fingerprints(source)
    def denied(path):
        raise PermissionError('D2_SYNTHETIC_SECRET_CANARY')
    monkeypatch.setattr(os, 'scandir', denied)
    with pytest.raises(Exception, match='D2_') as error:
        upload._d2_build_source_plan()
    assert 'D2_SYNTHETIC_SECRET_CANARY' not in str(error.value)
    monkeypatch.undo()
    assert fingerprints(source) == before
    captured = capsys.readouterr()
    assert 'D2_SYNTHETIC_SECRET_CANARY' not in captured.out + captured.err


def test_source_open_failure_is_safe_before_mutation(upload, source, monkeypatch):
    before = fingerprints(source)
    original = Path.open
    def denied(path, *args, **kwargs):
        if path == source / 'README.md':
            raise PermissionError('D2_SYNTHETIC_SECRET_CANARY')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', denied)
    with pytest.raises(Exception, match='D2_SOURCE_INVALID') as error:
        upload._d2_build_source_plan()
    assert 'D2_SYNTHETIC_SECRET_CANARY' not in str(error.value)
    monkeypatch.setattr(Path, 'open', original)
    assert fingerprints(source) == before


def test_hardlink_source_alias_refused(upload, source):
    import os
    before = fingerprints(source)
    alias = source / 'alias.csv'
    os.link(source / 'README.md', alias)
    assert (source / 'README.md').stat().st_nlink == 2
    with pytest.raises(Exception, match='D2_SOURCE_INVALID'):
        upload._d2_build_source_plan()
    alias.unlink()
    assert fingerprints(source) == before


def test_reparse_metadata_refused_before_read(upload, source, monkeypatch):
    before = fingerprints(source)
    original = Path.lstat
    def reparse(path, *args, **kwargs):
        info = original(path, *args, **kwargs)
        if path == source / 'README.md':
            return types.SimpleNamespace(st_mode=info.st_mode, st_file_attributes=1024, st_nlink=1)
        return info
    monkeypatch.setattr(Path, 'lstat', reparse)
    with pytest.raises(Exception, match='D2_SOURCE_INVALID'):
        upload._d2_build_source_plan()
    assert fingerprints(source) == before


def test_open_file_identity_mismatch_refused(upload, source):
    plan = upload._d2_build_source_plan()
    item = dict(plan['google'][0])
    expected = list(item['identity'])
    expected[2] += 1
    item['identity'] = tuple(expected)
    before = fingerprints(source)
    with (source / '10years_data_1d_interval.csv').open('rb') as stream:
        with pytest.raises(Exception, match='D2_SOURCE_CHANGED'):
            upload._d2_check_source_handle(stream, item)
    assert fingerprints(source) == before


def test_ftp_late_source_change_is_unknown_and_stops_peer_outputs(upload, source, monkeypatch):
    before, states, calls = fingerprints(source), [], []
    plan = upload._d2_build_source_plan()
    target = source / '10years_data_1d_interval.csv'
    class FTP:
        def storbinary(self, command, stream):
            calls.append(command)
            stream.read()
            target.write_bytes(before['10years_data_1d_interval.csv'][0] + b'CHANGED\n')
        def quit(self): pass
        def close(self): pass
    monkeypatch.setattr(upload.ftplib, 'FTP', lambda *a, **kw: FTP())
    original = builtins.open
    def fake_open(path, *args, **kwargs):
        if str(path).replace('\\','/').endswith('moex.foykes.com.json'):
            return io.StringIO('{"domain":"fixture","username":"fixture","password":"fixture"}')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(builtins, 'open', fake_open)
    with pytest.raises(Exception, match='D2_SOURCE_CHANGED'):
        upload.ftp_upload(str(source.parent), source_plan=plan['ftp'],
            output_observer=lambda token, state: states.append(state))
    assert calls == ['STOR 10years_data_1d_interval.csv']
    assert states == ['UNKNOWN']
    assert target.read_bytes() == before['10years_data_1d_interval.csv'][0] + b'CHANGED\n'
    for name, (data, digest) in before.items():
        if name != '10years_data_1d_interval.csv':
            assert (source / name).read_bytes() == data
            assert hashlib.sha256((source / name).read_bytes()).hexdigest() == digest


def test_ftp_cleanup_failure_preserves_primary_error(upload, source, monkeypatch):
    states, closes = [], []
    class FTP:
        def storbinary(self, *args): raise RuntimeError('D2_SOURCE_CHANGED')
        def quit(self):
            closes.append('quit')
            raise RuntimeError('D2_SYNTHETIC_SECONDARY_SECRET')
        def close(self):
            closes.append('close')
            raise RuntimeError('D2_SYNTHETIC_SECONDARY_SECRET')
    monkeypatch.setattr(upload.ftplib, 'FTP', lambda *args: FTP())
    original = builtins.open
    def fake_open(path, *args, **kwargs):
        if str(path).replace('\\','/').endswith('moex.foykes.com.json'):
            return io.StringIO('{"domain":"fixture","username":"fixture","password":"fixture"}')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(builtins, 'open', fake_open)
    with pytest.raises(Exception, match='D2_SOURCE_CHANGED') as error:
        upload.ftp_upload(str(source.parent), source_plan=upload._d2_build_source_plan()['ftp'],
            output_observer=lambda token, state: states.append(state))
    assert states == ['UNKNOWN']
    assert closes == ['quit','close']
    assert 'D2_SYNTHETIC_SECONDARY_SECRET' not in str(error.value)
