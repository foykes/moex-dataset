"""Literal publication interfaces and read-only source admission oracles."""

import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import types

import pytest


def test_original_blob_and_fixture_bytes(oracle):
    original = base64.b64decode(oracle['baseline_upload_base64'], validate=True)
    assert len(original) == 7210 == oracle['baseline_upload_bytes']
    assert hashlib.sha256(original).hexdigest() == '95945e89d8bc766f934132270a8ee5df75d5382cad86ff44771eb352d5e5acb1'
    blob = b'blob ' + str(len(original)).encode() + b'\0' + original
    assert hashlib.sha1(blob).hexdigest() == 'dc1f12bc63eadebe589a666ffcb6a0a2bb908747'
    csv = oracle['csv']['text'].encode('utf-8')
    assert len(csv) == 229
    assert hashlib.sha256(csv).hexdigest() == '6c49e66ff43bddee5e0d9c4dfa032c0483c807c996d613a4a66dcf179d3510c9'


def test_inventory_is_shallow_and_preserves_destinations(upload, source):
    plan = upload._d2_build_source_plan(str(source['project']))
    assert {row['destination'] for row in plan['ftp']} == {
        'README.md', '10years_data_1d_interval.csv',
        'dividends/dividend.csv', 'ticker_lists/tickers.csv',
    }
    assert len(plan['google']) == 1
    assert Path(plan['google'][0]['source_path']) == source['datasets'] / '10years_data_1d_interval.csv'
    assert all('dohod.ru' not in row['destination'] for row in plan['ftp'])


@pytest.mark.parametrize('directory', ['root', 'dividends', 'ticker_lists'])
def test_missing_inventory_directory_fails_before_launch(upload, source, directory):
    path = source['datasets'] if directory == 'root' else source['datasets'] / directory
    renamed = path.with_name(path.name + '-unavailable')
    path.rename(renamed)
    with pytest.raises(Exception):
        upload._d2_build_source_plan(str(source['project']))


def test_empty_optional_inventory_directories_are_admitted(upload, source):
    (source['datasets'] / 'dividends/dividend.csv').unlink()
    (source['datasets'] / 'ticker_lists/tickers.csv').unlink()
    plan = upload._d2_build_source_plan(str(source['project']))
    assert {row['destination'] for row in plan['ftp']} == {'README.md', '10years_data_1d_interval.csv'}


def test_source_binding_does_not_follow_callable_root_or_cwd(upload, source, tmp_path, monkeypatch):
    decoy = tmp_path / 'decoy'
    decoy.mkdir()
    (decoy / 'datasets').mkdir()
    monkeypatch.chdir(decoy)
    plan = upload._d2_build_source_plan(str(source['project']))
    assert Path(plan['google'][0]['source_path']) == source['datasets'] / '10years_data_1d_interval.csv'
    assert all(Path(row['source_path']).is_relative_to(source['datasets']) for row in plan['ftp'])


def test_inventory_drift_fails_without_silent_rescan(upload, source):
    plan = upload._d2_build_source_plan(str(source['project']))
    before = copy.deepcopy(plan)
    (source['datasets'] / 'new.csv').write_bytes(b'new\n')
    with pytest.raises(Exception):
        upload._d2_recheck_source_plan(plan)
    assert plan == before


def test_source_content_change_is_rejected(upload, source):
    plan = upload._d2_build_source_plan(str(source['project']))
    (source['datasets'] / 'README.md').write_bytes(b'changed-size-and-content\n')
    with pytest.raises(Exception):
        upload._d2_recheck_source_plan(plan)


def test_casefold_collision_is_rejected(upload, source):
    (source['datasets'] / 'readme.MD').write_bytes(b'collision\n')
    with pytest.raises(Exception):
        upload._d2_build_source_plan(str(source['project']))


def test_hardlink_source_alias_is_rejected(upload, source):
    os.link(source['datasets'] / 'README.md', source['datasets'] / 'alias.txt')
    with pytest.raises(Exception):
        upload._d2_build_source_plan(str(source['project']))


def test_symlink_source_alias_is_rejected(upload, source):
    assert sys.platform == 'linux', 'this no-follow oracle requires the admitted Linux suite'
    (source['datasets'] / 'alias').symlink_to(source['datasets'] / 'README.md')
    with pytest.raises(Exception):
        upload._d2_build_source_plan(str(source['project']))


def test_output_count_512_and_513_never_truncates(upload, source):
    for index in range(508):
        (source['datasets'] / ('extra-' + str(index) + '.csv')).write_bytes(b'x\n')
    plan = upload._d2_build_source_plan(str(source['project']))
    assert len(plan['ftp']) == len({row['output_id'] for row in plan['ftp']}) == 512
    (source['datasets'] / 'boundary-plus-one.csv').write_bytes(b'x\n')
    with pytest.raises(Exception):
        upload._d2_build_source_plan(str(source['project']))
    assert len(plan['ftp']) == 512


@pytest.mark.parametrize('key', ['max_ftp_outputs', 'max_checkpoint_bytes', 'max_result_bytes', 'max_run_bytes'])
@pytest.mark.parametrize('bad', [True, False, 0, -1, 1.5, '1', None])
def test_diagnostic_limits_are_closed_positive_integers(upload, key, bad):
    with pytest.raises(Exception):
        upload._d2_validate_limits({key: bad})


def test_diagnostic_limits_cannot_expand_or_change_timeout(upload, oracle):
    assert upload._d2_validate_limits() == oracle['diagnostic_profile']
    for key, maximum in oracle['diagnostic_profile'].items():
        with pytest.raises(Exception):
            upload._d2_validate_limits({key: maximum + 1})
    for unknown in ('timeout', 'work_seconds', 'bypass', 'target'):
        with pytest.raises(Exception):
            upload._d2_validate_limits({unknown: 1})


def _google_doubles(monkeypatch, fault=None):
    observation = {'clear': 0, 'write': 0}
    error = RuntimeError('D2_PRIVATE_CANARY_4c1e11')
    class Sheet:
        def clear(self):
            observation['clear'] += 1
            if fault == 'clear':
                raise error
        def set_dataframe(self, frame, start, **options):
            observation.update(write=observation['write'] + 1, columns=list(frame.columns),
                               rows=frame.values.tolist(), start=start, options=options)
            if fault == 'write':
                raise error
    class Client:
        def open_by_url(self, target):
            observation['target'] = target
            return [Sheet()]
    def authorize(**options):
        observation['authorization'] = options
        return Client()
    monkeypatch.setitem(sys.modules, 'pygsheets', types.SimpleNamespace(authorize=authorize))
    return observation, error


def test_google_projection_and_write_arguments_are_literal(upload, source, oracle, monkeypatch):
    observation, error = _google_doubles(monkeypatch)
    plan = upload._d2_build_source_plan(str(source['project']))
    states = []
    upload._d2_gdoc_upload(str(source['project']), 'D2_LITERAL_TARGET', source_plan=plan['google'],
                         output_observer=lambda *row: states.append(row))
    assert observation['columns'] == ['ticker', 'close', 'begin', 'open', 'volume', 'high', 'low', 'value', 'end', 'RSI14']
    assert observation['rows'] == oracle['csv']['rows']
    assert observation['target'] == 'D2_LITERAL_TARGET'
    assert observation['start'] == 'A1'
    assert observation['options'] == {'copy_index': False, 'copy_head': True, 'fit': False,
                                      'escape_formulae': False, 'nan': 'NaN'}
    credential = observation['authorization']['service_file']
    assert Path(credential).resolve() == source['secrets'] / 'python-304621-1e403209f05f.json'
    assert [row[1] for row in states] == ['UNKNOWN', 'ACKNOWLEDGED']


@pytest.mark.parametrize('fault', ['clear', 'write'])
def test_google_partial_mutation_preserves_original_error_and_unknown(upload, source, monkeypatch, fault):
    observation, error = _google_doubles(monkeypatch, fault)
    plan = upload._d2_build_source_plan(str(source['project']))
    states = []
    with pytest.raises(RuntimeError) as raised:
        upload._d2_gdoc_upload(str(source['project']), 'D2_LITERAL_TARGET', source_plan=plan['google'],
                             output_observer=lambda *row: states.append(row))
    assert raised.value is error
    assert observation['clear'] == 1
    assert [row[1] for row in states] == ['UNKNOWN']


@pytest.mark.parametrize('fail_at', [None, 1, 2, 3, 4])
def test_ftp_first_failure_stops_and_closes_with_exact_targets(upload, source, monkeypatch, fail_at):
    events, states = [], []
    error = RuntimeError('D2_PRIVATE_CANARY_4c1e11')
    class FTP:
        def __init__(self, *arguments):
            assert arguments == ('D2_FAKE_DOMAIN.invalid', 'D2_FAKE_USERNAME', 'D2_FAKE_PASSWORD')
        def storbinary(self, command, stream):
            events.append(('transfer', command, hashlib.sha256(stream.read()).hexdigest()))
            if fail_at == len([row for row in events if row[0] == 'transfer']):
                raise error
        def quit(self):
            events.append(('quit',))
        def close(self):
            events.append(('close',))
    monkeypatch.setitem(sys.modules, 'ftplib', types.SimpleNamespace(FTP=FTP))
    plan = upload._d2_build_source_plan(str(source['project']))
    if fail_at is None:
        upload._d2_ftp_upload(str(source['project']), source_plan=plan['ftp'],
                            output_observer=lambda *row: states.append(row))
    else:
        with pytest.raises(RuntimeError) as raised:
            upload._d2_ftp_upload(str(source['project']), source_plan=plan['ftp'],
                                output_observer=lambda *row: states.append(row))
        assert raised.value is error
    transfers = [row[1] for row in events if row[0] == 'transfer']
    assert len(transfers) == (4 if fail_at is None else fail_at)
    expected = ['STOR ' + row['destination'] for row in plan['ftp']]
    assert transfers == expected[:len(transfers)]
    assert events[-1][0] in {'quit', 'close'}
    assert [row[1] for row in states].count('UNKNOWN') == len(transfers)
    assert [row[1] for row in states].count('ACKNOWLEDGED') == (4 if fail_at is None else fail_at - 1)


def test_inventory_os_error_is_safe_and_read_only(upload, source, monkeypatch, capsys):
    original = upload.os.scandir
    reached = []
    def refused(path):
        if Path(path) == source['datasets'] / 'dividends':
            reached.append(True)
            raise PermissionError('D2_PRIVATE_CANARY_4c1e11')
        return original(path)
    before = {path.relative_to(source['datasets']).as_posix(): path.read_bytes()
              for path in source['datasets'].rglob('*') if path.is_file()}
    monkeypatch.setattr(upload.os, 'scandir', refused)
    with pytest.raises(Exception):
        upload._d2_build_source_plan(str(source['project']))
    assert reached == [True]
    monkeypatch.setattr(upload.os, 'scandir', original)
    after = {path.relative_to(source['datasets']).as_posix(): path.read_bytes()
             for path in source['datasets'].rglob('*') if path.is_file()}
    assert before == after
    output = capsys.readouterr()
    assert 'D2_PRIVATE_CANARY_4c1e11' not in output.out + output.err


def test_source_open_failure_is_before_mutation(upload, source, monkeypatch):
    original = upload.os.open
    reached = []
    def refused(path, *args, **options):
        if Path(path) == source['datasets'] / '10years_data_1d_interval.csv':
            reached.append(True)
            raise PermissionError('D2_PRIVATE_CANARY_4c1e11')
        return original(path, *args, **options)
    monkeypatch.setattr(upload.os, 'open', refused)
    with pytest.raises(Exception):
        upload._d2_build_source_plan(str(source['project']))
    assert reached == [True]


def test_open_file_identity_mismatch_refused(upload, source):
    plan = upload._d2_build_source_plan(str(source['project']))
    item = copy.deepcopy(plan['ftp'][0])
    item['identity'][1] += 1
    with open(item['source_path'], 'rb') as handle:
        with pytest.raises(Exception):
            upload._d2_check_source_handle(handle, item)


def test_ftp_late_source_change_stops_remaining_remote_operations(upload, source, monkeypatch):
    plan = upload._d2_build_source_plan(str(source['project']))
    later = Path(plan['ftp'][1]['source_path'])
    events, states = [], []
    class FTP:
        def __init__(self, *args):
            pass
        def storbinary(self, command, stream):
            events.append(command)
            stream.read()
            later.write_bytes(b'changed after first remote acknowledgement\n')
        def close(self):
            events.append('close')
        def quit(self):
            events.append('quit')
    monkeypatch.setitem(sys.modules, 'ftplib', types.SimpleNamespace(FTP=FTP))
    with pytest.raises(Exception):
        upload._d2_ftp_upload(str(source['project']), source_plan=plan['ftp'],
            output_observer=lambda *row: states.append(row))
    assert events == ['STOR ' + plan['ftp'][0]['destination'], 'close']
    assert [row[1] for row in states] == ['UNKNOWN', 'ACKNOWLEDGED']


def test_ftp_cleanup_failure_preserves_primary_error(upload, source, monkeypatch):
    original = RuntimeError('D2_PRIVATE_CANARY_4c1e11')
    class FTP:
        def __init__(self, *args):
            pass
        def storbinary(self, command, stream):
            raise original
        def close(self):
            raise OSError('D2_PRIVATE_CANARY_4c1e11')
        def quit(self):
            raise AssertionError('quit after failed transfer')
    monkeypatch.setitem(sys.modules, 'ftplib', types.SimpleNamespace(FTP=FTP))
    plan = upload._d2_build_source_plan(str(source['project']))
    with pytest.raises(RuntimeError) as raised:
        upload._d2_ftp_upload(str(source['project']), source_plan=plan['ftp'])
    assert raised.value is original
