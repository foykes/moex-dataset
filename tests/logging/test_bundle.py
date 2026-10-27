import hashlib
import json
import uuid
import zipfile

import pytest

from _probe import ROOT


def test_active_rejected_then_immutable_hashes(log, session):
    destination = ROOT / '.f-log/bundles' / (uuid.uuid4().hex + '.zip')
    with pytest.raises((ValueError, OSError)):
        log.export_diagnostic_bundle(session['_run_root'], destination)
    log.emit_event(session, 'ERROR', 'fixture', 'password=BUNDLE_CANARY', {})
    report = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    result = log.export_diagnostic_bundle(session['_run_root'], destination)
    assert result['sha256'] == hashlib.sha256(destination.read_bytes()).hexdigest()
    with zipfile.ZipFile(destination) as archive:
        manifest = json.loads(archive.read('bundle_manifest.json'))
        for item in manifest['files']:
            data = archive.read(item['file'])
            assert hashlib.sha256(data).hexdigest() == item['sha256']
            assert len(data) == item['bytes']
            assert b'BUNDLE_CANARY' not in data
            assert b'C:\\Users' not in data and b'c:\\users' not in data
        assert set(archive.namelist()) == {i['file'] for i in manifest['files']} | {'bundle_manifest.json'}
    assert result['evidence_incomplete'] == report['evidence_incomplete']


@pytest.mark.parametrize('name', ['events.jsonl', 'run_summary.json', 'context.json'])
def test_post_seal_mutation_rejected(log, session, name):
    log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    path = session['_run_root'] / name
    data = path.read_bytes()
    path.write_bytes(data.replace(b'COMPLETED', b'UNKNOWN  ') if b'COMPLETED' in data else data.replace(b'offline', b'changed'))
    with pytest.raises(ValueError):
        log.export_diagnostic_bundle(session['_run_root'], ROOT / '.f-log/bundles' / (uuid.uuid4().hex + '.zip'))


def test_sealed_failed_run_can_export(log, session):
    error = log.make_error(session, ValueError('safe'), 'fixture')
    report = log.finalize_logging(session, execution_outcome='FAILED', exit_status=1, application_error=error)
    assert report['storage_sealed']
    result = log.export_diagnostic_bundle(session['_run_root'], ROOT / '.f-log/bundles' / (uuid.uuid4().hex + '.zip'))
    assert not result['evidence_incomplete']


def test_mutation_during_copy_then_restore_rejected(log, session, monkeypatch):
    log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    original = log._write
    source = session['_run_root'] / 'run_summary.json'
    data = source.read_bytes()
    seal = session['_run_root'] / 'summary.sha256'
    seal_data = seal.read_bytes()
    changed = data.replace(b'COMPLETED', b'UNKNOWN  ')
    previous_snapshots = set((ROOT / '.f-log/bundles').glob('snapshot-*'))
    fired = [False]
    def write(handle, payload):
        if 'snapshot-' in str(handle.name) and Path(handle.name).name == 'run_summary.json':
            fired[0] = True
            source.write_bytes(changed)
            seal.write_bytes(hashlib.sha256(changed).hexdigest().encode() + b'\n')
            original(handle, changed)
            source.write_bytes(data)
            seal.write_bytes(seal_data)
            return
        return original(handle, payload)
    from pathlib import Path
    monkeypatch.setattr(log, '_write', write)
    with pytest.raises(ValueError, match='LOG_BUNDLE_MUTATED'):
        log.export_diagnostic_bundle(session['_run_root'], ROOT / '.f-log/bundles' / (uuid.uuid4().hex + '.zip'))
    assert fired[0]
    assert set((ROOT / '.f-log/bundles').glob('snapshot-*')) == previous_snapshots
