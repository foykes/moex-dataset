"""Actual disposable imports and Windows spawn; never execute live entrypoints."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
from _probe import MODULES

ROOT = Path(__file__).resolve().parents[2]
PROBE = Path(__file__).with_name('_probe.py')


def probe(action, root, *args, cwd=None):
    command = [sys.executable, '-I', '-B', '-X', 'utf8', str(PROBE), action, str(root), *args]
    result = subprocess.run(command, cwd=cwd or ROOT, capture_output=True, text=True,
                            encoding='utf-8', errors='strict', timeout=75)
    records = [line.removeprefix('F2_RESULT:') for line in result.stdout.splitlines() if line.startswith('F2_RESULT:')]
    assert len(records) == 1, (result.returncode, result.stdout, result.stderr)
    return result, json.loads(records[0])


@pytest.mark.parametrize('name', MODULES)
def test_real_module_import_is_offline(name, tmp_path, record_property):
    files = [ROOT / (stem + suffix) for stem in MODULES for suffix in ('.py', '.ipynb') if (ROOT / (stem + suffix)).exists()]
    before = {file.name: hashlib.sha256(file.read_bytes()).hexdigest() for file in files}
    result, report = probe('import', ROOT, name, cwd=tmp_path)
    assert result.returncode == 0, report
    assert report['outcome'] == 'PASS'
    assert report['counts'] == dict(network=0, conversion=0, writers=0, secrets=0, dataset_reads=0)
    assert result.stdout.splitlines() == ['F2_RESULT:' + json.dumps(report, ensure_ascii=False)]
    assert before == {file.name: hashlib.sha256(file.read_bytes()).hexdigest() for file in files}
    record_property('actual_import', report)


def test_real_windows_spawn_two_children(record_property):
    if sys.platform != 'win32':
        pytest.skip('Windows spawn AC requires a real Windows runner')
    result, report = probe('spawn', ROOT)
    assert result.returncode == 0, report
    assert report['start_method'] == 'spawn'
    assert len(report['children']) == 2
    for child in report['children']:
        assert child['sentinel'] == 'F2_SPAWN_OK'
        assert child['exit_code'] == 0
        assert child['elapsed_seconds'] <= 30
        assert child['outcome'] == 'PASS'
        assert not any(child['counts'].values())
    record_property('actual_spawn', report)


def test_guards_reject_real_controls_before_side_effects(tmp_path):
    result, report = probe('guard-controls', tmp_path)
    assert result.returncode == 0
    assert report['counts'] == dict(network=1, conversion=1, writers=1, secrets=1, dataset_reads=0)
    assert not (tmp_path / 'F2_FORBIDDEN_WRITE').exists()


@pytest.mark.parametrize('name', ('root with spaces', 'корень Юникод'))
def test_main_import_root_paths_other_cwd(name, tmp_path):
    root = tmp_path / name
    root.mkdir()
    (root / 'main.py').write_bytes((ROOT / 'main.py').read_bytes())
    foreign = tmp_path / 'foreign cwd'
    foreign.mkdir()
    # Windows Path produces actual backslashes; subprocess argv preserves them.
    result, report = probe('import', root, 'main', cwd=foreign)
    assert result.returncode == 0, report
    assert not any(report['counts'].values())


def test_evidence_hardlink_preflight_preserves_external_sentinel(tmp_path):
    root = tmp_path / 'isolated-profile'
    evidence = root / '.f2/evidence'
    evidence.mkdir(parents=True)
    sentinel = tmp_path / 'external-evidence-canary'
    sentinel.write_bytes(b'KEEP EVIDENCE\n')
    os.link(sentinel, evidence / 'unsafe.log')
    result = subprocess.run([sys.executable, '-I', '-B', '-S', str(PROBE),
                             'preflight', str(root), 'unsafe'], cwd=ROOT,
                            capture_output=True, timeout=30)
    assert result.returncode != 0
    assert b'alias' in result.stderr or b'Unsafe evidence' in result.stderr
    assert sentinel.read_bytes() == b'KEEP EVIDENCE\n'
    assert (evidence / 'unsafe.log').stat().st_nlink == 2
