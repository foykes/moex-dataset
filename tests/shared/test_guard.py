"""Real refused operations and disposable IO; no production data is read."""

import ast
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import runpy
import shutil
import sys
import time
import uuid

import pytest
import mds_offline_guard as guard


ROOT = Path(__file__).resolve().parents[2]
REVIEWED_BASE = '07945ed4ded747145b40d7a7c25e8ce97abeaa0d'


def test_real_sleep_is_refused_and_caught_violation_stays_journaled(monkeypatch):
    context = guard.ensure_context(ROOT)
    leaf_calls = []
    monkeypatch.setattr(guard, '_ORIGINAL_SLEEP', leaf_calls.append)
    before = context.counts.get('sleep', 0)
    with guard.expect_fault('sleep'):
        with pytest.raises(guard.OfflineViolation, match='F3_OFFLINE_SLEEP'):
            time.sleep(10)
    assert context.counts['sleep'] == before + 1
    assert leaf_calls == []
    before = context.counts['sleep']
    with guard.expect_fault('sleep'):
        try:
            time.sleep(10)
        except guard.OfflineViolation:
            pass
        else:
            raise AssertionError('The workload reached a real sleep')
    assert context.counts['sleep'] == before + 1
    assert context.violations[-1]['kind'] == 'sleep'
    journal = context.report_dir / 'processes' / (
        context.process_id + '.violation.' + str(context.violations[-1]['sequence']) + '.json')
    assert json.loads(journal.read_text(encoding='utf-8'))['kind'] == 'sleep'
    assert leaf_calls == []


@pytest.mark.parametrize('call', [
    lambda: socket.getaddrinfo('F3_SYNTHETIC_DNS.invalid', 443),
    lambda: socket.create_connection(('F3_SYNTHETIC_NETWORK.invalid', 443)),
])
def test_dns_and_connect_are_refused_before_real_access(call):
    context = guard.ensure_context(ROOT)
    before = context.counts.get('network', 0)
    with guard.expect_fault('network'):
        with pytest.raises(guard.OfflineViolation, match='F3_OFFLINE_NETWORK'):
            call()
    assert context.counts['network'] == before + 1


def test_owned_path_and_fdopen_bytes_and_synthetic_dataset_name(tmp_path):
    context = guard.ensure_context(ROOT)
    before = dict(context.counts)
    ordinary = tmp_path / 'bytes.bin'
    ordinary.write_bytes(b'F3_TMP_ROUNDTRIP\x00\xff\n')
    assert ordinary.read_bytes() == b'F3_TMP_ROUNDTRIP\x00\xff\n'
    descriptor_path = tmp_path / 'descriptor.bin'
    descriptor = os.open(descriptor_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'wb') as stream:
        stream.write(b'F3_TMP_ROUNDTRIP\x00\xff\n')
    assert descriptor_path.read_bytes() == b'F3_TMP_ROUNDTRIP\x00\xff\n'
    synthetic = tmp_path / 'datasets' / 'fixture.csv'
    synthetic.parent.mkdir()
    synthetic.write_bytes(b'ticker,close\nFIXTURE_SHARE,101.0\n')
    assert synthetic.read_bytes() == b'ticker,close\nFIXTURE_SHARE,101.0\n'
    assert context.counts == before


@pytest.mark.parametrize('relative', ['.env', 'credentials.json', 'secrets/canary.json'])
def test_only_synthetic_credential_canaries_are_refused_and_not_reported(relative, tmp_path, capsys, monkeypatch):
    context = guard.ensure_context(ROOT)
    canary = 'F3_SYNTHETIC_PRIVATE_CANARY_592'
    destination = tmp_path / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = tmp_path / 'owned-payload'
    payload.write_text(canary, encoding='utf-8')
    # Only owned fixture paths are renamed; no production secret is touched.
    payload.rename(destination)
    leaf_calls = []
    original_open = io.open

    class ObservedReader:
        def __init__(self, stream):
            self.stream = stream
        def __enter__(self):
            self.stream.__enter__()
            return self
        def __exit__(self, *args):
            return self.stream.__exit__(*args)
        def read(self, *args):
            leaf_calls.append(1)
            return self.stream.read(*args)

    def observe_open(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        return ObservedReader(stream) if Path(path) == destination else stream
    monkeypatch.setattr(io, 'open', observe_open)
    with guard.expect_fault('secrets'):
        with pytest.raises(guard.OfflineViolation, match='F3_OFFLINE_SECRETS'):
            destination.read_text(encoding='utf-8')
    assert leaf_calls == []
    output = capsys.readouterr()
    assert canary not in output.out + output.err
    for report in context.report_dir.rglob('*.json'):
        assert canary not in report.read_text(encoding='utf-8')


def test_hardlink_write_is_refused_and_external_fixture_hash_survives(tmp_path):
    sentinel = tmp_path.parent / ('external-canary-' + uuid.uuid4().hex)
    sentinel.write_bytes(b'EXTERNAL_FIXTURE_MUST_SURVIVE\n')
    before = hashlib.sha256(sentinel.read_bytes()).hexdigest()
    alias = tmp_path / 'fixture-alias-current'
    os.link(sentinel, alias)
    try:
        with guard.expect_fault('writers'):
            with pytest.raises(guard.OfflineViolation, match='F3_OFFLINE_WRITERS'):
                alias.write_bytes(b'FORBIDDEN_REPLACEMENT\n')
    finally:
        # Unlinking an owned final component does not dereference the hardlink.
        alias.unlink()
    assert sentinel.read_bytes() == b'EXTERNAL_FIXTURE_MUST_SURVIVE\n'
    assert hashlib.sha256(sentinel.read_bytes()).hexdigest() == before


def _canary_snapshot(path):
    content = path.read_bytes()
    return content, path.stat().st_size, hashlib.sha256(content).hexdigest()


def _assert_refused_mutation(operation, kind):
    context = guard.ensure_context(ROOT)
    before = context.counts.get(kind, 0)
    sequence = len(context.violations)
    with guard.expect_fault(kind):
        with pytest.raises(guard.OfflineViolation, match='F3_OFFLINE_' + kind.upper()):
            operation()
    assert context.counts[kind] == before + 1
    assert len(context.violations) == sequence + 1
    violation = context.violations[-1]
    assert violation['kind'] == kind
    journal = context.report_dir / 'processes' / (
        context.process_id + '.violation.' + str(violation['sequence']) + '.json')
    assert json.loads(journal.read_text(encoding='utf-8')) == violation


@pytest.mark.parametrize('variant, kind', [('path', 'writers'), ('fd', 'descriptors')])
def test_truncate_hardlink_alias_preserves_synthetic_canary(variant, kind, tmp_path):
    sentinel = tmp_path / 'truncate-canary.bin'
    sentinel.write_bytes(b'F3_TRUNCATE_CANARY_MUST_SURVIVE\x00\xff\n')
    before = _canary_snapshot(sentinel)
    descriptor = os.open(sentinel, os.O_RDWR)
    alias = tmp_path / 'truncate-alias-current'
    os.link(sentinel, alias)
    try:
        operation = (lambda: os.truncate(alias, 0)) if variant == 'path' else (
            lambda: os.ftruncate(descriptor, 0))
        _assert_refused_mutation(operation, kind)
    finally:
        # Removing the owned final link preserves its external object's bytes.
        alias.unlink()
        os.close(descriptor)
    assert _canary_snapshot(sentinel) == before


@pytest.mark.parametrize('variant', ['readonly', 'closed', 'unknown'])
def test_ftruncate_requires_a_current_owned_writable_descriptor(variant, tmp_path):
    sentinel = tmp_path / 'descriptor-canary.bin'
    sentinel.write_bytes(b'F3_DESCRIPTOR_CANARY_MUST_SURVIVE\n')
    before = _canary_snapshot(sentinel)
    if variant == 'unknown':
        descriptor = 65535
    else:
        descriptor = os.open(sentinel, os.O_RDONLY if variant == 'readonly' else os.O_RDWR)
        if variant == 'closed':
            os.close(descriptor)
    try:
        _assert_refused_mutation(lambda: os.ftruncate(descriptor, 0), 'descriptors')
    finally:
        if variant == 'readonly':
            os.close(descriptor)
    assert _canary_snapshot(sentinel) == before


@pytest.mark.parametrize('replacement', ['readonly_dup2', 'closed_fdopen'])
def test_ftruncate_rejects_readonly_same_file_descriptor_reuse(replacement, tmp_path):
    sentinel = tmp_path / 'reuse-canary.bin'
    sentinel.write_bytes(b'F3_DESCRIPTOR_MODE_MUST_SURVIVE\n')
    before = _canary_snapshot(sentinel)
    writable = os.open(sentinel, os.O_RDWR)
    if replacement == 'readonly_dup2':
        readonly = os.open(sentinel, os.O_RDONLY)
        os.dup2(readonly, writable)
        descriptor = writable
    else:
        stream = os.fdopen(writable, 'wb')
        stream.close()
        reader = open(sentinel, 'rb')
        descriptor = reader.fileno()
        assert descriptor == writable
    leaf_calls = []
    observing = [True]

    def observe_truncate(event, args):
        if observing[0] and event == 'os.truncate' and args[0] == descriptor:
            leaf_calls.append(1)

    # The guard's earlier audit hook must refuse the operation before this leaf.
    sys.addaudithook(observe_truncate)
    try:
        _assert_refused_mutation(lambda: os.ftruncate(descriptor, 0), 'descriptors')
    finally:
        observing[0] = False
        if replacement == 'readonly_dup2':
            os.close(writable)
            os.close(readonly)
        else:
            reader.close()
    assert leaf_calls == []
    assert _canary_snapshot(sentinel) == before


def test_real_owned_path_and_writable_descriptor_truncate_without_violations(tmp_path):
    context = guard.ensure_context(ROOT)
    before = dict(context.counts)
    sentinel = tmp_path / 'owned-truncate.bin'
    original = b'F3_OWNED_TRUNCATE\x00\xff\n'
    sentinel.write_bytes(original)
    os.truncate(sentinel, 7)
    assert _canary_snapshot(sentinel) == (
        original[:7], 7, hashlib.sha256(original[:7]).hexdigest())
    descriptor = os.open(sentinel, os.O_RDWR)
    duplicate = os.dup(descriptor)
    try:
        os.ftruncate(duplicate, 3)
    finally:
        os.close(duplicate)
        os.close(descriptor)
    assert _canary_snapshot(sentinel) == (
        original[:3], 3, hashlib.sha256(original[:3]).hexdigest())
    assert context.counts == before


def test_real_owned_dup2_and_fdopen_truncate_without_violations(tmp_path):
    context = guard.ensure_context(ROOT)
    before = dict(context.counts)
    sentinel = tmp_path / 'owned-dup2-truncate.bin'
    original = b'F3_OWNED_DESCRIPTOR_LIFECYCLE\x00\xff\n'
    sentinel.write_bytes(original)
    writable = os.open(sentinel, os.O_RDWR)
    replacement = os.open(sentinel, os.O_RDONLY)
    try:
        os.dup2(writable, replacement)
        os.ftruncate(replacement, 7)
        with os.fdopen(replacement, 'r+b') as stream:
            os.ftruncate(stream.fileno(), 3)
        replacement = None
    finally:
        if replacement is not None:
            os.close(replacement)
        os.close(writable)
    assert _canary_snapshot(sentinel) == (
        original[:3], 3, hashlib.sha256(original[:3]).hexdigest())
    assert context.counts == before


def _review_control(case, root, ticket):
    """Fixed child payload; only new synthetic objects are seeded before guard."""
    variant, scenario = case.split('-', 1)
    assert variant in {'truncate', 'ftruncate'}
    assert scenario in {'external', 'source', 'previous', 'alias'}
    issued = Path(ticket['roots'][0])
    # Keep synthetic fixture paths below Windows' ordinary path length limit.
    # The enclosing pytest root is unique; mkdir refuses any existing fixture.
    replica = issued.parent / ('r-' + ticket['ticket_id'][:8])
    relative = {
        'external': 'external-canary.bin',
        'source': 'synthetic-source/sample.py',
        'previous': '.f3/evidence/runs/old/p.json',
        'alias': 'external-canary.bin',
    }[scenario]
    sentinel = replica / relative
    sentinel.parent.mkdir(parents=True)
    sentinel.write_bytes(b'F3_CHILD_TRUNCATE_CANARY_MUST_SURVIVE\x00\xff\n')
    before = _canary_snapshot(sentinel)
    issued.mkdir(parents=True, exist_ok=True)
    target = sentinel
    if scenario == 'alias':
        target = issued / 'truncate-alias-current'
        os.link(sentinel, target)
    descriptor = os.open(target, os.O_RDWR) if variant == 'ftruncate' else None

    def synthetic_fence(event, args):
        if event == 'os.truncate':
            value = args[0]
            permitted = value == descriptor if isinstance(value, int) else (
                Path(value).absolute() == target.absolute())
            if not permitted:
                raise RuntimeError('F3_TRUNCATE_SYNTHETIC_FENCE')
    sys.addaudithook(synthetic_fence)
    context = guard.ensure_context(root, role='f3-control')
    leaf_calls = []

    def after_guard_audit(event, args):
        # Audit callbacks run in registration order. A refused operation never
        # reaches this observer immediately preceding the real OS leaf.
        if event == 'os.truncate':
            leaf_calls.append(1)
    sys.addaudithook(after_guard_audit)
    caught = False
    try:
        os.ftruncate(descriptor, 0) if descriptor is not None else os.truncate(target, 0)
    except guard.OfflineViolation:
        caught = True
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if scenario == 'alias':
            target.unlink()
    after = _canary_snapshot(sentinel)
    proof = dict(case=case, caught=caught, leaf_calls=len(leaf_calls),
                 before_size=before[1], after_size=after[1],
                 before_sha256=before[2], after_sha256=after[2],
                 canary_preserved=after == before)
    print(json.dumps(proof, sort_keys=True))
    guard.finish(0)
    return 0


@pytest.mark.parametrize('scenario', ['external', 'source', 'previous', 'alias'])
@pytest.mark.parametrize('variant, kind', [('truncate', 'writers'), ('ftruncate', 'descriptors')])
def test_truncate_in_permitted_child_preserves_forbidden_canary(variant, kind, scenario, tmp_path):
    context = guard.ensure_context(ROOT)
    issued = tmp_path / 'issued'
    issued.mkdir()
    case = variant + '-' + scenario
    command = [sys.executable, '-I', '-B', '-X', 'utf8',
               str(ROOT / 'tools/offline_guard.py'), 'control', case]
    result = guard.run_child(command, role='f3-control', roots=[issued], timeout=30)
    assert result.actual_exit == 0, result.stderr.decode('utf-8')
    assert result.returncode != 0 and not result.validated
    assert result.report_errors == ['UNEXPECTED_COUNTERS']
    assert result.report['counters'] == {kind: 1}
    assert result.report['violations'] == 1
    assert result.report['role'] == 'f3-control'
    assert result.report['run_id'] == context.run_id
    assert result.report['tested_sha'] == context.tested_sha
    assert result.report['source_digest'] == context.source_digest
    proof = json.loads(result.stdout.decode('utf-8').splitlines()[-1])
    assert proof['case'] == case and proof['caught']
    assert proof['leaf_calls'] == 0
    assert proof['canary_preserved']
    expected = b'F3_CHILD_TRUNCATE_CANARY_MUST_SURVIVE\x00\xff\n'
    assert proof['before_size'] == proof['after_size'] == 40
    assert proof['before_sha256'] == proof['after_sha256']
    identifier = result.ticket['ticket_id']
    relative = {
        'external': 'external-canary.bin',
        'source': 'synthetic-source/sample.py',
        'previous': '.f3/evidence/runs/old/p.json',
        'alias': 'external-canary.bin',
    }[scenario]
    sentinel = issued.parent / ('r-' + identifier[:8]) / relative
    assert _canary_snapshot(sentinel) == (expected, 40, hashlib.sha256(expected).hexdigest())
    assert proof['after_sha256'] == hashlib.sha256(expected).hexdigest()
    journal = context.report_dir / 'processes' / (identifier + '.violation.1.json')
    violation = json.loads(journal.read_text(encoding='utf-8'))
    assert violation['kind'] == kind and violation['ticket_id'] == identifier
    assert violation['run_id'] == context.run_id


def test_real_report_allocator_and_exclusive_writer_preserve_previous_runs(monkeypatch, tmp_path):
    replica = tmp_path / 'evidence-replica'
    replica.mkdir()
    monkeypatch.delenv('MDS_OFFLINE_REPORT_ROOT', raising=False)
    full_first = guard.report_root(replica, 'f3')
    full_second = guard.report_root(replica, 'f3')
    collect_only = guard.report_root(replica, 'f3')
    assert len({full_first, full_second, collect_only}) == 3
    first_report = full_first / 'pytest.json'
    second_report = full_second / 'pytest.json'
    guard.exclusive_json(first_report, {'run_id': full_first.name, 'mode': 'run'})
    guard.exclusive_json(second_report, {'run_id': full_second.name, 'mode': 'run'})
    previous = {path: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in (first_report, second_report)}
    guard.exclusive_json(collect_only / 'pytest.json',
                         {'run_id': collect_only.name, 'mode': 'collect'})
    with pytest.raises(FileExistsError):
        guard.exclusive_json(first_report, {'mode': 'collect', 'run_id': collect_only.name})
    assert {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in previous} == previous


def test_preflight_old_report_refusal_creates_no_replacement(monkeypatch, tmp_path):
    replica = tmp_path / 'preflight-replica'
    replica.mkdir()
    monkeypatch.delenv('MDS_OFFLINE_REPORT_ROOT', raising=False)
    old_root = guard.report_root(replica, 'f3')
    old_report = old_root / 'pytest.json'
    guard.exclusive_json(old_report, {'run_id': old_root.name, 'mode': 'run', 'exit_code': 0})
    before = old_report.read_bytes()
    before_hash = hashlib.sha256(before).hexdigest()
    names = set(old_root.iterdir())
    monkeypatch.setenv('MDS_OFFLINE_REPORT_ROOT', str(old_root))
    with pytest.raises(ValueError):
        guard.report_root(replica, 'f3')
    assert set(old_root.iterdir()) == names
    assert old_report.read_bytes() == before
    assert hashlib.sha256(old_report.read_bytes()).hexdigest() == before_hash


@pytest.mark.parametrize('lane, relative, container', [
    ('.f2', 'tests/f2/conftest.py', '_reports'),
    ('.f-log', 'tests/logging/conftest.py', 'REPORTS'),
])
def test_reviewed_native_fixed_report_destination_reproduces_overwrite(lane, relative, container, tmp_path):
    """Red-before is the real original callback; no missing-new-helper oracle."""
    from types import SimpleNamespace
    git = shutil.which('git')
    assert git is not None
    command = [git, 'show', REVIEWED_BASE + ':' + relative]
    ticket = guard.issue_ticket('native-git', command, native=True)
    original = guard.run_child(command, role='native-git', ticket=ticket, timeout=10)
    assert original.returncode == 0 and original.validated
    source = original.stdout.decode('utf-8')
    callbacks = [node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef)
                 and node.name in {'pytest_runtest_logreport', 'pytest_sessionfinish'}]
    assert len(callbacks) == 2
    helper_command = [git, 'show', REVIEWED_BASE + ':' + relative.replace('conftest.py', '_probe.py')]
    helper_ticket = guard.issue_ticket('native-git', helper_command, native=True)
    helper_result = guard.run_child(helper_command, role='native-git', ticket=helper_ticket, timeout=10)
    assert helper_result.returncode == 0 and helper_result.validated
    helper_source = helper_result.stdout.decode('utf-8')
    helpers = [node for node in ast.parse(helper_source).body
               if isinstance(node, ast.FunctionDef) and node.name == 'safe_tree']
    assert len(helpers) == 1
    # A fixture module has the original root calculation and unchanged callback
    # bodies. No live module/ROOT/handler/Path writer is patched or replaced.
    replica = tmp_path / 'reviewed-replica'
    destination = replica / relative
    destination.parent.mkdir(parents=True)
    prefix = ('import datetime,json,os,stat,sys\nfrom pathlib import Path\n'
              'ROOT=Path(__file__).resolve().parents[2]\n'
              f'{container}=[]\nGUARDS={{}}\n')
    original_helper = ast.get_source_segment(helper_source, helpers[0])
    destination.write_text(prefix + original_helper + '\n\n' +
                           '\n\n'.join(ast.get_source_segment(source, node) for node in callbacks),
                           encoding='utf-8')
    fixture = runpy.run_path(str(destination))
    report = SimpleNamespace(nodeid='fixture::first', when='call', outcome='passed',
                             duration=0.01, user_properties=[])
    fixture['pytest_runtest_logreport'](report)
    fixture['pytest_sessionfinish'](None, 0)
    evidence = replica / lane / 'evidence/pytest.json'
    first = evidence.read_bytes()
    report.nodeid = 'fixture::second'
    fixture['pytest_runtest_logreport'](report)
    fixture['pytest_sessionfinish'](None, 0)
    assert evidence.read_bytes() != first
    assert len(json.loads(evidence.read_text(encoding='utf-8'))['reports']) == 2
    fixture['pytest_sessionfinish'](None, 5)
    assert json.loads(evidence.read_text(encoding='utf-8'))['exit_code'] == 5
    assert evidence.read_bytes() != first
