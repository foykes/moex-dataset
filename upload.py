# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
"""Publication targets and Linux parent-owned supervision.

Imports are inert. Private tickets contain source paths and are not diagnostic
exports. Public receipts contain only bounded logical output identities.
"""
from contextlib import contextmanager, nullcontext
import datetime
import gc
import hashlib
import io
import json
import os
from pathlib import Path
import pickle
import re
import select
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid


current_path = sys.path[0]
secrets_prefix = current_path.rsplit('/', 1)[0] + '/secrets/'
gdoc_to_write = 'https://docs.google.com/spreadsheets/d/1HXXoxcDVqIrWN6QEg5ij88AxcNAKT-G-xm2UTUfQe1Q/edit?usp=sharing'
datasets_path = current_path + '/datasets/'
start_time = time.time()

_D2_LIMITS = dict(max_ftp_outputs=512, max_checkpoint_bytes=65536,
                  max_result_bytes=131072, max_run_bytes=1048576)
_D2_PROFILE = None
_D2_ACTIVE = None
_D2_PENDING = None
_D2_SCOPE_LOCK = threading.Lock()
_D2_UNSETTLED = []
_D2_WORKER_ROLE = None
_D2_WORKER_ROOT = None
_D2_TRACKER_OWNER = None
_D2_TRACKER_AUDIT_INSTALLED = False
D2_TEST_CASES = (
    'success', 'google_error', 'ftp_error', 'exit23', 'system_exit0',
    'system_exit_string', 'abnormal_before_ready', 'abnormal_after_final',
    'normal_without_final', 'final_then_exit23', 'start_google_before',
    'start_ftp_before', 'start_google_after_native', 'start_ftp_after_native',
    'constructor_error', 'hang_google', 'hang_ftp', 'cleanup_unsettled',
    'partial_google', 'partial_ftp', 'checkpoint_missing', 'checkpoint_corrupt',
    'checkpoint_oversize', 'checkpoint_stale_run', 'checkpoint_stale_pid',
    'checkpoint_stale_sequence', 'listener_failure', 'ack_failure',
    'bootstrap_replay_failure', 'bootstrap_import_failure',
    'bootstrap_unpickle_failure', 'foreign_fd', 'canary_positive_control',
    'parent_finalization_raises', 'parent_finalization_incomplete',
    'postseal_cleanup_failure', 'cancel_google', 'cancel_ftp',
    'ticket_ftp_failure', 'pidfd_google_failure', 'identity_google_failure',
    'registry_google_failure', 'registration_google_failure',
    'registration_ftp_failure', 'bootstrap_pipe_failure', 'replace_failure', 'tracker_death',
    'preupload_stage_failure', 'post_context_bind_failure', 'serialization_write_failure')


def _d2_error(code):
    return RuntimeError(code)


def _d2_bytes(document):
    return (json.dumps(document, ensure_ascii=True, allow_nan=False,
        separators=(',', ':'), sort_keys=True) + '\n').encode('utf-8')


def _d2_clone(document):
    return json.loads(_d2_bytes(document))


def _d2_fdopen(descriptor, mode):
    try:
        return os.fdopen(descriptor, mode)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise


def _d2_validate_limits(diagnostic_limits=None):
    limits = dict(_D2_LIMITS)
    if diagnostic_limits is not None:
        if type(diagnostic_limits) is not dict or set(diagnostic_limits) - set(limits):
            raise _d2_error('D2_LIMITS_INVALID')
        for key, value in diagnostic_limits.items():
            if type(value) is not int or not 0 < value <= limits[key]:
                raise _d2_error('D2_LIMITS_INVALID')
            limits[key] = value
    return limits


def _d2_identity(info):
    return [info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns]


def _d2_fd_identity(descriptor):
    info = os.fstat(descriptor)
    return (info.st_dev, info.st_ino, info.st_mode, info.st_rdev)


def _d2_pidfd_pid(descriptor):
    fields = Path('/proc/self/fdinfo/' + str(descriptor)).read_text(encoding='ascii').splitlines()
    values = [line.split(':', 1)[1].strip() for line in fields if line.startswith('Pid:')]
    if len(values) != 1 or not re.fullmatch('-?[0-9]+', values[0]):
        raise _d2_error('D2_PIDFD_IDENTITY')
    return int(values[0])


def _d2_checked_path(path, *, directory=False):
    path = Path(os.path.abspath(path))
    for component in reversed((path, *path.parents)):
        info = component.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise _d2_error('D2_SOURCE_ALIAS')
        if component != path and not stat.S_ISDIR(info.st_mode):
            raise _d2_error('D2_SOURCE_TYPE')
    info = path.lstat()
    if directory:
        valid = stat.S_ISDIR(info.st_mode)
    else:
        valid = stat.S_ISREG(info.st_mode) and info.st_nlink == 1
    if not valid:
        raise _d2_error('D2_SOURCE_TYPE')
    return path


def _d2_source_item(path, output_id, group, destination):
    path = _d2_checked_path(path)
    expected = _d2_identity(path.lstat())
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    with _d2_fdopen(descriptor, 'rb') as handle:
        before = os.fstat(handle.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                or _d2_identity(before) != expected):
            raise _d2_error('D2_SOURCE_TYPE')
        digest = hashlib.file_digest(handle, 'sha256').hexdigest()
        if _d2_identity(before) != _d2_identity(os.fstat(handle.fileno())):
            raise _d2_error('D2_SOURCE_CHANGED')
    return dict(source_path=str(path), output_id=output_id, group=group,
                destination=destination, identity=_d2_identity(before), sha256=digest)


def _d2_check_source_handle(handle, item):
    info = os.fstat(handle.fileno())
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or _d2_identity(info) != item['identity']):
        raise _d2_error('D2_SOURCE_CHANGED')
    position = handle.tell()
    handle.seek(0)
    digest = hashlib.file_digest(handle, 'sha256').hexdigest()
    handle.seek(position)
    if digest != item['sha256']:
        raise _d2_error('D2_SOURCE_CHANGED')


def _d2_check_source(item):
    path = _d2_checked_path(item['source_path'])
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    with _d2_fdopen(descriptor, 'rb') as handle:
        _d2_check_source_handle(handle, item)


def _d2_build_source_plan(current_path=None):
    import unicodedata
    # Preserve the two historical bindings; isolated workers receive this plan.
    if current_path is not None:
        if (type(current_path) is not str or not os.path.isabs(current_path)
                or '\\' in current_path or str(Path(os.path.abspath(current_path))) != current_path):
            raise _d2_error('D2_SOURCE_BINDING_UNSUPPORTED')
        _d2_checked_path(current_path, directory=True)
    root = _d2_checked_path(datasets_path, directory=True)
    plan = dict(google=[_d2_source_item(root / '10years_data_1d_interval.csv',
        'google-000001', 'google', None)], ftp=[], directories=[])
    if plan['google'][0]['identity'][3] == 0:
        raise _d2_error('D2_GOOGLE_SOURCE_EMPTY')
    identities = set()
    destinations = set()
    for group, suffix in (('root', ''), ('dividends', 'dividends'),
                          ('ticker_lists', 'ticker_lists')):
        directory = _d2_checked_path(root / suffix, directory=True)
        before = directory.stat()
        with os.scandir(directory) as scan:
            entries = sorted(list(scan), key=lambda entry: entry.name)
        for entry in entries:
            if '/' in entry.name or '\\' in entry.name or any(ord(character) < 32 for character in entry.name):
                raise _d2_error('D2_SOURCE_NAME_UNSUPPORTED')
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                continue  # The legacy scan is shallow, including dohod.ru.
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise _d2_error('D2_SOURCE_ALIAS')
            if (info.st_dev, info.st_ino) in identities:
                raise _d2_error('D2_SOURCE_ALIAS')
            identities.add((info.st_dev, info.st_ino))
            relative = entry.name if not suffix else suffix + '/' + entry.name
            canonical = unicodedata.normalize('NFC', relative).casefold()
            if canonical in destinations:
                raise _d2_error('D2_SOURCE_DESTINATION_ALIAS')
            destinations.add(canonical)
            output_id = 'ftp-' + format(len(plan['ftp']) + 1, '06d')
            plan['ftp'].append(_d2_source_item(entry.path, output_id, group, relative))
            if len(plan['ftp']) > _D2_LIMITS['max_ftp_outputs']:
                raise _d2_error('D2_OUTPUT_LIMIT')
        if _d2_identity(before) != _d2_identity(directory.stat()):
            raise _d2_error('D2_SOURCE_CHANGED')
        plan['directories'].append(dict(path=str(directory), identity=_d2_identity(before),
            filenames=[entry.name for entry in entries]))
    return plan


def _d2_recheck_source_plan(plan):
    for directory in plan['directories']:
        path = _d2_checked_path(directory['path'], directory=True)
        with os.scandir(path) as scan:
            names = sorted(entry.name for entry in scan)
        if names != directory['filenames'] or _d2_identity(path.stat()) != directory['identity']:
            raise _d2_error('D2_SOURCE_CHANGED')
    for role in ('google', 'ftp'):
        for item in plan[role]:
            _d2_check_source(item)


def _d2_outputs(items):
    return [dict(output_id=item['output_id'], group=item['group'],
                 transfer_state='NOT_ATTEMPTED', commit_state='NOT_ATTEMPTED')
            for item in items]


def _d2_empty_worker(worker_id, items):
    return dict(worker_id=worker_id, required=True, started=False, pid=None,
        exitcode=None, settled=False, alive=False, timed_out=False,
        cleanup_confirmed=False, checkpoint_state='MISSING', execution_state='NOT_STARTED',
        normal_return_observed=False, diagnostics_state='NOT_OBSERVED',
        error_code=None, outputs=_d2_outputs(items), birth_attempted=False,
        process_identity=None, pidfd_bound=False, reaped=False)


def _d2_result(run_id, attempt_id, producer_sha, plan):
    return dict(kind='D2_RESULT', receipt_version=1, run_id=run_id,
        attempt_id=attempt_id, producer_sha=producer_sha, artifact_id='d2-results-' + attempt_id,
        receipt_scope='PREFINAL_SUPERVISION', finalization_state='NOT_OBSERVED',
        diagnostic_success_allowed=False, diagnostics_state='NOT_OBSERVED',
        final_exit_status=None, storage_sealed='UNKNOWN', parent_outcome='UNKNOWN',
        error_code=None, failed_required_channels=[],
        workers={role: _d2_empty_worker(role, plan[role]) for role in ('google', 'ftp')},
        committed_outputs=[], uncommitted_outputs=[], unknown_outputs=[])


def _d2_public_result(result):
    return {key: value for key, value in result.items() if not key.startswith('_')}


def _d2_worker_ok(row):
    return (type(row) is dict and row.get('required') is True and row.get('started') is True
        and type(row.get('pid')) is int and row['pid'] > 0 and row.get('settled') is True
        and row.get('alive') is False and type(row.get('exitcode')) is int and row['exitcode'] == 0
        and row.get('timed_out') is False and row.get('cleanup_confirmed') is True
        and row.get('checkpoint_state') == 'VALID' and row.get('execution_state') == 'RETURNED'
        and row.get('normal_return_observed') is True and row.get('diagnostics_state') == 'COMPLETE'
        and row.get('error_code') is None
        and all(output['transfer_state'] == 'ACKNOWLEDGED' for output in row['outputs']))


def _d2_workers_ok(result):
    workers = result.get('workers')
    return (type(workers) is dict and set(workers) == {'google', 'ftp'}
        and all(_d2_worker_ok(row) for row in workers.values())
        and workers['google']['pid'] != workers['ftp']['pid'])


def _d2_aggregate(result):
    result['failed_required_channels'] = [role for role, row in result['workers'].items()
                                         if not _d2_worker_ok(row)]
    result['committed_outputs'] = []
    result['uncommitted_outputs'], result['unknown_outputs'] = [], []
    for row in result['workers'].values():
        for output in row['outputs']:
            key = 'uncommitted_outputs' if output['transfer_state'] == 'NOT_ATTEMPTED' else 'unknown_outputs'
            result[key].append(output['output_id'])


def _d2_private_directory(path):
    path = _d2_checked_path(path, directory=True)
    info = path.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise _d2_error('D2_PRIVATE_DIRECTORY')
    return path


def _d2_read_json(path, limit):
    path = Path(path)
    if _D2_WORKER_ROLE in ('google', 'ftp'):
        allowed = {_D2_WORKER_ROOT / 'plan.json',
                   _D2_WORKER_ROOT / _D2_WORKER_ROLE / 'checkpoint.json'}
        if path not in allowed:
            raise _d2_error('D2_CHECKPOINT_READER_ROLE')
    runtime = _D2_ACTIVE or _D2_PENDING
    if runtime is not None and runtime.go_released:
        for item in runtime.children:
            if path == runtime.root / item['role'] / 'checkpoint.json':
                if item['process'] is None or item['process'].poll() is None:
                    raise _d2_error('D2_CHECKPOINT_WORKER_LIVE')
    _d2_private_directory(path.parent)
    leaf = path.lstat()
    if not stat.S_ISREG(leaf.st_mode) or leaf.st_nlink != 1:
        raise _d2_error('D2_CHECKPOINT_INVALID')
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    with _d2_fdopen(descriptor, 'rb') as handle:
        info = os.fstat(handle.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or info.st_size > limit or _d2_identity(info) != _d2_identity(leaf)):
            raise _d2_error('D2_CHECKPOINT_INVALID')
        data = handle.read(limit + 1)
        if len(data) > limit or len(data) != info.st_size:
            raise _d2_error('D2_CHECKPOINT_INVALID')
        if _d2_identity(info) != _d2_identity(os.fstat(handle.fileno())):
            raise _d2_error('D2_CHECKPOINT_CHANGED')
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                raise _d2_error('D2_CHECKPOINT_INVALID')
            value[key] = item
        return value
    return json.loads(data, object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(_d2_error('D2_CHECKPOINT_INVALID')))


def _d2_atomic_json(root, name, document, limit, limits=None):
    if _D2_WORKER_ROLE in ('google', 'ftp') and (
            Path(root) != _D2_WORKER_ROOT / _D2_WORKER_ROLE or name != 'checkpoint.json'):
        raise _d2_error('D2_CHECKPOINT_WRITER_ROLE')
    limits = _d2_validate_limits(limits)
    root = _d2_private_directory(root)
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', name) or name.endswith('.pending'):
        raise _d2_error('D2_CHECKPOINT_NAME')
    data = _d2_bytes(document)
    if len(data) > limit:
        raise _d2_error('D2_CHECKPOINT_TOO_LARGE')
    destination, temporary = root / name, root / (name + '.pending')
    try:
        destination.lstat()
    except FileNotFoundError:
        pass
    else:
        existing = _d2_checked_path(destination).lstat()
        if existing.st_size > limit:
            # Preserve invalid prior evidence; a smaller replacement must not
            # silently erase a file that already exceeded its own slot bound.
            raise _d2_error('D2_CHECKPOINT_TOO_LARGE')
    # Scope-local accounting does not inspect a sibling's mutable slot. The
    # parent separately reserves both slots before GO and audits the whole run
    # only after settlement. Existing pending bytes remain owned evidence.
    with os.scandir(root) as entries:
        entries = list(entries)
    present = 0
    for entry in entries:
        info = entry.stat(follow_symlinks=False)
        if stat.S_ISREG(info.st_mode):
            if info.st_nlink != 1:
                raise _d2_error('D2_CHECKPOINT_ALIAS')
            present += info.st_size
        elif not stat.S_ISDIR(info.st_mode):
            raise _d2_error('D2_CHECKPOINT_ALIAS')
    if present + len(data) > limits['max_run_bytes']:
        raise _d2_error('D2_RUN_TOO_LARGE')
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    with _d2_fdopen(descriptor, 'wb') as handle:
        handle.write(data)
        handle.flush()
    # Failure preserves prior destination and visible pending evidence. No retry.
    os.replace(temporary, destination)
    return len(data)


def _d2_slot_atomic_json(root, worker_id, document, limits):
    if (_D2_WORKER_ROLE is not None and worker_id != _D2_WORKER_ROLE
            or worker_id not in ('google', 'ftp') or document['worker_id'] != worker_id):
        raise _d2_error('D2_CHECKPOINT_ROLE')
    return _d2_atomic_json(Path(root) / worker_id, 'checkpoint.json', document,
                           limits['max_checkpoint_bytes'], limits)


def _d2_checkpoint(run_id, attempt_id, producer_sha, worker_id, items):
    return dict(kind='D2_WORKER_CHECKPOINT', receipt_version=1, run_id=run_id,
        attempt_id=attempt_id, producer_sha=producer_sha, worker_id=worker_id,
        pid=os.getpid(), sequence=1, execution_state='READY',
        normal_return_observed=False, diagnostics_state='ACTIVE',
        lock_state='NOT_USED', error_code=None, outputs=_d2_outputs(items))


def _d2_validate_checkpoint_record(record, worker_id, *, run_id, attempt_id, pid,
        outputs, minimum_sequence=0, producer_sha=None):
    keys = {'kind', 'receipt_version', 'run_id', 'attempt_id', 'producer_sha', 'worker_id',
        'pid', 'sequence', 'execution_state', 'normal_return_observed', 'diagnostics_state',
        'lock_state', 'error_code', 'outputs'}
    if (type(record) is not dict or set(record) != keys or record['kind'] != 'D2_WORKER_CHECKPOINT'
            or type(record['receipt_version']) is not int or record['receipt_version'] != 1
            or record['run_id'] != run_id or record['attempt_id'] != attempt_id
            or record['worker_id'] != worker_id or type(record['pid']) is not int or record['pid'] != pid
            or type(record['sequence']) is not int or not max(1, minimum_sequence) <= record['sequence'] <= 2 * len(outputs) + 8
            or record['execution_state'] not in ('READY', 'RUNNING', 'RETURNED', 'FAILED', 'INTERRUPTED')
            or type(record['normal_return_observed']) is not bool
            or record['diagnostics_state'] not in ('ACTIVE', 'COMPLETE', 'INCOMPLETE')
            or record['lock_state'] != 'NOT_USED'
            or not isinstance(record['producer_sha'], str) or not re.fullmatch('[0-9a-f]{7,40}', record['producer_sha'])
            or producer_sha is not None and record['producer_sha'] != producer_sha
            or record['error_code'] is not None and record['error_code'] not in
                ('D2_CHILD_FAILED', 'D2_INTERRUPTED', 'D2_GOOGLE_FAILED', 'D2_FTP_FAILED', 'D2_LOGGING_INCOMPLETE')
            or type(record['outputs']) is not list or len(record['outputs']) != len(outputs)):
        raise _d2_error('D2_CHECKPOINT_INVALID')
    for actual, expected in zip(record['outputs'], outputs):
        if (type(actual) is not dict or set(actual) != {'output_id', 'group', 'transfer_state', 'commit_state'}
                or actual['output_id'] != expected['output_id'] or actual['group'] != expected['group']
                or actual['transfer_state'] not in ('NOT_ATTEMPTED', 'UNKNOWN', 'ACKNOWLEDGED')
                or actual['commit_state'] != ('NOT_ATTEMPTED' if actual['transfer_state'] == 'NOT_ATTEMPTED' else 'UNKNOWN')):
            raise _d2_error('D2_CHECKPOINT_INVALID')
    if record['normal_return_observed'] and record['execution_state'] not in ('RETURNED', 'FAILED', 'INTERRUPTED'):
        raise _d2_error('D2_CHECKPOINT_INVALID')
    if (record['execution_state'] == 'RETURNED' and not record['normal_return_observed']
            or record['diagnostics_state'] != 'ACTIVE'
            and record['execution_state'] not in ('RETURNED', 'FAILED', 'INTERRUPTED')):
        raise _d2_error('D2_CHECKPOINT_INVALID')
    return record


def _d2_read_checkpoint(root, worker_id, *, run_id, attempt_id, pid, outputs,
        producer_sha=None, minimum_sequence=0, previous=None):
    if _D2_WORKER_ROLE in ('google', 'ftp') and worker_id != _D2_WORKER_ROLE:
        raise _d2_error('D2_CHECKPOINT_READER_ROLE')
    runtime = _D2_ACTIVE or _D2_PENDING
    if runtime is not None and Path(root) == runtime.root and runtime.go_released:
        item = next((item for item in runtime.children if item['role'] == worker_id), None)
        if item is None or item['process'] is None or item['process'].poll() is None:
            raise _d2_error('D2_CHECKPOINT_WORKER_LIVE')
    limit = runtime.limits['max_checkpoint_bytes'] if runtime is not None and Path(root) == runtime.root else _D2_LIMITS['max_checkpoint_bytes']
    record = _d2_validate_checkpoint_record(
        _d2_read_json(Path(root) / worker_id / 'checkpoint.json', limit),
        worker_id, run_id=run_id, attempt_id=attempt_id, pid=pid, outputs=outputs,
        minimum_sequence=minimum_sequence, producer_sha=producer_sha)
    if previous is not None:
        if (record['sequence'] < previous['sequence']
                or record['sequence'] == previous['sequence'] and record != previous):
            raise _d2_error('D2_CHECKPOINT_STALE')
        ranks = {'NOT_ATTEMPTED': 0, 'UNKNOWN': 1, 'ACKNOWLEDGED': 2}
        if any(ranks[now['transfer_state']] < ranks[old['transfer_state']]
               for now, old in zip(record['outputs'], previous['outputs'])):
            raise _d2_error('D2_CHECKPOINT_STALE')
        terminal = ('RETURNED', 'FAILED', 'INTERRUPTED')
        if (previous['execution_state'] in terminal and record != previous
                or previous['execution_state'] == 'RUNNING' and record['execution_state'] == 'READY'
                or previous['normal_return_observed'] and not record['normal_return_observed']
                or previous['diagnostics_state'] != 'ACTIVE' and record['diagnostics_state'] != previous['diagnostics_state']
                or previous['error_code'] is not None and record['error_code'] != previous['error_code']):
            raise _d2_error('D2_CHECKPOINT_STALE')
    return record


def _d2_run_inventory(root, limits, *, settled=False, required=()):
    if _D2_WORKER_ROLE in ('google', 'ftp') or not settled:
        raise _d2_error('D2_INVENTORY_LIVE_WORKER')
    runtime = _D2_ACTIVE or _D2_PENDING
    if runtime is not None and Path(root) == runtime.root:
        if any(item['birth_attempted'] and (item['process'] is None
               or item['process'].poll() is None) for item in runtime.children):
            raise _d2_error('D2_INVENTORY_LIVE_WORKER')
    root = _d2_private_directory(root)
    total = 0
    allowed = {'plan.json', 'tracker.ticket.json', 'google.ticket.json', 'ftp.ticket.json',
               'tracker.ticket.json.claim', 'google.ticket.json.claim', 'ftp.ticket.json.claim',
               'google/checkpoint.json', 'ftp/checkpoint.json', 'parent-result.json', 'process-registry.json'}
    seen = set()
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in dirs:
            if (Path(directory) / name).relative_to(root).as_posix() not in ('google', 'ftp'):
                raise _d2_error('D2_INVENTORY_INVALID')
            _d2_private_directory(Path(directory) / name)
        for name in files:
            relative = (Path(directory) / name).relative_to(root).as_posix()
            if relative not in allowed or name.endswith('.pending'):
                raise _d2_error('D2_INVENTORY_INVALID')
            seen.add(relative)
            path = _d2_checked_path(Path(directory) / name)
            total += path.stat().st_size
            if total > limits['max_run_bytes']:
                raise _d2_error('D2_RUN_TOO_LARGE')
    if not set(required) <= seen:
        raise _d2_error('D2_INVENTORY_MISSING')
    return total


# %%
def _d2_gdoc_upload(current_path, gdoc_to_write, *, source_plan=None, output_observer=None):
    import pandas as pd
    import pygsheets
    prefix = current_path.rsplit('/', 1)[0] + '/secrets/'
    gc_client = pygsheets.authorize(service_file=prefix + '/python-304621-1e403209f05f.json')
    if source_plan is None:
        df = pd.read_csv(datasets_path + '10years_data_1d_interval.csv')
        item = None
    else:
        item = source_plan[0]
        descriptor = os.open(item['source_path'], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        with _d2_fdopen(descriptor, 'rb') as source:
            _d2_check_source_handle(source, item)
            df = pd.read_csv(source)
            _d2_check_source_handle(source, item)
    white_list_columns = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker', 'RSI14']
    df.drop([column for column in df.columns if column not in white_list_columns], axis=1, inplace=True)
    document = gc_client.open_by_url(gdoc_to_write)
    sheet = document[0]
    if item is not None:
        _d2_check_source(item)
    if output_observer is not None:
        output_observer(item['output_id'], 'UNKNOWN')
    sheet.clear()
    sheet.set_dataframe(df, 'A1', copy_index=False, copy_head=True, fit=False,
                        escape_formulae=False, nan='NaN')
    if item is not None:
        _d2_check_source(item)
    if output_observer is not None:
        output_observer(item['output_id'], 'ACKNOWLEDGED')


def gdoc_upload(current_path, gdoc_to_write, *, source_plan=None, output_observer=None):
    return _d2_gdoc_upload(current_path, gdoc_to_write,
        source_plan=source_plan, output_observer=output_observer)


def _d2_ftp_upload(current_path, *, source_plan=None, output_observer=None):
    import ftplib
    prefix = current_path.rsplit('/', 1)[0] + '/secrets/'
    with open(prefix + 'moex.foykes.com.json', encoding='utf-8') as handle:
        credentials = json.load(handle)
    server = ftplib.FTP(credentials['domain'], credentials['username'], credentials['password'])
    server.encoding = 'utf-8'
    if source_plan is None:
        source_plan = _d2_build_source_plan(current_path)['ftp']
    try:
        for item in source_plan:
            descriptor = os.open(item['source_path'], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
            with _d2_fdopen(descriptor, 'rb') as source:
                _d2_check_source_handle(source, item)
                if output_observer is not None:
                    output_observer(item['output_id'], 'UNKNOWN')
                server.storbinary('STOR ' + item['destination'], source)
                _d2_check_source_handle(source, item)
                if output_observer is not None:
                    output_observer(item['output_id'], 'ACKNOWLEDGED')
        server.quit()
    except BaseException:
        try:
            server.close()
        except BaseException:
            pass
        raise
    else:
        server.close()


def ftp_upload(current_path, *, source_plan=None, output_observer=None):
    return _d2_ftp_upload(current_path, source_plan=source_plan, output_observer=output_observer)


# %%
def _d2_process_identity(pid):
    fields = Path('/proc/' + str(pid) + '/stat').read_text(encoding='ascii').rsplit(')', 1)[1].split()
    return dict(start_ticks=int(fields[19]), parent_pid=int(fields[1]), session=int(fields[3]))


def _d2_tracker_audit(event, args):
    """Only the pinned native resource-tracker launch is forbidden here."""
    if _D2_TRACKER_OWNER is None or event != '_posixsubprocess.fork_exec' or len(args) < 2:
        return
    command = args[1]
    if (type(command) in (tuple, list) and len(command) >= 3
            and command[-2] in ('-c', b'-c')):
        code = command[-1]
        if isinstance(code, bytes):
            code = code.decode('ascii', errors='replace')
        if isinstance(code, str) and re.fullmatch(r'from multiprocessing\.resource_tracker import main;main\([0-9]+\)', code):
            _D2_TRACKER_OWNER.tracker_failed = True
            raise _d2_error('D2_TRACKER_RELAUNCH_REFUSED')


def _d2_check_tracker(runtime):
    if not runtime.owns_tracker:
        return
    item = runtime.tracker
    process = item['process']
    try:
        valid = (process is not None and process.poll() is None
            and runtime.tracker_state._fd == runtime.tracker_write
            and runtime.owned_fds.get(runtime.tracker_write) == _d2_fd_identity(runtime.tracker_write)
            and item.get('pidfd_identity') == _d2_fd_identity(item['pidfd'])
            and _d2_pidfd_pid(item['pidfd']) == process.pid
            and _d2_process_identity(process.pid) == item['identity'])
    except (OSError, ValueError, RuntimeError, TypeError):
        valid = False
    if not valid:
        runtime.tracker_failed = True
        raise _d2_error('D2_TRACKER_UNAVAILABLE')


def _d2_guard_owned_tracker(runtime, *, initializing=False):
    global _D2_TRACKER_OWNER, _D2_TRACKER_AUDIT_INSTALLED
    if not runtime.owns_tracker:
        return
    if not _D2_TRACKER_AUDIT_INSTALLED:
        sys.addaudithook(_d2_tracker_audit)
        _D2_TRACKER_AUDIT_INSTALLED = True
    _D2_TRACKER_OWNER = runtime
    state = runtime.tracker_state
    names = ('_ensure_running_and_write', '_send', '_launch', '_teardown_dead_process')
    runtime.tracker_originals = {name: (name in state.__dict__, getattr(state, name)) for name in names}
    original_send = state._send
    def ensure_owned(message=None):
        # CPython 3.14.8 _send calls this primitive directly, not ensure_running.
        # Preserve its locked, atomic-write fast path without teardown/relaunch.
        with state._lock:
            if state._lock._recursion_count() > 1:
                if message is None:
                    raise state._reentrant_call_error()
                return state._reentrant_messages.append(message)
            _d2_check_tracker(runtime)
            try:
                state._write(state._make_probe_message() if message is None else message)
            except OSError:
                runtime.tracker_failed = True
                raise _d2_error('D2_TRACKER_UNAVAILABLE') from None
        while state._reentrant_messages:
            try:
                state._write(state._reentrant_messages.popleft())
            except OSError:
                runtime.tracker_failed = True
                raise _d2_error('D2_TRACKER_UNAVAILABLE') from None
    def send_owned(command, name, resource_type):
        process = runtime.tracker['process']
        proven_dead = process is not None and process.poll() is not None
        if proven_dead:
            runtime.tracker_failed = True
            if command == 'UNREGISTER':
                # SemLock._cleanup already unlinked its local semaphore. No
                # receiver exists; avoid util.Finalize printing a raw traceback.
                # This is failed cleanup bookkeeping, never a successful ACK.
                runtime.failed_unregisters += 1
                return
            raise _d2_error('D2_TRACKER_UNAVAILABLE')
        try:
            return original_send(command, name, resource_type)
        except RuntimeError:
            # A tracker can die between the preflight and its atomic write.
            # Suppress only the same proven-dead cleanup bookkeeping case.
            if command == 'UNREGISTER' and process is not None and process.poll() is not None:
                runtime.tracker_failed = True
                runtime.failed_unregisters += 1
                return
            raise
    def refuse_launch():
        runtime.tracker_failed = True
        raise _d2_error('D2_TRACKER_RELAUNCH_REFUSED')
    def refuse_teardown():
        runtime.tracker_failed = True
        # In particular, do not os.close an integer possibly reused by a caller.
        raise _d2_error('D2_TRACKER_TEARDOWN_REFUSED')
    runtime.tracker_wrappers = dict(_ensure_running_and_write=ensure_owned,
        _send=send_owned, _launch=refuse_launch, _teardown_dead_process=refuse_teardown)
    for name, wrapper in runtime.tracker_wrappers.items():
        setattr(state, name, wrapper)
    if not initializing:
        _d2_check_tracker(runtime)


def _d2_unguard_owned_tracker(runtime):
    global _D2_TRACKER_OWNER
    process = runtime.tracker['process']
    if ((process is None and runtime.tracker['birth_attempted'])
            or process is not None and process.poll() is None
            or runtime.tracker_write in runtime.owned_fds):
        raise _d2_error('D2_TRACKER_UNSETTLED')
    for name, wrapper in runtime.tracker_wrappers.items():
        if getattr(runtime.tracker_state, name) is not wrapper:
            runtime.fd_error = True
            raise _d2_error('D2_TRACKER_OWNERSHIP_CHANGED')
    for name, (present, original) in runtime.tracker_originals.items():
        if present:
            setattr(runtime.tracker_state, name, original)
        else:
            delattr(runtime.tracker_state, name)
    runtime.tracker_wrappers.clear()
    if _D2_TRACKER_OWNER is runtime:
        _D2_TRACKER_OWNER = None


def _d2_release_tracker_pipe(runtime, *, allow_unbound=False):
    descriptor = runtime.tracker_write
    if descriptor is None:
        return
    state = runtime.tracker_state
    if descriptor not in runtime.owned_fds:
        if state._fd is not None:
            runtime.fd_error = True
            raise _d2_error('D2_TRACKER_OWNERSHIP_CHANGED')
        return
    try:
        matches = _d2_fd_identity(descriptor) == runtime.owned_fds[descriptor]
    except OSError:
        matches = False
    if not matches or (state._fd != descriptor and not (allow_unbound and state._fd is None)):
        runtime.fd_error = True
        raise _d2_error('D2_TRACKER_OWNERSHIP_CHANGED')
    if state._fd == descriptor:
        state._fd = None
    runtime.close_fd(descriptor)


def _d2_fault(runtime, worker_id='parent'):
    if runtime.profile is not None and runtime.result is not None:
        rows = runtime.result.setdefault('_fault_witnesses', [])
        record = dict(case=runtime.case, worker_id=worker_id, pid=os.getpid(), count=1)
        if record not in rows:
            rows.append(record)


def _d2_source_sha(root):
    git = Path(root) / '.git'
    if git.is_file():
        git = (Path(root) / git.read_text(encoding='utf-8').strip().split(': ', 1)[1]).resolve()
    head = (git / 'HEAD').read_text(encoding='ascii').strip()
    if not head.startswith('ref: '):
        return head
    ref = head[5:]
    common = git
    if (git / 'commondir').exists():
        common = (git / (git / 'commondir').read_text(encoding='ascii').strip()).resolve()
    if (common / ref).exists():
        return (common / ref).read_text(encoding='ascii').strip()
    for line in (common / 'packed-refs').read_text(encoding='ascii').splitlines():
        if line.endswith(' ' + ref):
            return line.split(' ', 1)[0]
    raise _d2_error('D2_SOURCE_SHA_UNAVAILABLE')


@contextmanager
def _d2_test_profile(profile):
    global _D2_PROFILE
    from tools import offline_guard_linux as guard
    if (_D2_PROFILE is not None or _D2_ACTIVE is not None or _D2_PENDING is not None
            or _D2_SCOPE_LOCK.locked() or not guard.verify_d2_profile(profile)):
        raise _d2_error('D2_TEST_ADMISSION_REFUSED')
    _D2_PROFILE = _d2_clone(profile)
    try:
        yield
    finally:
        _D2_PROFILE = None


class _D2Runtime:
    """One parent owns launch handles; borrowed logging transports stay borrowed."""
    def __init__(self, logging_context, diagnostic_limits=None):
        self.parent_pid = os.getpid()
        self.thread = threading.get_ident()
        self.repo = Path(__file__).resolve().parent
        self.profile = _d2_clone(_D2_PROFILE) if _D2_PROFILE is not None else None
        self.case = self.profile['case'] if self.profile else 'production'
        self.limits = _d2_validate_limits(diagnostic_limits)
        if 2 * (2 * self.limits['max_checkpoint_bytes'] + self.limits['max_result_bytes']) > self.limits['max_run_bytes']:
            raise _d2_error('D2_RUN_TOO_LARGE')
        _d2_spawn_paths(self.repo)
        self.nonce = uuid.uuid4().hex
        self.context = logging_context
        self.owns_context = False
        self.active = True
        self.closed = False
        self.cleanup_complete = False
        self.transport_complete = False
        self.children = []
        self.bootstraps = []
        self.owned_fds = {}
        self.fd_error = False
        self.tracker = dict(role='tracker', process=None, pidfd=None, identity=None, birth_attempted=False)
        self.owns_tracker = False
        self.tracker_failed = False
        self.failed_unregisters = 0
        self.tracker_wrappers = {}
        self.tracker_originals = {}
        self.transport_cleanup_done = False
        self.tracker_write = None
        self.go_released = False
        self.deadline = None
        self.deadline_detected = False
        self.cleanup_requested = False
        self.cleanup_deadline = None
        self.cleanup_term_until = None
        self.result = None
        self.result_bound = False
        self.initialized = False
        base = Path(self.profile['scratch']) if self.profile else self.repo / '.f-log' / 'd2-private'
        base.mkdir(mode=0o700, parents=True, exist_ok=True)
        _d2_private_directory(base)
        self.root = base / ('run-' + self.nonce)
        self.root.mkdir(mode=0o700)
        for role in ('google', 'ftp'):
            (self.root / role).mkdir(mode=0o700)
        from multiprocessing import resource_tracker
        tracker_state = resource_tracker._resource_tracker
        self.tracker_state = tracker_state
        if tracker_state._fd is not None or tracker_state._pid is not None:
            if logging_context is None:
                raise _d2_error('D2_FOREIGN_TRACKER')
        else:
            self.owns_tracker = True
            _d2_guard_owned_tracker(self, initializing=True)
            read_end, write_end = self.pipe()
            self.tracker_write = write_end
            ticket = self.ticket('tracker', tracker_fd=read_end, descriptors={read_end})
            try:
                self.launch(ticket, {read_end}, self.tracker)
                tracker_state._fd = write_end
                _d2_check_tracker(self)
            finally:
                self.close_fd(read_end)
        self.initialized = True

    def pipe(self):
        pair = os.pipe()
        try:
            for descriptor in pair:
                if descriptor in self.owned_fds:
                    self.fd_error = True
                self.owned_fds[descriptor] = _d2_fd_identity(descriptor)
        except BaseException:
            self.fd_error = True
            for descriptor in pair:
                self.owned_fds.setdefault(descriptor, None)
                self.close_fd(descriptor)
            raise
        return pair

    def close_fd(self, descriptor):
        if descriptor in self.owned_fds:
            expected = self.owned_fds.pop(descriptor)
            try:
                current = _d2_fd_identity(descriptor)
            except OSError:
                self.fd_error = True
                return
            if current != expected:
                # The integer now belongs to another owner. Never close it.
                self.fd_error = True
                return
            try:
                os.close(descriptor)
            except OSError:
                self.fd_error = True

    def bind_context(self, context):
        if (not self.active or self.closed or self.parent_pid != os.getpid()
                or self.thread != threading.get_ident() or _D2_ACTIVE is not self):
            raise _d2_error('D2_RUNTIME_INACTIVE')
        if self.context is not None and context is not self.context:
            raise _d2_error('D2_CONTEXT_CHANGED')
        if context is not None:
            # A caller-created session already uses this owned transport.
            # Retain its custody and sink identity before later checks fail.
            self.context = context
            self.bind_result()
        _d2_check_tracker(self)
        if context is None:
            import run_logging
            context = run_logging.configure_logging(self.root.parent, run_id=uuid.uuid4().hex,
                environment='offline' if self.profile else 'local', producer_sha=_d2_source_sha(self.repo),
                config={'log_root': '.f-log/log-' + self.nonce, 'retained_runs': 1})
            self.owns_context = True
        self.context = context
        self.bind_result()
        if context['config']['shutdown_deadline_s'] > 10:
            raise _d2_error('D2_LOGGING_BUDGET_UNSUPPORTED')
        _d2_check_tracker(self)
        return context

    def bind_result(self):
        if self.result is None or self.context is None:
            return
        if type(self.result) is not dict:
            raise _d2_error('D2_RESULT_SINK_INVALID')
        if not self.result_bound:
            # No publication worker exists at this boundary. These rows say
            # NOT_STARTED, not a synthetic worker completion or OS outcome.
            self.result.update(_d2_result(self.context['run_id'], uuid.uuid4().hex,
                self.context['producer_sha'], {'google': [], 'ftp': []}))
            self.result_bound = True
        self.result['_runtime'] = self

    def ticket(self, role, *, tracker_fd, descriptors, bootstrap_fd=None,
               ready_fd=None, go_fd=None, witness_fd=None, plan_sha256=None):
        source = {name: hashlib.sha256((self.repo / name).read_bytes()).hexdigest()
                  for name in ('upload.py', 'run_logging.py')}
        value = dict(schema='d2-worker-ticket-v1', role=role, repo_root=str(self.repo),
            run_root=str(self.root), runtime_nonce=self.nonce, parent_pid=self.parent_pid,
            parent_start_ticks=_d2_process_identity(self.parent_pid)['start_ticks'],
            source_files_sha256=source, tracker_fd=tracker_fd, bootstrap_fd=bootstrap_fd,
            ready_fd=ready_fd, go_fd=go_fd, witness_fd=witness_fd,
            expected_fds=sorted(descriptors), plan_sha256=plan_sha256,
            profile=self.profile, limits=self.limits)
        name = role + '.ticket.json'
        _d2_atomic_json(self.root, name, value, 65536, self.limits)
        return self.root / name

    def launch(self, ticket, descriptors, owner):
        owner['_fd_owner'] = self
        command = (str(Path(sys.executable).absolute()), '-I', '-S', '-B', '-X', 'utf8',
                   str(self.repo / 'upload.py'), '--d2-worker', str(ticket))
        scope = nullcontext()
        if self.profile is not None:
            from tools import offline_guard_linux as guard
            scope = guard.d2_launch_scope(command, descriptors, ticket)
        with scope:
            owner['birth_attempted'] = True
            process = subprocess.Popen(command, executable=command[0], cwd=self.repo,
                env={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8'},
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                close_fds=True, pass_fds=tuple(sorted(descriptors)), start_new_session=True)
            owner['process'] = process  # Publish birth before any later failure.
        if self.case == 'pidfd_google_failure' and owner['role'] == 'google':
            _d2_fault(self)
            raise OSError('D2_PIDFD_FAILED')
        owner['pidfd'] = os.pidfd_open(process.pid, 0)
        owner['pidfd_identity'] = _d2_fd_identity(owner['pidfd'])
        if owner['pidfd'] in self.owned_fds:
            self.fd_error = True
        self.owned_fds[owner['pidfd']] = owner['pidfd_identity']
        if _d2_pidfd_pid(owner['pidfd']) != process.pid:
            self.fd_error = True
            raise _d2_error('D2_PIDFD_IDENTITY')
        if self.case == 'identity_google_failure' and owner['role'] == 'google':
            _d2_fault(self)
            raise _d2_error('D2_PROCESS_IDENTITY')
        identity = _d2_process_identity(process.pid)
        owner['identity'] = identity
        if identity['parent_pid'] != self.parent_pid or identity['session'] != process.pid:
            raise _d2_error('D2_PROCESS_IDENTITY')
        if self.case == 'registry_google_failure' and owner['role'] == 'google':
            _d2_fault(self)
            raise _d2_error('D2_REGISTRY_FAILED')
        records = [dict(role=item['role'], pid=item['process'].pid,
                        identity=item['identity']) for item in [self.tracker] + self.children
                   if item['process'] is not None]
        _d2_atomic_json(self.root, 'process-registry.json', records, 65536, self.limits)
        return process

    def close(self):
        if self.closed:
            return
        self.active = False
        self.closed = True
        if any(item['process'] is not None and item['process'].poll() is None for item in self.children):
            self.cleanup_requested = True
            _d2_settle(self.children)
        for item in self.children:
            for name in ('ready', 'go', 'bootstrap', 'witness'):
                self.close_fd(item.get(name))
            if item['process'] is not None and item['process'].returncode is not None:
                self.close_fd(item.get('pidfd'))
        self.cleanup_complete = not self.fd_error and all(not item['birth_attempted'] or item['process'] is not None
            and item['process'].returncode is not None for item in self.children)
        if not self.cleanup_complete:
            _D2_UNSETTLED.append(self)


@contextmanager
def runtime_scope(*, logging_context=None, diagnostic_limits=None):
    global _D2_ACTIVE, _D2_PENDING
    if not _D2_SCOPE_LOCK.acquire(blocking=False):
        raise _d2_error('D2_RUNTIME_BUSY')
    runtime = None
    try:
        with _d2_runtime_scope(logging_context=logging_context,
                               diagnostic_limits=diagnostic_limits) as runtime:
            yield runtime
    finally:
        if runtime is not None and runtime.owns_tracker and not runtime.transport_complete:
            _D2_PENDING = runtime
        _D2_ACTIVE = None
        _D2_SCOPE_LOCK.release()


@contextmanager
def _d2_runtime_scope(*, logging_context=None, diagnostic_limits=None):
    global _D2_ACTIVE
    # This gate precedes credentials, inventories and subprocess creation.
    if sys.platform != 'linux' or not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        raise _d2_error('D2_PLATFORM_UNSUPPORTED')
    if sys.implementation.name != 'cpython' or sys.version_info[:3] != (3, 14, 8):
        raise _d2_error('D2_RUNTIME_UNSUPPORTED')
    if signal.getsignal(signal.SIGCHLD) != signal.SIG_DFL:
        raise _d2_error('D2_SIGCHLD_UNSUPPORTED')
    if _D2_ACTIVE is not None or _D2_PENDING is not None or _D2_UNSETTLED:
        raise _d2_error('D2_RUNTIME_BUSY')
    if logging_context is not None and logging_context['config']['shutdown_deadline_s'] > 10:
        raise _d2_error('D2_LOGGING_BUDGET_UNSUPPORTED')
    # These existing read-only attestations pin every private spawn reduction
    # used below to the already accepted Linux CPython source profile.
    from tools.offline_guard_linux import attest_runtime
    attest_runtime()
    runtime = _D2Runtime.__new__(_D2Runtime)
    try:
        # Publish ownership before __init__ can launch a process then fail.
        _D2Runtime.__init__(runtime, logging_context, diagnostic_limits)
        _D2_ACTIVE = runtime
        yield runtime
    finally:
        primary_active = sys.exc_info()[0] is not None
        if getattr(runtime, 'initialized', False):
            try:
                runtime.close()
                if runtime.context is None:
                    _d2_finish_transport(runtime)
            except BaseException:
                runtime.cleanup_complete = False
                _D2_UNSETTLED.append(runtime)
                if not primary_active:
                    raise _d2_error('D2_CLEANUP_FAILED') from None
        else:
            tracker = getattr(runtime, 'tracker', None)
            if tracker is not None and tracker['process'] is not None:
                _d2_settle([tracker])
            if getattr(runtime, 'owns_tracker', False):
                try:
                    _d2_release_tracker_pipe(runtime, allow_unbound=True)
                except BaseException:
                    runtime.fd_error = True
            for descriptor in tuple(getattr(runtime, 'owned_fds', ())):
                if (getattr(runtime, 'owns_tracker', False)
                        and descriptor == runtime.tracker_write
                        and runtime.fd_error):
                    # An uncertain singleton/FD identity is retained, never
                    # closed using an integer that may now be foreign.
                    continue
                if (tracker is not None and descriptor == tracker.get('pidfd')
                        and tracker['process'] is not None and tracker['process'].poll() is None):
                    continue
                try:
                    runtime.close_fd(descriptor)
                except OSError:
                    pass
            try:
                if getattr(runtime, 'owns_tracker', False):
                    _d2_unguard_owned_tracker(runtime)
            except BaseException:
                runtime.fd_error = True
            if (getattr(runtime, 'fd_error', False) or tracker is not None and tracker['birth_attempted']
                    and (tracker['process'] is None or tracker['process'].poll() is None)):
                _D2_UNSETTLED.append(runtime)
        _D2_ACTIVE = None


class _D2PassedFd:
    def __init__(self, descriptor):
        self.descriptor = descriptor
    def detach(self):
        return self.descriptor


class _D2SpawnSerialization:
    DupFd = _D2PassedFd
    def __init__(self):
        self.descriptors = set()
    def duplicate_for_child(self, descriptor):
        os.fstat(descriptor)
        self.descriptors.add(descriptor)
        return descriptor


def _d2_serialize(bootstrap, repo):
    from multiprocessing import context, reduction
    if context.get_spawning_popen() is not None:
        raise _d2_error('D2_NESTED_SPAWN')
    adapter = _D2SpawnSerialization()
    preparation = dict(name='d2-worker', sys_path=_d2_spawn_paths(repo), sys_argv=['d2-worker'],
                       dir=str(repo), orig_dir=str(repo), start_method='spawn')
    first, second = io.BytesIO(), io.BytesIO()
    dump_code = reduction.dump.__code__  # Source-attested CPython 3.14.8 shim.
    serializer_frame = sys._getframe()
    context.set_spawning_popen(adapter)
    try:
        reduction.dump(preparation, first)
        reduction.dump(bootstrap, second)
    except BaseException as error:
        # Relinquish only this direct call's bootstrap/payload frame. A nested
        # foreign call may use the same pinned shim and remains untouched.
        trace = error.__traceback__
        while trace is not None:
            frame = trace.tb_frame
            if (frame.f_code is dump_code and frame.f_back is serializer_frame
                    and frame.f_locals.get('obj') is bootstrap
                    and frame.f_locals.get('file') is second):
                try:
                    frame.clear()
                except RuntimeError:
                    pass  # An active frame cannot be relinquished here.
            trace = trace.tb_next
        bootstrap = None
        raise
    finally:
        context.set_spawning_popen(None)
        serializer_frame = None
    payload = first.getvalue() + second.getvalue()
    if len(payload) > 65536:
        raise _d2_error('D2_BOOTSTRAP_TOO_LARGE')
    return payload, adapter.descriptors


def _d2_spawn_paths(repo):
    import sysconfig
    paths = sysconfig.get_paths()
    standard = Path(paths['stdlib'])
    allowed = {str(Path(repo)), *(str(Path(paths[key])) for key in ('stdlib', 'platstdlib', 'purelib', 'platlib')),
               str(standard / 'lib-dynload'), str(standard.parent / ('python' + str(sys.version_info.major) + str(sys.version_info.minor) + '.zip'))}
    result = []
    for value in sys.path:
        if type(value) is not str:
            raise _d2_error('D2_SPAWN_PATH_REFUSED')
        absolute = str(Path(os.path.abspath(value)))
        if absolute not in allowed:
            raise _d2_error('D2_SPAWN_PATH_REFUSED')
        if absolute not in result:
            result.append(absolute)
    if str(repo) not in result:
        result.insert(0, str(repo))
    return result


def _d2_write_bootstrap(descriptor, data, deadline):
    os.set_blocking(descriptor, False)
    view = memoryview(data)
    while view:
        if time.monotonic() >= deadline:
            raise _d2_error('D2_DEADLINE')
        _, writable, _ = select.select([], [descriptor], [], min(.1, max(0, deadline - time.monotonic())))
        if writable:
            try:
                count = os.write(descriptor, view)
            except BlockingIOError:
                continue
            view = view[count:]


def _d2_signal_owned(item, signum):
    process = item['process']
    if process is None or process.poll() is not None:
        return
    descriptor = item.get('pidfd')
    if descriptor is not None:
        try:
            matches = (_d2_fd_identity(descriptor) == item.get('pidfd_identity')
                       and _d2_pidfd_pid(descriptor) == process.pid)
        except (OSError, ValueError, RuntimeError):
            matches = False
        if matches:
            signal.pidfd_send_signal(descriptor, signum)
            return
        owner = item.get('_fd_owner')
        if owner is not None:
            owner.fd_error = True
    # The original unreaped Popen owns this PID. Never signal a reused pidfd.
    process.send_signal(signum)


def _d2_settle(children, *, deadline=None):
    if _D2_ACTIVE is not None and _D2_ACTIVE.case == 'cleanup_unsettled':
        # Fixed negative control: the cleanup operation is unavailable. Keep
        # actual live ownership; independent fixture cleanup is never success.
        _d2_fault(_D2_ACTIVE)
        return
    runtime = _D2_ACTIVE
    if runtime is not None and any(any(item is owned for owned in runtime.children) for item in children):
        if runtime.cleanup_deadline is None:
            runtime.cleanup_deadline = time.monotonic() + 10.0
            runtime.cleanup_term_until = runtime.cleanup_deadline - 5.0
        deadline, term_until = runtime.cleanup_deadline, runtime.cleanup_term_until
    else:
        deadline = time.monotonic() + 10.0 if deadline is None else deadline
        term_until = min(time.monotonic() + 5.0, deadline)
    for signum, until in ((signal.SIGTERM, term_until), (signal.SIGKILL, deadline)):
        if time.monotonic() >= deadline:
            return
        for item in children:
            process = item['process']
            if process is None or process.poll() is not None:
                continue
            try:
                _d2_signal_owned(item, signum)
            except ProcessLookupError:
                pass
        while time.monotonic() < until:
            alive = [item for item in children if item['process'] is not None
                     and item['process'].poll() is None]
            if not alive:
                return
            time.sleep(.05)


def _d2_observe_process(item, deadline):
    # Bracket the actual poll. A late terminal observation does not establish
    # when the OS exit happened; cleanup must never upgrade that timing proof.
    before = time.monotonic()
    code = item['process'].poll()
    after = time.monotonic()
    if item.get('deadline_state', 'UNOBSERVED') == 'UNOBSERVED':
        if code is not None and after < deadline:
            item['deadline_state'] = 'SETTLED_BEFORE_DEADLINE'
        elif code is None and before >= deadline:
            item['deadline_state'] = 'LIVE_AT_DEADLINE'
    return code


def _d2_worker_timeout(item, deadline_detected):
    if not deadline_detected or item.get('deadline_state') == 'SETTLED_BEFORE_DEADLINE':
        return False
    if item.get('deadline_state') == 'LIVE_AT_DEADLINE':
        return True
    return None  # No observation proves either timeliness or a live deadline miss.


def _d2_release_go(runtime, children):
    import run_logging
    _d2_check_tracker(runtime)
    if not run_logging.check_logging_health(runtime.context)['healthy']:
        raise _d2_error('D2_LOGGING_INCOMPLETE')
    if time.monotonic() >= runtime.deadline:
        raise _d2_error('D2_DEADLINE')
    # _d2_read_checkpoint uses context-managed readers. No reader survives GO.
    runtime.go_released = True  # The first successful write may already release mutation.
    for item in children:
        if os.write(item['go'], b'G') != 1:
            raise _d2_error('D2_GO_WRITE_FAILED')
        item['go_sent'] = True
        runtime.close_fd(item['go'])


def _d2_supervise(session, plan, runtime, result_sink):
    import run_logging
    run_id, attempt_id = session['run_id'], uuid.uuid4().hex
    result = _d2_result(run_id, attempt_id, session['producer_sha'], plan)
    result_sink.update(result)
    runtime.result = result_sink
    result_sink['_runtime'] = runtime
    if len(plan['ftp']) > runtime.limits['max_ftp_outputs']:
        raise _d2_error('D2_OUTPUT_LIMIT')
    # Reserve full worst-case receipts without truncating output lists.
    reservation = 2 * (2 * runtime.limits['max_checkpoint_bytes'] + runtime.limits['max_result_bytes'])
    if reservation > runtime.limits['max_run_bytes']:
        raise _d2_error('D2_RUN_TOO_LARGE')
    private = dict(run_id=run_id, attempt_id=attempt_id, producer_sha=session['producer_sha'],
        current_path=plan['_current_path'], gdoc_target=plan['_target'], source_plan=plan)
    plan_bytes = _d2_bytes(private)
    for role in ('google', 'ftp'):
        largest = _d2_checkpoint(run_id, attempt_id, session['producer_sha'], role, plan[role])
        largest.update(pid=2147483647, sequence=2 * len(plan[role]) + 8,
                       execution_state='INTERRUPTED', diagnostics_state='INCOMPLETE',
                       error_code='D2_LOGGING_INCOMPLETE')
        if len(_d2_bytes(largest)) > runtime.limits['max_checkpoint_bytes']:
            raise _d2_error('D2_CHECKPOINT_TOO_LARGE')
    # Private source/ticket metadata plus maximum simultaneous checkpoint and
    # parent pending bytes must fit before starting publication workers.
    ticket_bound = 3 * 65536
    if len(plan_bytes) + ticket_bound + reservation + 65536 > runtime.limits['max_run_bytes']:
        raise _d2_error('D2_RUN_TOO_LARGE')
    _d2_atomic_json(runtime.root, 'plan.json', private, runtime.limits['max_run_bytes'], runtime.limits)
    for role in ('google', 'ftp'):
        if runtime.case == 'registration_' + role + '_failure':
            _d2_fault(runtime)
            raise _d2_error('D2_REGISTRATION_FAILED')
        runtime.bootstraps.append(run_logging.prepare_worker(session, role))
    if runtime.case == 'listener_failure':
        session['_primary'].close()
        _d2_fault(runtime)
    if runtime.case == 'ack_failure':
        session['_ack_senders'][runtime.bootstraps[0]['_token']].close()
        _d2_fault(runtime)
    runtime.deadline = time.monotonic() + (15.0 if runtime.profile is not None else 3600.0)
    primary = None
    try:
        for index, role in enumerate(('google', 'ftp')):
            item = dict(role=role, process=None, pidfd=None, identity=None, birth_attempted=False,
                        ready=None, go=None, bootstrap=None, witness=None, ready_record=None,
                        go_sent=False, deadline_state='UNOBSERVED')
            runtime.children.append(item)
            if runtime.case == 'start_' + role + '_before':
                _d2_fault(runtime)
                raise _d2_error('D2_START_FAILED')
            payload, descriptors = _d2_serialize(runtime.bootstraps[index], runtime.repo)
            local = []
            try:
                read_end, write_end = runtime.pipe()
                local.extend((read_end, write_end))
                item['bootstrap'] = write_end
                ready_read, ready_write = runtime.pipe()
                local.extend((ready_read, ready_write))
                item['ready'] = ready_read
                go_read, go_write = runtime.pipe()
                local.extend((go_read, go_write))
                item['go'] = go_write
                witness_read, witness_write = runtime.pipe()
                local.extend((witness_read, witness_write))
                item['witness'] = witness_read
                if runtime.case == 'ticket_ftp_failure' and role == 'ftp':
                    _d2_fault(runtime)
                    raise _d2_error('D2_TICKET_FAILED')
                inherited = descriptors | {read_end, ready_write, go_read, witness_write,
                                            runtime.tracker_state._fd}
                ticket = runtime.ticket(role, tracker_fd=runtime.tracker_state._fd,
                    descriptors=inherited, bootstrap_fd=read_end, ready_fd=ready_write,
                    go_fd=go_read, witness_fd=witness_write,
                    plan_sha256=hashlib.sha256(plan_bytes).hexdigest())
                if runtime.case == 'constructor_error' and role == 'ftp':
                    # The real constructor forks and internally waits for its
                    # failed exec/chdir. Its hidden PID is not donated to ledger.
                    item['birth_attempted'] = True
                    _d2_fault(runtime)
                    command = (str(Path(sys.executable).absolute()), '-I', '-S', '-B', '-X', 'utf8',
                               str(runtime.repo / 'upload.py'), '--d2-worker', str(ticket))
                    from tools import offline_guard_linux as guard
                    with guard.d2_launch_scope(command, inherited, ticket):
                        subprocess.Popen(command, cwd=runtime.root / 'missing-cwd',
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            close_fds=True, pass_fds=tuple(sorted(inherited)), start_new_session=True)
                    raise _d2_error('D2_CONSTRUCTOR_FAULT_NOT_REACHED')
                runtime.launch(ticket, inherited, item)
                # Only a fully successful launch transfers parent endpoints.
                # A fault in any acquisition/constructor/binding keeps every
                # already acquired endpoint in this cleanup scope.
                for descriptor in (ready_read, go_write, witness_read):
                    local.remove(descriptor)
                if runtime.case == 'start_' + role + '_after_native':
                    _d2_fault(runtime)
                    raise _d2_error('D2_START_FAILED')
                if runtime.case == 'bootstrap_pipe_failure' and role == 'google':
                    runtime.close_fd(write_end)
                    _d2_fault(runtime)
                    os.write(write_end, payload)
                if runtime.case == 'bootstrap_unpickle_failure' and role == 'google':
                    first = io.BytesIO(payload)
                    pickle.load(first)  # Keep real preparation before the fault.
                    payload = payload[:first.tell()] + b'!invalid-pickle!'
                _d2_write_bootstrap(write_end, payload, runtime.deadline)
            finally:
                for descriptor in local:
                    runtime.close_fd(descriptor)
        pending = list(runtime.children)
        while pending:
            _d2_check_tracker(runtime)
            if time.monotonic() >= runtime.deadline:
                runtime.deadline_detected = True
                raise _d2_error('D2_DEADLINE')
            if not run_logging.check_logging_health(session)['healthy']:
                raise _d2_error('D2_LOGGING_INCOMPLETE')
            for item in list(pending):
                if _d2_observe_process(item, runtime.deadline) is not None:
                    raise _d2_error('D2_CHILD_BEFORE_READY')
                readable, _, _ = select.select([item['ready']], [], [], 0)
                if readable:
                    if os.read(item['ready'], 1) != b'R':
                        raise _d2_error('D2_READY_MISSING')
                    record = _d2_read_checkpoint(runtime.root, item['role'], run_id=run_id,
                        attempt_id=attempt_id, pid=item['process'].pid,
                        outputs=plan[item['role']], producer_sha=session['producer_sha'])
                    if record['execution_state'] != 'READY' or record['sequence'] != 1:
                        raise _d2_error('D2_READY_INVALID')
                    item['ready_record'] = record
                    runtime.close_fd(item['ready'])
                    pending.remove(item)
            if pending:
                time.sleep(.1)
        _d2_recheck_source_plan(plan)
        if runtime.case == 'tracker_death':
            # The fixed negative control kills the actual owned auxiliary only
            # after both READY validations. Independent observers retain its
            # real birth/exit and prove no replacement process was created.
            _d2_signal_owned(runtime.tracker, signal.SIGKILL)
            runtime.tracker['process'].wait(timeout=max(0, min(2, runtime.deadline - time.monotonic())))
            _d2_fault(runtime)
            probes = dict(actual_exit=runtime.tracker['process'].returncode,
                          before_go=not runtime.go_released)
            for name, operation in (
                    ('ensure_running_refused', runtime.tracker_state.ensure_running),
                    ('register_refused', lambda: runtime.tracker_state.register('D2_TRACKER_PROBE', 'noop')),
                    ('launch_refused', runtime.tracker_state._launch),
                    ('teardown_refused', runtime.tracker_state._teardown_dead_process)):
                try:
                    operation()
                except RuntimeError:
                    probes[name] = True
                else:
                    probes[name] = False
            result_sink['_tracker_probes'] = probes
            raise _d2_error('D2_TRACKER_UNAVAILABLE')
        _d2_release_go(runtime, runtime.children)
        while True:
            _d2_check_tracker(runtime)
            alive = [item for item in runtime.children
                     if _d2_observe_process(item, runtime.deadline) is None]
            if not alive:
                if time.monotonic() >= runtime.deadline:
                    runtime.deadline_detected = True
                    raise _d2_error('D2_DEADLINE')
                break
            if any(item['process'].returncode not in (None, 0) for item in runtime.children):
                raise _d2_error('D2_CHILD_FAILED')
            if not run_logging.check_logging_health(session)['healthy']:
                raise _d2_error('D2_LOGGING_INCOMPLETE')
            if time.monotonic() >= runtime.deadline:
                runtime.deadline_detected = True
                raise _d2_error('D2_DEADLINE')
            if runtime.case in ('cancel_google', 'cancel_ftp'):
                _d2_fault(runtime)
                raise KeyboardInterrupt()
            time.sleep(.1)
    except BaseException as error:
        primary = error
        if time.monotonic() >= runtime.deadline:
            runtime.deadline_detected = True
        # Capture per-worker timing before TERM/KILL/wait. Neither exit0 after
        # cleanup nor a sibling's deadline is evidence about this worker.
        for item in runtime.children:
            if item['process'] is not None:
                try:
                    _d2_observe_process(item, runtime.deadline)
                except BaseException:
                    pass  # Preserve the primary error and still request owned cleanup.
        runtime.cleanup_requested = True
        _d2_settle(runtime.children)
    finally:
        for role, row in result_sink['workers'].items():
            item = next((item for item in runtime.children if item['role'] == role), None)
            process = item['process'] if item else None
            attempted = bool(item and item['birth_attempted'])
            row['birth_attempted'] = attempted
            row['started'] = True if process is not None else None if attempted else False
            if process is not None:
                process.poll()
                settled = process.returncode is not None
                row.update(pid=process.pid, exitcode=process.returncode, settled=settled,
                    alive=not settled, timed_out=_d2_worker_timeout(item, runtime.deadline_detected),
                    execution_state='GO_RELEASED' if item['go_sent'] else
                        'READY' if item['ready_record'] is not None else 'STARTED',
                    cleanup_confirmed=settled, reaped=settled, process_identity=item['identity'],
                    pidfd_bound=item['pidfd'] is not None)
                if settled and runtime.go_released:
                    try:
                        record = _d2_read_checkpoint(runtime.root, role, run_id=run_id,
                            attempt_id=attempt_id, pid=process.pid, outputs=plan[role],
                            producer_sha=session['producer_sha'], previous=item['ready_record'])
                        if record['execution_state'] not in ('RETURNED', 'FAILED', 'INTERRUPTED') or record['sequence'] <= 1:
                            raise _d2_error('D2_CHECKPOINT_NONTERMINAL')
                        row.update({key: record[key] for key in ('execution_state',
                            'normal_return_observed', 'diagnostics_state', 'error_code', 'outputs')})
                        row['checkpoint_state'] = 'VALID'
                    except BaseException:
                        row.update(checkpoint_state='INVALID', diagnostics_state='INCOMPLETE',
                                   error_code='D2_CHECKPOINT_INVALID')
                        for output in row['outputs']:
                            output.update(transfer_state='UNKNOWN', commit_state='UNKNOWN')
            if attempted and process is None:
                row.update(alive=True, execution_state='UNKNOWN', error_code='D2_BIRTH_UNKNOWN')
                _D2_UNSETTLED.append(item)
            if runtime.go_released and not row['settled']:
                for output in row['outputs']:
                    output.update(transfer_state='UNKNOWN', commit_state='UNKNOWN')
            if (not (attempted and process is None) and row['timed_out'] is not None
                    and role in session['_expected']):
                try:
                    run_logging.record_worker_outcome(session, role, started=process is not None,
                        exit_status=process.returncode if process is not None else None,
                        timed_out=row['timed_out'])
                except BaseException:
                    if primary is None:
                        primary = _d2_error('D2_LOGGING_INCOMPLETE')
            if process is not None and item.get('witness') is not None:
                try:
                    # This fixed pipe is not a mutable checkpoint reader. A
                    # nonblocking read also retains the target-entry witness
                    # when cleanup cannot obtain actual OS settlement.
                    os.set_blocking(item['witness'], False)
                    try:
                        data = os.read(item['witness'], 8193)
                    except BlockingIOError:
                        data = b''
                    if len(data) > 8192:
                        raise _d2_error('D2_WITNESS_TOO_LARGE')
                    events = data.decode('ascii').splitlines()
                    result_sink.setdefault('_worker_witnesses', {})[role] = events
                    for event in events:
                        if event == 'FAULT:' + runtime.case:
                            result_sink.setdefault('_fault_witnesses', []).append(
                                dict(case=runtime.case, worker_id=role, pid=process.pid, count=1))
                    for event in events:
                        if event.startswith('HANG_AT:'):
                            stamp = event.removeprefix('HANG_AT:')
                            if not re.fullmatch('[0-9]{1,20}', stamp):
                                raise _d2_error('D2_WITNESS_INVALID')
                            result_sink['_target_entered_before_deadline'] = int(stamp) <= int(runtime.deadline * 1_000_000_000)
                except BaseException:
                    if primary is None:
                        primary = _d2_error('D2_WITNESS_INVALID')
        settled = all(not item['birth_attempted'] or item['process'] is not None
                      and item['process'].returncode is not None for item in runtime.children)
        try:
            _d2_check_tracker(runtime)
        except BaseException:
            if primary is None:
                primary = _d2_error('D2_TRACKER_UNAVAILABLE')
        if settled:
            try:
                _d2_run_inventory(runtime.root, runtime.limits, settled=True,
                    required=('google/checkpoint.json', 'ftp/checkpoint.json') if runtime.go_released else ())
            except BaseException:
                if primary is None:
                    primary = _d2_error('D2_INVENTORY_INVALID')
        _d2_aggregate(result_sink)
        successful = primary is None and _d2_workers_ok(result_sink)
        result_sink.update(parent_outcome='RETURNED' if successful else
            'INTERRUPTED' if isinstance(primary, (KeyboardInterrupt, SystemExit)) else 'FAILED',
            error_code=None if successful else 'D2_DEADLINE' if runtime.deadline_detected else 'D2_SUPERVISION_FAILED')
        result_sink['_deadline_detected'] = runtime.deadline_detected
        result_sink['_cleanup_requested'] = runtime.cleanup_requested
        if settled:
            try:
                _d2_atomic_json(runtime.root, 'parent-result.json', _d2_public_result(result_sink),
                                runtime.limits['max_result_bytes'], runtime.limits)
            except BaseException:
                if primary is None:
                    primary = _d2_error('D2_PARENT_RECEIPT_FAILED')
    if primary is not None:
        if isinstance(primary, (KeyboardInterrupt, SystemExit)):
            raise primary
        raise _d2_error('D2_SUPERVISION_FAILED') from None
    if not _d2_workers_ok(result_sink):
        raise _d2_error('D2_SUPERVISION_FAILED')


# %%
def _d2_worker_ticket():
    if len(sys.argv) != 3 or sys.argv[1] != '--d2-worker' or sys.platform != 'linux':
        raise _d2_error('D2_WORKER_ARGUMENTS')
    path = Path(os.path.abspath(sys.argv[2]))
    ticket = _d2_read_json(path, 65536)
    keys = {'schema', 'role', 'repo_root', 'run_root', 'runtime_nonce', 'parent_pid',
        'parent_start_ticks', 'source_files_sha256', 'tracker_fd', 'bootstrap_fd',
        'ready_fd', 'go_fd', 'witness_fd', 'expected_fds', 'plan_sha256', 'profile', 'limits'}
    if (type(ticket) is not dict or set(ticket) != keys or ticket['schema'] != 'd2-worker-ticket-v1'
            or ticket['role'] not in ('tracker', 'google', 'ftp')
            or ticket['repo_root'] != str(Path(__file__).resolve().parent)
            or ticket['run_root'] != str(path.parent) or path.name != ticket['role'] + '.ticket.json'
            or type(ticket['runtime_nonce']) is not str or not re.fullmatch('[0-9a-f]{32}', ticket['runtime_nonce'])
            or type(ticket['source_files_sha256']) is not dict
            or set(ticket['source_files_sha256']) != {'upload.py', 'run_logging.py'}
            or type(ticket['parent_start_ticks']) is not int or ticket['parent_start_ticks'] <= 0
            or type(ticket['parent_pid']) is not int or os.getppid() != ticket['parent_pid']
            or _d2_process_identity(os.getppid())['start_ticks'] != ticket['parent_start_ticks']
            or ticket['expected_fds'] != sorted(set(ticket['expected_fds']))
            or any(type(fd) is not int or fd < 3 for fd in ticket['expected_fds'])):
        raise _d2_error('D2_WORKER_TICKET')
    descriptor_fields = ('tracker_fd',) if ticket['role'] == 'tracker' else ('tracker_fd', 'bootstrap_fd', 'ready_fd', 'go_fd', 'witness_fd')
    if (any(type(ticket[name]) is not int or ticket[name] < 3 for name in descriptor_fields)
            or any(ticket[name] not in ticket['expected_fds'] for name in descriptor_fields)
            or ticket['role'] == 'tracker' and any(ticket[name] is not None for name in ('bootstrap_fd', 'ready_fd', 'go_fd', 'witness_fd'))):
        raise _d2_error('D2_WORKER_TICKET')
    for name, digest in ticket['source_files_sha256'].items():
        if name not in ('upload.py', 'run_logging.py') or hashlib.sha256((Path(ticket['repo_root']) / name).read_bytes()).hexdigest() != digest:
            raise _d2_error('D2_WORKER_SOURCE')
    claim = os.open(str(path) + '.claim', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(claim)
    sys.path.insert(0, ticket['repo_root'])
    return ticket


def _d2_worker_entry(ticket):
    global _D2_WORKER_ROLE, _D2_WORKER_ROOT
    import ctypes
    library = ctypes.CDLL(None, use_errno=True)
    library.prctl.argtypes = (ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong)
    library.prctl.restype = ctypes.c_int
    if library.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != ticket['parent_pid']:
        return 1
    null = os.stat('/dev/null')
    if any(not stat.S_ISCHR(os.fstat(fd).st_mode) or os.fstat(fd).st_rdev != null.st_rdev for fd in (0, 1, 2)):
        return 1
    if os.getsid(0) != os.getpid() or os.getpgrp() != os.getpid():
        return 1
    observed = []
    for leaf in os.listdir('/proc/self/fd'):
        fd = int(leaf)
        if fd >= 3:
            try:
                os.fstat(fd)
            except OSError:
                continue
            observed.append(fd)
    if sorted(observed) != ticket['expected_fds']:
        return 1
    profile = ticket['profile']
    case = profile['case'] if profile is not None else 'production'
    if profile is not None:
        from tools import offline_guard_linux as guard
        guard.d2_worker_admission(ticket)
    if ticket['role'] == 'tracker':
        from multiprocessing.resource_tracker import main as tracker_main
        try:
            tracker_main(ticket['tracker_fd'])
        except SystemExit as error:
            # The pinned tracker exits here even after a clean EOF. Preserve
            # its real status; the outer bootstrap catch must not turn 0 to 1.
            if type(error.code) is int and error.code in (0, 1, 2, 3):
                return error.code
            raise
        return 1  # The pinned tracker never returns normally.
    _D2_WORKER_ROLE = ticket['role']
    _D2_WORKER_ROOT = Path(ticket['run_root'])
    # Install before preparation/unpickle can touch multiprocessing resources.
    # A worker never owns another child, including a replacement tracker.
    def child_guard(event, args):
        if event in ('subprocess.Popen', 'os.fork', 'os.posix_spawn', 'os.exec', 'os.spawn', '_posixsubprocess.fork_exec'):
            raise _d2_error('D2_WORKER_CHILD_REFUSED')
    sys.addaudithook(child_guard)
    # Spawn reductions refer to the inert upload module, not a second execution
    # of this source with a different reader/writer role.
    sys.modules['upload'] = sys.modules[__name__]
    def witness(event):
        os.write(ticket['witness_fd'], (event + '\n').encode('ascii'))
    witness('NATIVE_ENTRY')
    if profile is not None:
        # These real writes precede import/preparation/unpickle faults. Native
        # exec redirection contains them even when no wrapper is reached.
        os.write(1, b'D2_PRIVATE_CANARY_4c1e11')
        os.write(2, b'D2_PRIVATE_CANARY_4c1e11')
        witness('CANARY_WRITES')
    if case == 'bootstrap_import_failure' and ticket['role'] == 'google':
        witness('BOOTSTRAP_IMPORT_BOUNDARY')
        witness('FAULT:' + case)
        __import__('D2_BOOTSTRAP_CANARY_MISSING')
    from multiprocessing import resource_tracker, spawn
    tracker = resource_tracker._resource_tracker
    if tracker._fd is not None or tracker._pid is not None:
        raise _d2_error('D2_WORKER_TRACKER')
    tracker._fd = ticket['tracker_fd']
    with _d2_fdopen(ticket['bootstrap_fd'], 'rb') as handle:
        witness('PREPARATION_BOUNDARY')
        preparation = pickle.load(handle)
        if case == 'bootstrap_replay_failure' and ticket['role'] == 'google':
            witness('FAULT:' + case)
            preparation['dir'] = '/D2_BOOTSTRAP_CANARY_MISSING'
        spawn.prepare(preparation)
        witness('UNPICKLE_BOUNDARY')
        if case == 'bootstrap_unpickle_failure' and ticket['role'] == 'google':
            witness('FAULT:' + case)
        bootstrap = pickle.load(handle)
    private_path = Path(ticket['run_root']) / 'plan.json'
    private = _d2_read_json(private_path, ticket['limits']['max_run_bytes'])
    if hashlib.sha256(_d2_bytes(private)).hexdigest() != ticket['plan_sha256']:
        raise _d2_error('D2_PLAN_CHANGED')
    if profile is not None:
        sys.path.insert(0, profile['vendor_root'])
    import run_logging
    context = run_logging.configure_worker(bootstrap)
    role = ticket['role']
    checkpoint = _d2_checkpoint(private['run_id'], private['attempt_id'], private['producer_sha'], role,
                                private['source_plan'][role])
    limits = _d2_validate_limits(ticket['limits'])
    code, normal = 1, False
    try:
        if case == 'abnormal_before_ready' and role == 'google':
            witness('FAULT:' + case)
            os._exit(23)
        _d2_slot_atomic_json(ticket['run_root'], role, checkpoint, limits)
        os.write(ticket['ready_fd'], b'R')
        os.close(ticket['ready_fd'])
        if os.read(ticket['go_fd'], 1) != b'G':
            raise _d2_error('D2_GO_MISSING')
        os.close(ticket['go_fd'])
        witness('GO_ENTERED')
        checkpoint.update(sequence=2, execution_state='RUNNING')
        _d2_slot_atomic_json(ticket['run_root'], role, checkpoint, limits)
        if case == 'replace_failure' and role == 'google':
            # Real failing filesystem replacement, without a retry adapter.
            witness('FAULT:' + case)
            os.replace(Path(ticket['run_root']) / role / 'not-present.pending',
                       Path(ticket['run_root']) / role / 'checkpoint.json')
        if case == 'hang_' + role or case == 'cleanup_unsettled' and role == 'google':
            witness('HANG_ENTERED')
            witness('HANG_AT:' + str(time.monotonic_ns()))
            witness('FAULT:' + case)
            while True:
                time.sleep(1)
        if case in ('exit23', 'system_exit0', 'system_exit_string') and role == 'google':
            witness('TARGET_EXIT_BOUNDARY')
            witness('FAULT:' + case)
            if case == 'exit23':
                os._exit(23)
            raise SystemExit(0 if case == 'system_exit0' else 'D2_CANARY')
        if case == 'canary_positive_control' and role == 'google':
            from tools import offline_linux as runner
            try:
                runner.d2_admit_export_bytes(b'D2_PRIVATE_CANARY_4c1e11')
            except runner.Refusal:
                witness('CANARY_REJECTED')
                witness('FAULT:' + case)
                raise _d2_error('D2_CANARY_REJECTED') from None
            raise _d2_error('D2_CANARY_CONTROL_NOT_REJECTED')
        def observe(output_id, state):
            output = next(output for output in checkpoint['outputs'] if output['output_id'] == output_id)
            output.update(transfer_state=state, commit_state='UNKNOWN')
            checkpoint['sequence'] += 1
            _d2_slot_atomic_json(ticket['run_root'], role, checkpoint, limits)
        if role == 'google':
            gdoc_upload(private['current_path'], private['gdoc_target'],
                source_plan=private['source_plan']['google'], output_observer=observe)
        else:
            ftp_upload(private['current_path'], source_plan=private['source_plan']['ftp'],
                       output_observer=observe)
        normal, code = True, 0
    except BaseException as error:
        if case in ('google_error', 'partial_google') and role == 'google' or case in ('ftp_error', 'partial_ftp') and role == 'ftp':
            witness('FAULT:' + case)
        checkpoint.update(execution_state='INTERRUPTED' if isinstance(error, (KeyboardInterrupt, SystemExit)) else 'FAILED',
                          error_code='D2_CHILD_FAILED')
        # Preserve the primary object locally. Export only a fixed code and a
        # closed type label, never exception arguments, traceback or repr.
        try:
            record = run_logging.make_error(context, Exception(), 'upload', code='D2_CHILD_FAILED')
            record['message'] = 'D2_CHILD_FAILED'
            name = type(error).__name__
            record['exception_type'] = name if name in (
                'RuntimeError', 'ValueError', 'OSError', 'PermissionError',
                'FileNotFoundError', 'KeyboardInterrupt', 'SystemExit', 'Exception') else 'BaseException'
            run_logging.emit_event(context, 'ERROR', 'upload_child_error',
                'Publication worker failed', {'stage': 'upload', 'error': record, 'outcome': 'FAILED'})
        except BaseException:
            pass  # Secondary diagnostics cannot change the primary failure.
    if case == 'normal_without_final' and role == 'google':
        witness('FAULT:' + case)
        return 0
    try:
        final = run_logging.finish_worker(context, execution_outcome='COMPLETED' if code == 0 else 'FAILED', exit_status=code)
        feeder = context['_queue']._thread
        context['_queue'].close()
        if feeder is not None:
            feeder.join(timeout=2)
            if feeder.is_alive():
                raise _d2_error('D2_WORKER_TRANSPORT_UNSETTLED')
        checkpoint.update(sequence=checkpoint['sequence'] + 1,
            execution_state='RETURNED' if normal else checkpoint['execution_state'],
            normal_return_observed=normal, diagnostics_state='COMPLETE' if final['confirmed'] else 'INCOMPLETE')
        if not final['confirmed']:
            code = 1
        if role == 'google':
            if case.startswith('checkpoint_'):
                witness('FAULT:' + case)
            if case == 'checkpoint_stale_run':
                checkpoint['run_id'] = 'foreign'
            if case == 'checkpoint_stale_pid':
                checkpoint['pid'] += 1
            if case == 'checkpoint_stale_sequence':
                checkpoint['sequence'] = 0
        _d2_slot_atomic_json(ticket['run_root'], role, checkpoint, limits)
        if role == 'google' and case in ('checkpoint_missing', 'checkpoint_corrupt', 'checkpoint_oversize'):
            path = Path(ticket['run_root']) / role / 'checkpoint.json'
            if case == 'checkpoint_missing':
                path.unlink()
            else:
                with path.open('wb') as handle:
                    handle.write(b'{' if case == 'checkpoint_corrupt' else b'x' * 65537)
        if role == 'google' and case in ('abnormal_after_final', 'final_then_exit23'):
            witness('FINAL_THEN_EXIT23')
            witness('FAULT:' + case)
            os._exit(23)
        witness('WORKER_RETURNED')
    except BaseException:
        return 1
    return code


# %%
def _d2_finish_transport(runtime):
    global _D2_PENDING
    if runtime is None or not runtime.owns_tracker:
        return True
    if runtime.transport_cleanup_done:
        return runtime.transport_complete
    if not runtime.closed or not runtime.cleanup_complete:
        _D2_UNSETTLED.append(runtime)
        return False
    context = runtime.context
    if runtime.cleanup_deadline is None:
        runtime.cleanup_deadline = time.monotonic() + 10.0
        runtime.cleanup_term_until = runtime.cleanup_deadline - 5.0
    deadline = runtime.cleanup_deadline
    try:
        process = runtime.tracker['process']
        if process is None:
            raise _d2_error('D2_TRACKER_UNKNOWN')
        try:
            _d2_check_tracker(runtime)
        except BaseException:
            # Failed transport remains INCOMPLETE. Before releasing owned
            # SemLocks, obtain actual tracker death so UNREGISTER cannot enter
            # CPython's relaunch path or print finalizer tracebacks.
            runtime.tracker_failed = True
            if process.poll() is None:
                _d2_settle([runtime.tracker], deadline=deadline)
            if process.poll() is None:
                raise _d2_error('D2_TRACKER_UNSETTLED')
        if context is not None:
            if context['_thread'].is_alive():
                raise _d2_error('D2_TRANSPORT_UNSETTLED')
            queue = context['_queue']
            feeder = queue._thread
            queue.close()
            if feeder is not None:
                feeder.join(timeout=min(2, max(0, deadline - time.monotonic())))
            if feeder is not None and feeder.is_alive() or context['_thread'].is_alive():
                raise _d2_error('D2_TRANSPORT_UNSETTLED')
            # Owned session is finalized; relinquish Queue/SemLock references
            # before tracker EOF. Its actual immutable report remains cached.
            context['_queue'] = None
            context['_ack'] = None
            context['_receivers'].clear()
            context['_ack_senders'].clear()
            from types import SimpleNamespace
            context['_run_failed'] = SimpleNamespace(value=context['_run_failed'].value)
            queue = feeder = None
        runtime.bootstraps.clear()
        gc.collect()
        _d2_release_tracker_pipe(runtime)
        try:
            process.wait(timeout=min(5, max(0, deadline - time.monotonic())))
        except subprocess.TimeoutExpired:
            _d2_settle([runtime.tracker], deadline=deadline)
        if process.returncode is None:
            raise _d2_error('D2_TRACKER_UNSETTLED')
        runtime.close_fd(runtime.tracker.get('pidfd'))
        _d2_unguard_owned_tracker(runtime)
        runtime.transport_cleanup_done = True
        runtime.transport_complete = (process.returncode == 0
                                      and not runtime.tracker_failed and not runtime.fd_error)
        if runtime.result is not None and runtime.case == 'tracker_death':
            runtime.result.setdefault('_tracker_probes', {})['failed_unregisters'] = runtime.failed_unregisters
        if _D2_PENDING is runtime:
            _D2_PENDING = None
        return runtime.transport_complete
    except BaseException:
        _D2_UNSETTLED.append(runtime)
        return False


def _d2_apply_finalization(result_sink, report, *, status=0):
    if type(result_sink) is not dict:
        return status if type(status) is int and status else 1
    runtime = result_sink.get('_runtime')
    same = (type(report) is dict and report.get('run_id') == result_sink.get('run_id')
            and runtime is not None and runtime.context is not None
            and runtime.context.get('_final') is report)
    complete = (same and _d2_workers_ok(result_sink) and runtime.closed and runtime.cleanup_complete
        and not runtime.tracker_failed and not runtime.fd_error
        and report['execution_outcome'] == 'COMPLETED' and report['exit_status'] == 0
        and report['delivery_outcome'] == 'COMPLETE' and report['storage_sealed'] is True
        and not report['evidence_incomplete'] and status == 0)
    transport = _d2_finish_transport(runtime)
    if runtime is not None and runtime.case == 'postseal_cleanup_failure':
        _d2_fault(runtime)
        transport = False
    complete = complete and transport
    report_status = report.get('exit_status') if same else None
    final_status = status if type(status) is int and status else report_status if type(report_status) is int and report_status else 0 if complete else 1
    result_sink.update(receipt_scope='FINAL_IN_MEMORY', finalization_state='OBSERVED_COMPLETE' if complete else 'OBSERVED_INCOMPLETE',
        diagnostic_success_allowed=bool(complete), diagnostics_state='COMPLETE' if complete else 'INCOMPLETE',
        final_exit_status=final_status, storage_sealed=report['storage_sealed'] if same else 'UNKNOWN')
    result_sink['_logging_report'] = report
    result_sink['_transport_settled'] = transport
    return final_status


def _d2_completion_allowed(result_sink):
    return (type(result_sink) is dict and result_sink.get('diagnostic_success_allowed') is True
        and result_sink.get('finalization_state') == 'OBSERVED_COMPLETE'
        and result_sink.get('final_exit_status') == 0 and result_sink.get('storage_sealed') is True
        and result_sink.get('parent_outcome') == 'RETURNED' and _d2_workers_ok(result_sink))


def _d2_finalize_owned(runtime, sink, *, outcome, status, application_error=None):
    import run_logging
    session = runtime.context
    original_freeze, original_inventory = run_logging._freeze_and_wait, run_logging._inventory
    def fault_freeze(context):
        if context is session:
            if not _d2_workers_ok(sink) or len(context['_worker_finals']) != 2:
                raise _d2_error('D2_FAULT_BOUNDARY_NOT_REACHED')
            _d2_atomic_json(runtime.root.parent, 'preseal-witness.json',
                dict(boundary='FREEZE_ENTRY', both_workers_success=True, worker_finals=2,
                     prior_seal=context['_final'] is not None), 65536)
            _d2_fault(runtime)
            sink['_preseal_preconditions'] = dict(actual_children_settled=2,
                actual_children_exit_zero=2, worker_final_completed_zero=2,
                final_absent=context['_final'] is None,
                summary_absent=not (context['_run_root'] / 'run_summary.json').exists(),
                fault_boundary='FREEZE_ENTRY')
            raise _d2_error('D2_FINALIZATION_FAILED')
        return original_freeze(context)
    def fault_inventory(root, boundary):
        if root == session['_run_root']:
            if not _d2_workers_ok(sink) or len(session['_worker_finals']) != 2:
                raise _d2_error('D2_FAULT_BOUNDARY_NOT_REACHED')
            _d2_fault(runtime)
            sink['_preseal_preconditions'] = dict(actual_children_settled=2,
                actual_children_exit_zero=2, worker_final_completed_zero=2,
                final_absent=session['_final'] is None,
                summary_absent=not (session['_run_root'] / 'run_summary.json').exists(),
                fault_boundary='SEAL_INVENTORY')
            raise OSError('D2_FINALIZATION_INCOMPLETE')
        return original_inventory(root, boundary)
    try:
        if runtime.case == 'parent_finalization_raises':
            run_logging._freeze_and_wait = fault_freeze
        elif runtime.case == 'parent_finalization_incomplete':
            run_logging._inventory = fault_inventory
        try:
            report = run_logging.finalize_logging(session, execution_outcome=outcome,
                exit_status=status, application_error=application_error)
        except BaseException:
            status = status or 1
            sink['error_code'] = 'D2_FINALIZATION_FAILED'
            # No replacement application finalization. Independent fixture
            # cleanup may later produce a separately labelled failed report.
            report = None
    finally:
        run_logging._freeze_and_wait = original_freeze
        run_logging._inventory = original_inventory
    return _d2_apply_finalization(sink, report, status=status)


def _d2_render_completion(elapsed, *, pipeline=False):
    mins, secs = divmod(elapsed, 60)
    hours, mins = divmod(mins, 60)
    if pipeline:
        print('Скрипт закончил отрабатывать: {}'.format(datetime.datetime.now()))
        message = 'Скрипт полностью закончил работу.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'
    else:
        message = 'Скрипт загрузки данных закончил работу.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'
    print(message.format(round(hours), round(mins), round(secs), round(elapsed, 3), 'сек'))
    if not pipeline:
        print('-----------------------')


def main(current_path, gdoc_to_write, *, logging_context=None, result_sink=None,
         diagnostic_limits=None, _runtime=None):
    sink = {} if result_sink is None else result_sink
    if type(sink) is not dict:
        raise _d2_error('D2_RESULT_SINK_INVALID')
    owned = _runtime is None
    runtime = None
    primary = None
    try:
        with runtime_scope(logging_context=logging_context, diagnostic_limits=diagnostic_limits) if owned else nullcontext(_runtime) as runtime:
            if not isinstance(runtime, _D2Runtime):
                raise _d2_error('D2_RUNTIME_INVALID')
            if diagnostic_limits is not None and _d2_validate_limits(diagnostic_limits) != runtime.limits:
                raise _d2_error('D2_LIMITS_CHANGED')
            sink['_runtime'] = runtime
            runtime.result = sink
            context = runtime.bind_context(logging_context)
            sink.update(_d2_result(context['run_id'], uuid.uuid4().hex,
                context['producer_sha'], {'google': [], 'ftp': []}))
            sink['_runtime'] = runtime
            runtime.result = sink
            plan = _d2_build_source_plan(current_path)
            plan.update(_current_path=current_path, _target=gdoc_to_write)
            _d2_supervise(context, plan, runtime, sink)
    except BaseException as error:
        primary = error
    if owned and runtime is not None and runtime.owns_context:
        if primary is not None and sink.get('parent_outcome') == 'UNKNOWN':
            sink.update(parent_outcome='INTERRUPTED' if isinstance(primary, (KeyboardInterrupt, SystemExit)) else 'FAILED',
                        error_code='D2_PREFLIGHT_FAILED')
        status = _d2_finalize_owned(runtime, sink,
            outcome='INTERRUPTED' if isinstance(primary, (KeyboardInterrupt, SystemExit)) else 'FAILED' if primary else 'COMPLETED',
            status=130 if isinstance(primary, KeyboardInterrupt) else 1 if primary else 0)
        if status and primary is None:
            primary = _d2_error('D2_DIAGNOSTICS_INCOMPLETE')
        if _d2_completion_allowed(sink):
            _d2_render_completion(time.time() - start_time)
    if primary is not None:
        raise primary
    return None


def _d2_run_cli():
    if len(sys.argv) > 1 and sys.argv[1] == '--d2-worker':
        try:
            return _d2_worker_entry(_d2_worker_ticket())
        except BaseException:
            return 1
    if len(sys.argv) != 1:
        sys.stderr.write('D2_ARGUMENTS_INVALID\n')
        return 1
    try:
        main(current_path, gdoc_to_write)
        return 0
    except BaseException as error:
        sys.stderr.write('D2_UPLOAD_INCOMPLETE\n')
        return 130 if isinstance(error, KeyboardInterrupt) else 1


if __name__ == '__main__':
    raise SystemExit(_d2_run_cli())
