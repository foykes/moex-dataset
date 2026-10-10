"""Windows-only cooperative directory lock and atomic replacement of one file.

Importing this module performs no filesystem, logger or Windows DLL operations.
The caller owns target validation and the parent-owned F-LOG session.
"""

import contextlib
import copy
import hashlib
import math
import ntpath
import os
import re
import stat
import sys
import threading
import time


__all__ = ['resource_lock', 'atomic_write_file']

# Реестр содержит authority только живых lease, а не публичные receipts.
# object-token нельзя собрать из словаря caller; PID/thread исключают передачу
# чужому исполнителю. Здесь нет logging session, cache admission или lockfiles.
_leases = {}
_active_resources = set()
_registry_lock = threading.Lock()
_REPARSE = 0x400
_EVENTS = {
    'd1_lock_acquired': ('INFO', 'ACQUIRED', 'Получена блокировка ресурса'),
    'd1_lock_denied': ('WARNING', 'DENIED', 'Ресурс занят; lease не выдан'),
    'd1_lock_abandoned': ('INFO', 'ABANDONED', 'Обнаружено завершение предыдущего владельца'),
    'd1_target_admitted': ('INFO', 'ADMITTED', 'Текущий target допущен к локальной операции'),
    'd1_target_rejected': ('ERROR', 'REJECTED', 'Текущий target не допущен'),
    'd1_temp_prepared': ('INFO', 'PREPARED', 'Временный файл подготовлен'),
    'd1_temp_validated': ('INFO', 'VALIDATED', 'Временный файл проверен'),
    'd1_replace_precommit': ('INFO', 'READY_TO_REPLACE', 'Подготовлена замена отдельного файла'),
    'd1_file_replaced': ('INFO', 'REPLACED', 'Отдельный файл заменён'),
    'd1_write_failed': ('ERROR', 'FAILED', 'Локальная запись завершилась ошибкой'),
    'd1_lock_released': ('INFO', 'RELEASED', 'Блокировка ресурса освобождена'),
}


def _windows_backend():
    # DLL и native структуры связываются только после platform admission.
    import ctypes
    from ctypes import wintypes

    class FileIdInfo(ctypes.Structure):
        _fields_ = [('VolumeSerialNumber', ctypes.c_ulonglong),
                    ('Identifier', ctypes.c_ubyte * 16)]

    class FileInfo(ctypes.Structure):
        _fields_ = [('attributes', wintypes.DWORD),
                    ('created', wintypes.FILETIME), ('accessed', wintypes.FILETIME),
                    ('written', wintypes.FILETIME), ('serial', wintypes.DWORD),
                    ('size_high', wintypes.DWORD), ('size_low', wintypes.DWORD),
                    ('links', wintypes.DWORD), ('index_high', wintypes.DWORD),
                    ('index_low', wintypes.DWORD)]

    dll = ctypes.WinDLL('kernel32', use_last_error=True)
    signatures = {
        'CreateFileW': ([wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                         wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD,
                         wintypes.HANDLE], wintypes.HANDLE),
        'GetFileInformationByHandleEx': ([wintypes.HANDLE, ctypes.c_int,
                                          wintypes.LPVOID, wintypes.DWORD], wintypes.BOOL),
        'GetFileInformationByHandle': ([wintypes.HANDLE, wintypes.LPVOID], wintypes.BOOL),
        'GetDriveTypeW': ([wintypes.LPCWSTR], wintypes.UINT),
        'CreateMutexW': ([wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR], wintypes.HANDLE),
        'WaitForSingleObject': ([wintypes.HANDLE, wintypes.DWORD], wintypes.DWORD),
        'ReleaseMutex': ([wintypes.HANDLE], wintypes.BOOL),
        'CloseHandle': ([wintypes.HANDLE], wintypes.BOOL),
    }
    result = dict(ctypes=ctypes, FileIdInfo=FileIdInfo, FileInfo=FileInfo)
    for name, (arguments, returns) in signatures.items():
        function = getattr(dll, name)
        function.argtypes = arguments
        function.restype = returns
        result[name] = function
    return result


def _native_error(backend):
    return backend['ctypes'].WinError(backend['ctypes'].get_last_error())


def _parent_identity(path, backend):
    handle = backend['CreateFileW'](path, 0x80, 7, None, 3, 0x02200000, None)
    invalid = backend['ctypes'].c_void_p(-1).value
    if not handle or handle == invalid:
        raise _native_error(backend)
    primary = False
    try:
        details = backend['FileInfo']()
        if not backend['GetFileInformationByHandle'](handle, backend['ctypes'].byref(details)):
            raise _native_error(backend)
        if not details.attributes & 0x10 or details.attributes & _REPARSE:
            raise ValueError('D1_PARENT_UNSAFE')
        identity = backend['FileIdInfo']()
        if not backend['GetFileInformationByHandleEx'](
                handle, 18, backend['ctypes'].byref(identity), backend['ctypes'].sizeof(identity)):
            raise _native_error(backend)
        # Нет legacy/stat fallback: только все 64 + 128 bits одного HANDLE.
        return identity.VolumeSerialNumber, bytes(identity.Identifier)
    except BaseException:
        primary = True
        raise
    finally:
        try:
            closed = backend['CloseHandle'](handle)
        except BaseException:
            # Cleanup не заменяет уже возникшую ошибку identity query и её traceback.
            if not primary:
                raise
        else:
            if not closed and not primary:
                raise _native_error(backend)


def _mutex_name(identity):
    serial, identifier = identity
    if type(serial) is not int or not 0 <= serial < 2**64 or type(identifier) is not bytes or len(identifier) != 16:
        raise ValueError('D1_IDENTITY_UNCONFIRMED')
    payload = b'MDS-D1-v1\0' + serial.to_bytes(8, 'little') + identifier
    return 'Global\\MDS-D1-v1-' + hashlib.sha256(payload).hexdigest()


def _path_text(value):
    value = os.fspath(value)
    if type(value) is not str or not value or '\0' in value:
        raise ValueError('D1_PATH_INVALID')
    return value.replace('/', '\\')


def _check_components(parts):
    reserved = {'CON', 'PRN', 'AUX', 'NUL', 'CLOCK$', 'CONIN$', 'CONOUT$'}
    for part in parts:
        stem = part.split('.')[0].rstrip(' .').upper()
        if (part in {'.', '..'} or part.endswith((' ', '.'))
                or any(character in ':*?"<>|' or ord(character) < 32 for character in part)
                or stem in reserved or re.fullmatch(r'(COM|LPT)[1-9¹²³]', stem)
                or re.search(r'~[0-9]+(?:\.|$)', part)):
            raise ValueError('D1_PATH_ALIAS')


def _check_chain(path, *, absent=False):
    drive, tail = ntpath.splitdrive(path)
    current = drive + '\\'
    details = os.lstat(current)
    if not stat.S_ISDIR(details.st_mode) or getattr(details, 'st_file_attributes', 0) & _REPARSE:
        raise ValueError('D1_PARENT_UNSAFE')
    parts = [part for part in tail.split('\\') if part]
    for index, part in enumerate(parts):
        current = ntpath.join(current, part)
        try:
            details = os.lstat(current)
        except FileNotFoundError:
            if absent and index == len(parts) - 1:
                return None
            raise
        if stat.S_ISLNK(details.st_mode) or getattr(details, 'st_file_attributes', 0) & _REPARSE:
            raise ValueError('D1_PATH_REPARSE')
        if index != len(parts) - 1 and not stat.S_ISDIR(details.st_mode):
            raise ValueError('D1_PARENT_UNSAFE')
    return details


def _paths(relative_path, resource_root, backend):
    root = _path_text(resource_root)
    drive, tail = ntpath.splitdrive(root)
    if not re.fullmatch('[A-Za-z]:', drive) or not tail.startswith('\\'):
        raise ValueError('D1_ROOT_LOCAL_ABSOLUTE_REQUIRED')
    relative = _path_text(relative_path)
    if ntpath.splitdrive(relative)[0] or ntpath.isabs(relative):
        raise ValueError('D1_PATH_RELATIVE_REQUIRED')
    _check_components([part for part in tail.split('\\') if part])
    _check_components([part for part in relative.split('\\') if part])
    root = ntpath.normpath(root)
    target = ntpath.normpath(ntpath.join(root, relative))
    if ntpath.normcase(ntpath.commonpath([root, target])) != ntpath.normcase(root) or target == root:
        raise ValueError('D1_PATH_OUTSIDE_ROOT')
    if backend['GetDriveTypeW'](drive + '\\') not in {2, 3, 6}:
        raise ValueError('D1_NETWORK_OR_UNKNOWN_DRIVE')
    if not stat.S_ISDIR(_check_chain(root).st_mode):
        raise ValueError('D1_ROOT_DIRECTORY_REQUIRED')
    parent = ntpath.dirname(target)
    if not stat.S_ISDIR(_check_chain(parent).st_mode):
        raise ValueError('D1_PARENT_UNSAFE')
    _check_chain(target, absent=True)
    return target, parent


def _file_identity(details):
    if (not stat.S_ISREG(details.st_mode) or details.st_nlink != 1
            or not details.st_ino or getattr(details, 'st_file_attributes', 0) & _REPARSE):
        raise ValueError('D1_FILE_UNSAFE')
    return details.st_dev, details.st_ino


def _open_regular(path):
    before = _check_chain(path)
    identity = _file_identity(before)
    stream = open(path, 'rb')
    try:
        if _file_identity(os.fstat(stream.fileno())) != identity:
            raise ValueError('D1_FILE_CHANGED')
    except BaseException:
        try:
            stream.close()
        except BaseException:
            pass
        raise
    return stream, identity


@contextlib.contextmanager
def _closing(stream):
    try:
        yield stream
    except BaseException:
        # Ошибка закрытия borrowed stream не подменяет исходную ошибку callback.
        try:
            stream.close()
        except BaseException:
            pass
        raise
    else:
        stream.close()


def _stream_observation(stream):
    before = os.fstat(stream.fileno())
    identity = _file_identity(before)
    stream.seek(0)
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
        size += len(chunk)
    after = os.fstat(stream.fileno())
    if (_file_identity(after) != identity or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns or size != after.st_size):
        raise ValueError('D1_FILE_CHANGED')
    stream.seek(0)
    return dict(exists=True, sha256=digest.hexdigest(), bytes=size), identity


def _observe(path, backend):
    if _check_chain(path, absent=True) is None:
        return dict(exists=False, sha256=None, bytes=None), None
    stream, identity = _open_regular(path)
    with _closing(stream):
        observation, observed_identity = _stream_observation(stream)
    if identity != observed_identity or _file_identity(_check_chain(path)) != identity:
        raise ValueError('D1_FILE_CHANGED')
    return observation, identity


def _validate_file(path, observation, identity, callback):
    if not observation['exists']:
        return callback(None)
    stream, opened_identity = _open_regular(path)
    with _closing(stream):
        if opened_identity != identity or _stream_observation(stream)[0] != observation:
            raise ValueError('D1_FILE_CHANGED')
        # Borrowed stream начинает с cursor=0; callback не должен его закрывать.
        result = callback(stream)
        if _stream_observation(stream)[0] != observation:
            raise ValueError('D1_FILE_CHANGED')
    return result


def _new_receipt():
    operation = os.urandom(16).hex()
    return dict(receipt_version=1, operation_id=operation, file_ref='d1-file-' + operation,
        lock_observation='NOT_ACQUIRED', lock_state='NOT_ACQUIRED',
        target_state='UNKNOWN', target_validation='NOT_RUN',
        lease_entry_observation=None, attempt_baseline=None, final_observation=None,
        prepared_candidate=None, commit_state='NOT_ATTEMPTED', final_relation='UNKNOWN',
        write_status='NOT_RUN', diagnostics_state='NOT_CONFIRMED',
        mandatory_sequences=[], barrier_sequence=None, errors=[],
        null_reasons={key: 'NOT_REACHED' for key in (
            'lease_entry_observation', 'attempt_baseline', 'final_observation',
            'prepared_candidate', 'barrier_sequence')})


def _publish(info):
    # Даже если caller изменил sink во время callback, произвольные поля не
    # попадут в output. Вся authority остаётся в private info.
    info['sink'].clear()
    info['sink'].update(copy.deepcopy(info['receipt']))


def _known(info, key, value):
    info['receipt'][key] = value
    info['receipt']['null_reasons'].pop(key, None)


def _error(info, stage, code, *, primary=False):
    row = dict(stage=stage, code=code)
    errors = info['receipt']['errors']
    if primary:
        errors.insert(0, row)
    elif len(errors) < 16:
        errors.append(row)
    del errors[16:]


def _target_failure(info, error):
    # Проверяем только собственные стабильные codes, не str/repr application
    # exception. Недоступный файл не превращается в подтверждённый ABSENT.
    changed = (type(error) is ValueError and len(error.args) == 1
        and type(error.args[0]) is str and error.args[0] in {
            'D1_FILE_CHANGED', 'D1_TARGET_CHANGED', 'D1_PARENT_CHANGED'})
    info['receipt']['target_state'] = 'CHANGED' if changed else 'UNKNOWN'


def _logging(info):
    import run_logging
    return run_logging


def _healthy(info):
    session = info['session']
    if type(session) is not dict or session.get('worker_id', 'worker') is not None:
        raise ValueError('D1_PARENT_LOGGING_SESSION_REQUIRED')
    config = session.get('config')
    levels = {'DEBUG': 10, 'INFO': 20, 'WARNING': 30, 'ERROR': 40}
    if (type(config) is not dict or levels.get(config.get('file_level'), 100) > 20
            or levels.get(config.get('console_level'), 100) > 20):
        raise ValueError('D1_INFO_VISIBILITY_REQUIRED')
    health = _logging(info).check_logging_health(session)
    if health.get('healthy') is not True or health.get('state') != 'ACTIVE':
        raise ValueError('D1_LOGGING_UNHEALTHY')


def _event(info, name, *, mandatory=False, error=None):
    level, outcome, message = _EVENTS[name]
    fields = dict(stage='runtime', file=info['receipt']['file_ref'], outcome=outcome,
        elapsed_ms=max(0, (time.monotonic() - info['attempt_started']) * 1000), fields={})
    if error is not None:
        fields['error'] = _logging(info).make_error(info['session'], error, 'runtime', code='D1_OPERATION_FAILED')
    emitted = _logging(info).emit_event(info['session'], level, name, message, fields)
    sequence = emitted.get('sequence')
    if emitted.get('accepted') is not True or type(sequence) is not int or sequence <= 0:
        raise ValueError('D1_EVENT_NOT_ACCEPTED')
    producer = emitted.get('producer')
    if type(producer) is not int or not 0 <= producer < 2**64:
        raise ValueError('D1_DIAGNOSTICS_PRODUCER_INVALID')
    if 'producer' not in info:
        info['producer'] = producer
    elif producer != info['producer']:
        raise ValueError('D1_DIAGNOSTICS_PRODUCER_CHANGED')
    if sequence <= info.get('last_sequence', 0):
        raise ValueError('D1_DIAGNOSTICS_SEQUENCE_INVALID')
    info['last_sequence'] = sequence
    if mandatory:
        info['receipt']['mandatory_sequences'].append(sequence)
    _publish(info)


def _barrier(info, state):
    barrier = _logging(info).flush_logging(info['session'])
    sequence = barrier.get('sequence')
    preceding = info['receipt']['mandatory_sequences']
    if (barrier.get('accepted') is not True or barrier.get('confirmed') is not True
            or type(sequence) is not int or sequence <= max(preceding, default=0)
            or sequence <= info.get('last_sequence', 0)
            or type(barrier.get('producer')) is not int
            or barrier.get('producer') != info.get('producer')):
        raise ValueError('D1_DELIVERY_NOT_CONFIRMED')
    _healthy(info)
    info['last_sequence'] = sequence
    _known(info, 'barrier_sequence', sequence)
    info['receipt']['diagnostics_state'] = state
    _publish(info)


def _secondary(info, stage, code, action):
    try:
        action()
    except BaseException:
        _error(info, stage, code)
        info['diagnostics_failed'] = True
        info['receipt']['diagnostics_state'] = 'INCOMPLETE'


def _failure(info, stage, error, code):
    _error(info, stage, code, primary=True)
    if stage in {'diagnostics', 'release'}:
        info['diagnostics_failed'] = True
        info['receipt']['diagnostics_state'] = 'INCOMPLETE'
    _secondary(info, 'diagnostics', 'D1_FAILURE_EVENT_FAILED',
               lambda: _event(info, 'd1_write_failed', error=error))
    _publish(info)


def _wait_for_mutex(backend, handle, timeout_s):
    deadline = time.monotonic() + timeout_s
    while True:
        remaining = max(0, deadline - time.monotonic())
        milliseconds = min(100, math.ceil(remaining * 1000)) if timeout_s else 0
        result = backend['WaitForSingleObject'](handle, milliseconds)
        if result in {0, 0x80}:
            return result
        if result == 0xffffffff:
            raise _native_error(backend)
        if result != 0x102:
            raise ValueError('D1_WAIT_UNEXPECTED')
        if not timeout_s or time.monotonic() >= deadline:
            raise TimeoutError('D1_LOCK_DENIED')


@contextlib.contextmanager
def resource_lock(relative_path, *, resource_root, validate_target,
                  logging_session, receipt, timeout_s=0):
    """Admit this target under its physical parent directory mutex.

    The caller must keep current reads and candidate computation inside the
    context. A new empty plain receipt dict is required for every attempt.
    """
    if type(receipt) is not dict or receipt:
        raise ValueError('D1_EMPTY_RECEIPT_REQUIRED')
    info = dict(sink=receipt, receipt=_new_receipt(), session=logging_session,
                owner=(os.getpid(), threading.get_ident()), used=False,
                diagnostics_failed=False, attempt_started=time.monotonic())
    _publish(info)
    handle = None
    acquired = False
    registered = False
    lease = object()
    primary = None
    stage = 'admission'
    try:
        if sys.platform != 'win32':
            raise OSError('D1_UNSUPPORTED_PLATFORM')
        if type(timeout_s) not in {int, float} or not math.isfinite(timeout_s) or not 0 <= timeout_s <= 60:
            raise ValueError('D1_TIMEOUT_INVALID')
        if not callable(validate_target):
            raise ValueError('D1_VALIDATOR_REQUIRED')
        stage = 'diagnostics'
        _healthy(info)
        stage = 'path'
        backend = _windows_backend()
        target, parent = _paths(relative_path, resource_root, backend)
        identity = _parent_identity(parent, backend)
        resource = (*info['owner'], identity)
        info.update(backend=backend, target=target, parent=parent,
                    parent_identity=identity, resource=resource)
        with _registry_lock:
            if resource in _active_resources:
                raise ValueError('D1_REENTRANT_LOCK')
            _active_resources.add(resource)
            registered = True
        stage = 'lock'
        handle = backend['CreateMutexW'](None, False, _mutex_name(identity))
        if not handle:
            raise _native_error(backend)
        info['handle'] = handle
        try:
            observed = _wait_for_mutex(backend, handle, timeout_s)
        except TimeoutError:
            _secondary(info, 'diagnostics', 'D1_DENIAL_EVENT_FAILED', lambda: _event(info, 'd1_lock_denied'))
            raise
        acquired = True
        info['receipt'].update(lock_state='OWNED', lock_observation='ABANDONED' if observed == 0x80 else 'NORMAL')
        _publish(info)
        stage = 'diagnostics'
        _event(info, 'd1_lock_acquired', mandatory=True)
        if observed == 0x80:
            _event(info, 'd1_lock_abandoned', mandatory=True)
        stage = 'admission'
        if _parent_identity(parent, backend) != identity:
            raise ValueError('D1_PARENT_CHANGED')
        try:
            entry, file_identity = _observe(target, backend)
        except BaseException as error:
            _target_failure(info, error)
            raise
        _known(info, 'lease_entry_observation', entry)
        info['receipt']['target_state'] = 'PRESENT' if entry['exists'] else 'ABSENT'
        _publish(info)
        try:
            permitted = _validate_file(target, entry, file_identity, validate_target)
        except BaseException as error:
            info['receipt']['target_validation'] = 'ERROR'
            _target_failure(info, error)
            raise
        if permitted is not True:
            info['receipt']['target_validation'] = 'REJECTED'
            _secondary(info, 'diagnostics', 'D1_REJECTION_EVENT_FAILED', lambda: _event(info, 'd1_target_rejected'))
            raise ValueError('D1_RECOVERY_REQUIRED')
        try:
            current, current_identity = _observe(target, backend)
        except BaseException as error:
            _target_failure(info, error)
            raise
        if current != entry or current_identity != file_identity:
            info['receipt']['target_state'] = 'CHANGED'
            raise ValueError('D1_TARGET_CHANGED')
        info.update(entry=entry, entry_identity=file_identity)
        info['receipt']['target_validation'] = 'ADMITTED'
        stage = 'diagnostics'
        _event(info, 'd1_target_admitted', mandatory=True)
        _barrier(info, 'PRECOMMIT_CONFIRMED')
        with _registry_lock:
            _leases[lease] = info
        stage = 'caller'
        yield lease
    except BaseException as error:
        primary = error
        # Atomic helper уже записал свою первичную ошибку: повторная запись
        # context manager не должна менять порядок или факт REPLACED.
        if info.get('atomic_error') is not error:
            if stage == 'admission' and info['receipt']['target_validation'] == 'NOT_RUN':
                info['receipt']['target_validation'] = 'ERROR'
            _failure(info, stage, error, 'D1_' + stage.upper() + '_FAILED')
        raise
    finally:
        with _registry_lock:
            _leases.pop(lease, None)
        cleanup_error = None
        if acquired:
            try:
                if not backend['ReleaseMutex'](handle):
                    info['receipt']['lock_state'] = 'UNKNOWN'
                    raise _native_error(backend)
                acquired = False
                info['receipt']['lock_state'] = 'RELEASED'
            except BaseException as error:
                cleanup_error = error
                _error(info, 'release', 'D1_RELEASE_FAILED', primary=primary is None)
                info['diagnostics_failed'] = True
        if handle is not None:
            try:
                if not backend['CloseHandle'](handle):
                    raise _native_error(backend)
            except BaseException as error:
                cleanup_error = cleanup_error or error
                _error(info, 'release', 'D1_HANDLE_CLOSE_FAILED', primary=primary is None and cleanup_error is error)
                info['diagnostics_failed'] = True
        if registered:
            with _registry_lock:
                _active_resources.discard(resource)
        if info['receipt']['lock_state'] == 'RELEASED':
            try:
                _event(info, 'd1_lock_released', mandatory=True)
                _barrier(info, 'COMPLETE' if not info['diagnostics_failed'] else 'INCOMPLETE')
            except BaseException as error:
                cleanup_error = cleanup_error or error
                _error(info, 'diagnostics', 'D1_RELEASE_DIAGNOSTICS_FAILED', primary=primary is None and cleanup_error is error)
                info['diagnostics_failed'] = True
        if info['diagnostics_failed']:
            info['receipt']['diagnostics_state'] = 'INCOMPLETE'
        if cleanup_error is not None:
            info['receipt']['write_status'] = 'FAILED'
        _publish(info)
        if primary is None and cleanup_error is not None:
            raise cleanup_error


def _lease_info(lease):
    if sys.platform != 'win32':
        raise OSError('D1_UNSUPPORTED_PLATFORM')
    if type(lease) is not object:
        raise ValueError('D1_LEASE_INVALID')
    with _registry_lock:
        info = _leases.get(lease)
    if info is None or info['owner'] != (os.getpid(), threading.get_ident()):
        raise ValueError('D1_LEASE_INVALID')
    return info


def _readback(lease):
    info = _lease_info(lease)
    if _parent_identity(info['parent'], info['backend']) != info['parent_identity']:
        raise ValueError('D1_PARENT_CHANGED')
    return _observe(info['target'], info['backend'])[0]


def _final(info, observation):
    _known(info, 'final_observation', observation)
    baseline = info['receipt']['attempt_baseline']
    candidate = info['receipt']['prepared_candidate']
    candidate_observation = dict(exists=True, **candidate) if candidate is not None else None
    if info['receipt']['commit_state'] == 'REPLACED' and observation == candidate_observation:
        relation = 'MATCHES_CANDIDATE'
    elif baseline is not None and observation == baseline:
        relation = 'MATCHES_BASELINE'
    elif candidate is not None and observation == candidate_observation:
        relation = 'MATCHES_CANDIDATE'
    else:
        relation = 'OTHER'
    info['receipt']['final_relation'] = relation
    if baseline is not None and observation != baseline and info['receipt']['commit_state'] != 'REPLACED':
        info['receipt']['target_state'] = 'CHANGED'
    else:
        info['receipt']['target_state'] = 'PRESENT' if observation['exists'] else 'ABSENT'


def _cleanup_temp(info):
    path = info.get('temp')
    if path is None:
        return
    details = _check_chain(path, absent=True)
    if details is None:
        return
    if (_file_identity(details) != info['temp_identity']
            or _parent_identity(info['parent'], info['backend']) != info['parent_identity']):
        raise ValueError('D1_TEMP_IDENTITY_CHANGED')
    os.unlink(path)


def atomic_write_file(lease, prepare_candidate, validate_candidate):
    """Prepare, validate and replace exactly one file under an admitted lease."""
    info = _lease_info(lease)
    if info['used']:
        raise ValueError('D1_LEASE_ALREADY_USED')
    info['used'] = True
    stage = 'baseline'
    try:
        if not callable(prepare_candidate) or not callable(validate_candidate):
            raise ValueError('D1_CALLBACK_REQUIRED')
        baseline, identity = _observe(info['target'], info['backend'])
        _known(info, 'attempt_baseline', baseline)
        if baseline != info['entry'] or identity != info['entry_identity']:
            info['receipt']['target_state'] = 'CHANGED'
            raise ValueError('D1_TARGET_CHANGED')
        _publish(info)
        stage = 'preparation'
        temp = ntpath.join(info['parent'], '.d1-' + os.urandom(16).hex() + '.tmp')
        descriptor = os.open(temp, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_BINARY, 0o600)
        info['temp'] = temp
        try:
            info['temp_identity'] = _file_identity(os.fstat(descriptor))
            writer = os.fdopen(descriptor, 'w+b')
        except BaseException:
            _secondary(info, 'cleanup', 'D1_TEMP_CLOSE_FAILED', lambda: os.close(descriptor))
            raise
        try:
            prepare_candidate(writer)
            writer.flush()
            os.fsync(writer.fileno())
        except BaseException:
            _secondary(info, 'cleanup', 'D1_TEMP_CLOSE_FAILED', writer.close)
            raise
        writer.close()
        candidate, candidate_identity = _observe(temp, info['backend'])
        if candidate_identity != info['temp_identity']:
            raise ValueError('D1_TEMP_IDENTITY_CHANGED')
        _known(info, 'prepared_candidate', {key: candidate[key] for key in ('sha256', 'bytes')})
        stage = 'diagnostics'
        _event(info, 'd1_temp_prepared', mandatory=True)
        stage = 'candidate_validation'
        if _validate_file(temp, candidate, candidate_identity, validate_candidate) is not True:
            raise ValueError('D1_CANDIDATE_REJECTED')
        stage = 'diagnostics'
        _event(info, 'd1_temp_validated', mandatory=True)
        _event(info, 'd1_replace_precommit', mandatory=True)
        _barrier(info, 'PRECOMMIT_CONFIRMED')
        stage = 'precommit'
        if _parent_identity(info['parent'], info['backend']) != info['parent_identity']:
            raise ValueError('D1_PARENT_CHANGED')
        if _observe(temp, info['backend']) != (candidate, candidate_identity):
            raise ValueError('D1_TEMP_CHANGED')
        if _observe(info['target'], info['backend']) != (baseline, identity):
            raise ValueError('D1_TARGET_CHANGED')
        stage = 'diagnostics'
        _healthy(info)
        stage = 'replace'
        info['receipt']['commit_state'] = 'UNKNOWN'
        _publish(info)
        os.replace(temp, info['target'])
        # Sticky syscall fact публикуется прежде readback/event/callback.
        # Последующий сбой не означает сохранность historical previous.
        info['receipt']['commit_state'] = 'REPLACED'
        _publish(info)
        stage = 'readback'
        final = _readback(lease)
        _final(info, final)
        if info['receipt']['final_relation'] != 'MATCHES_CANDIDATE':
            raise ValueError('D1_FINAL_MISMATCH')
        stage = 'diagnostics'
        _event(info, 'd1_file_replaced', mandatory=True)
        _barrier(info, 'POSTCOMMIT_CONFIRMED')
        info['receipt']['write_status'] = 'SUCCEEDED'
        _publish(info)
        return copy.deepcopy(info['receipt'])
    except BaseException as error:
        info['atomic_error'] = error
        info['receipt']['write_status'] = 'FAILED'
        if stage == 'readback':
            # Измеренный mismatch остаётся фактом; UNKNOWN нужен только без readback.
            if info['receipt']['final_observation'] is None:
                info['receipt']['final_relation'] = 'UNKNOWN'
                info['receipt']['target_state'] = 'UNKNOWN'
                info['receipt']['null_reasons']['final_observation'] = 'READBACK_FAILED'
        elif info['receipt']['final_observation'] is None:
            try:
                _final(info, _readback(lease))
            except BaseException:
                info['receipt']['null_reasons']['final_observation'] = 'READBACK_FAILED'
                info['receipt']['target_state'] = 'UNKNOWN'
                _error(info, 'readback', 'D1_FAILURE_READBACK_FAILED')
        if info['receipt']['commit_state'] == 'REPLACED':
            info['receipt']['diagnostics_state'] = 'INCOMPLETE'
            info['diagnostics_failed'] = True
        _failure(info, stage, error, 'D1_' + stage.upper() + '_FAILED')
        _secondary(info, 'cleanup', 'D1_TEMP_CLEANUP_FAILED', lambda: _cleanup_temp(info))
        _publish(info)
        # bare raise сохраняет тот же исходный application exception/traceback;
        # diagnostics и cleanup только дополняют typed secondary errors.
        raise
