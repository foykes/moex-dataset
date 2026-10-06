"""Explicit, bounded F-LOG runtime. Importing this module opens no resources.

F0 event/error records are separate from the private queue protocol. A parent
owns the single writer; callers own worker processes and report their exits.
"""

import datetime
import hashlib
import importlib.metadata
import json
import logging
import math
import multiprocessing
import os
from pathlib import Path
import queue
import re
import shutil
import stat
import struct
import sys
import threading
import time
from urllib.parse import urlsplit, urlunsplit
import uuid
import zipfile


DEFAULTS = dict(console_level='INFO', file_level='DEBUG', queue_size=1024,
    max_record_bytes=65536, max_workers=16, critical_deadline_s=1.0,
    shutdown_deadline_s=10.0, rotation_bytes=10 * 1024**2, rotation_segments=5,
    protected_bytes=10 * 1024**2, fallback_bytes_per_producer=1024**2,
    context_bytes=1024**2, summary_bytes=1024**2, retention_days=7,
    retained_runs=10, root_budget_bytes=1024**3, log_root='.f-log/logs')
LEVELS = dict(DEBUG=10, INFO=20, WARNING=30, ERROR=40)
EVENT_NULLABLE = ('run_id', 'producer_sha', 'process', 'worker', 'snapshot_id',
    'release_id', 'dataset_id', 'instrument', 'interval', 'page', 'file',
    'artifact_id', 'target_id', 'outcome', 'elapsed_ms', 'duration_ms',
    'counts', 'retries', 'error')
ERROR_NULLABLE = ('run_id', 'snapshot_id', 'dataset_id', 'instrument', 'interval',
    'page', 'file', 'artifact_id', 'target_id', 'endpoint', 'attempt', 'exception_type')
CATEGORIES = {'CAPACITY', 'CAPACITY_NOT_VERIFIED', 'QUOTA_429', 'TIMEOUT',
    'DISK_FULL', 'MEMORY_ERROR', 'ACCESS', 'SOURCE', 'QUALITY', 'CONFIG',
    'IO', 'REMOTE', 'UNKNOWN'}
CAPACITY_KEYS = {'assessment', 'horizon', 'candidate_snapshot_id',
    'saved_candidate_artifact_ids', 'saved_candidate_rows', 'previous_release_id',
    'previous_as_of', 'previous_verification', 'previous_freshness',
    'preservation_outcome', 'preservation_evidence_ref', 'export_status',
    'publication_status', 'readback_status', 'observed_generation',
    'failed_required_exports', 'release_status', 'exit_status'}
CRITICAL_EVENTS = {'stage_started', 'stage_returned', 'stage_failed',
    'stage_interrupted', 'worker_completed', 'worker_failed', 'worker_interrupted',
    'run_completed', 'run_failed', 'run_interrupted'}
STAGES = ('data_gathering', 'dividends', 'tech', 'dohodru_data', 'upload')
ACK = struct.Struct('!BQIB')  # version, producer token, sequence, success
_clock = time.monotonic


def _utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')


def _versions():
    versions = {'python': sys.version.split()[0], 'platform': sys.platform}
    reasons = {}
    # Distribution metadata only; never import a project/live dependency here.
    for name in ('numpy', 'pandas', 'requests', 'beautifulsoup4', 'TA-Lib',
                 'pygsheets', 'lxml', 'html5lib', 'openpyxl'):
        try:
            versions[name] = importlib.metadata.version(name)
        except (importlib.metadata.PackageNotFoundError, OSError, ValueError):
            versions[name] = None
            reasons['versions.' + name] = 'VERSION_NOT_VERIFIED'
    return versions, reasons


def _bytes(value):
    return (json.dumps(value, ensure_ascii=False, allow_nan=False,
                       separators=(',', ':')) + '\n').encode('utf-8')


def _positive(value, integer=False):
    try:
        return (type(value) is int if integer else type(value) in (int, float)) and math.isfinite(value) and value > 0
    except OverflowError:
        return False


def _reserve(config):
    return (config['rotation_bytes'] * config['rotation_segments'] +
            config['protected_bytes'] + config['fallback_bytes_per_producer'] *
            (config['max_workers'] + 1) + config['context_bytes'] + config['summary_bytes'] + 65)


def validate_logging_config(config=None):
    """Pure validation; errors never echo untrusted keys or values."""
    if config is not None and type(config) is not dict:
        raise ValueError('LOG_CONFIG_INVALID')
    result = dict(DEFAULTS)
    if config:
        if set(config) - set(DEFAULTS):
            raise ValueError('LOG_CONFIG_UNKNOWN_KEY')
        result.update(config)
    for key, value in result.items():
        if key.endswith('_level'):
            if type(value) is not str or value not in LEVELS:
                raise ValueError('LOG_CONFIG_LEVEL')
        elif key == 'log_root':
            if (type(value) is not str or not value or '\\' in value or ':' in value
                    or not value.startswith('.f-log/') or '..' in value.split('/')
                    or any(p in ('', '.') for p in value.split('/'))):
                raise ValueError('LOG_CONFIG_ROOT')
        elif not _positive(value, integer=not key.endswith('_s')):
            raise ValueError('LOG_CONFIG_BOUND')
    if result['max_record_bytes'] > 65536 or result['max_workers'] > 16 or result['rotation_segments'] > 5:
        raise ValueError('LOG_CONFIG_BOUND')
    if result['rotation_bytes'] < result['max_record_bytes'] or result['protected_bytes'] < result['max_record_bytes']:
        raise ValueError('LOG_CONFIG_RECORD_RESERVE')
    if _reserve(result) * result['retained_runs'] > result['root_budget_bytes']:
        raise ValueError('LOG_CONFIG_ROOT_RESERVE')
    return result


def _safe_path(path, boundary):
    path, boundary = Path(os.path.abspath(path)), Path(os.path.abspath(boundary))
    if not path.is_relative_to(boundary) or boundary.resolve() != boundary:
        raise ValueError('LOG_PATH_UNSAFE')
    current = boundary
    for part in ('', *path.relative_to(boundary).parts):
        if part:
            current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            continue
        if (stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT
                or stat.S_ISREG(info.st_mode) and info.st_nlink != 1):
            raise ValueError('LOG_PATH_ALIAS')
    return path


def _tree_files(root, boundary):
    files = []
    for directory, dirs, names in os.walk(root, followlinks=False):
        for name in dirs + names:
            path = _safe_path(Path(directory) / name, boundary)
            if path.is_file():
                files.append(path)
    return files


def _secret_key(key):
    key = re.sub(r'[_\-\s]', '', key).casefold()
    return any(word in key for word in ('password', 'passwd', 'token', 'secret',
        'credential', 'authorization', 'authentication', 'cookie', 'apikey', 'privatekey')) or key in {'key', 'auth'}


def _text(context, text):
    for secret in context['_secrets']:
        text = text.replace(secret, '<redacted>')
    # Redact the remainder of a credential-bearing line. This also handles
    # Bearer/Basic headers, quoted JSON and malformed/unclosed quoted values
    # without a backtracking parser or interpolation of arbitrary objects.
    text = re.sub(r'''(?im)\b(password|passwd|token|secret|api[_ -]?key|private[_ -]?key|authorization|authentication|proxy-authorization|cookie|set-cookie|credentials?)["']?\s*[:=]\s*[^\r\n]+''',
                  lambda match: match.group(1) + '=<redacted>', text)

    def url(match):
        try:
            parts = urlsplit(match.group(0))
            host = parts.hostname or 'invalid'
            if parts.scheme == 'file':
                return '<external>'
            if host in {'docs.google.com', 'drive.google.com', 'sheets.googleapis.com', 'www.googleapis.com', 'drive.googleapis.com'}:
                return parts.scheme + '://' + host + '/<private-target>'
            if parts.scheme == 'ftp' or host == 'localhost' or re.fullmatch(r'\d+\.\d+\.\d+\.\d+', host):
                return parts.scheme + '://<private-target>'
            if host not in {'iss.moex.com', 'moex.com', 'www.moex.com', 'www.dohod.ru'}:
                return parts.scheme + '://<private-target>'
            return urlunsplit((parts.scheme, host, parts.path, '', ''))
        except ValueError:
            return '<redacted-url>'
    text = re.sub(r'(?i)(?:https?|ftp|file)://[^\s<>"\']+', url, text)
    root = context.get('_checkout')
    if root:
        text = text.replace(str(root) + os.sep, '').replace(str(root).replace('\\', '/') + '/', '')
    text = re.sub(r'(?<![\w:/])[A-Za-z]:[\\/][^"\'<>\r\n]+', '<external>', text)
    text = re.sub(r'\\\\[^"\'<>\r\n]+', '<external>', text)
    text = re.sub(r'(?<![\w:/])/(?!/)[^"\'<>\r\n]+', '<external>', text)
    return text


def _sanitize(context, value, depth=0, budget=None):
    if budget is None:
        budget = [0]
    budget[0] += 1
    if depth > 8 or budget[0] > 256:
        raise ValueError('LOG_PAYLOAD_LIMIT')
    if value is None or type(value) in (bool, int):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if type(value) is str:
        if len(value) > context['config']['max_record_bytes']:
            raise ValueError('LOG_PAYLOAD_LIMIT')
        return _text(context, value)
    if type(value) is list:
        return [_sanitize(context, item, depth + 1, budget) for item in value]
    if type(value) is dict and all(type(k) is str for k in value):
        return {_text(context, k): '<redacted>' if _secret_key(k) else _sanitize(context, v, depth + 1, budget)
                for k, v in value.items()}
    raise ValueError('LOG_PAYLOAD_TYPE')


def _secrets(values):
    if not isinstance(values, (tuple, list)) or len(values) > 256 or any(type(v) is not str or not v or len(v) > 65536 for v in values):
        raise ValueError('LOG_SENSITIVE_VALUES_INVALID')
    return tuple(sorted(set(values), key=len, reverse=True))


def make_error(context, error, stage, *, code='APPLICATION_ERROR'):
    """No repr/str(exception), traceback locals or source lines are evaluated."""
    message = type(error).__name__
    for arg in error.args:
        try:
            value = _sanitize(context, arg)
            message += ': ' + json.dumps(value, ensure_ascii=False, allow_nan=False)
        except (ValueError, TypeError):
            message += ': <unsupported>'
    trace = error.__traceback__
    while trace:
        frame = trace.tb_frame.f_code
        message += '\n' + _text(context, frame.co_filename) + ':' + str(trace.tb_lineno) + ' ' + _text(context, frame.co_name)
        trace = trace.tb_next
    record = dict(category='UNKNOWN', code=code, stage=stage, retryable=False,
                  final=True, message=_text(context, message))
    record.update({k: None for k in ERROR_NULLABLE})
    record.update(run_id=context['run_id'], exception_type=type(error).__name__)
    record['null_reasons'] = {k: 'NOT_AVAILABLE_OR_NOT_APPLICABLE' for k in ERROR_NULLABLE if record[k] is None}
    # Exception messages do not bypass the event-size ceiling.
    if len(_bytes(record)) > context['config']['max_record_bytes'] // 2:
        record['message'] = 'EXCEPTION_MESSAGE_TOO_LARGE; ' + type(error).__name__
    return record


def _validate_error(record):
    expected = {'category', 'code', 'stage', 'retryable', 'final', 'message', 'null_reasons', *ERROR_NULLABLE}
    if type(record) is not dict or set(record) != expected or type(record['category']) is not str or record['category'] not in CATEGORIES:
        raise ValueError('LOG_ERROR_SCHEMA')
    if type(record['retryable']) is not bool or type(record['final']) is not bool:
        raise ValueError('LOG_ERROR_SCHEMA')
    for key in ('code', 'stage', 'message'):
        if type(record[key]) is not str or not record[key]:
            raise ValueError('LOG_ERROR_SCHEMA')
    for key in ERROR_NULLABLE:
        value = record[key]
        if value is None:
            if type(record['null_reasons']) is not dict or not record['null_reasons'].get(key):
                raise ValueError('LOG_ERROR_NULL_REASON')
        elif key in ('interval', 'attempt'):
            if type(value) is not int or value < 0:
                raise ValueError('LOG_ERROR_SCHEMA')
        elif key == 'page':
            if type(value) not in (str, int) or type(value) is int and value < 0:
                raise ValueError('LOG_ERROR_SCHEMA')
        elif type(value) is not str:
            raise ValueError('LOG_ERROR_SCHEMA')
    _null_reasons(record, ERROR_NULLABLE)


def _null_reasons(record, keys):
    reasons = record['null_reasons']
    if (type(reasons) is not dict or set(reasons) - set(keys)
            or any(type(v) is not str or not v for v in reasons.values())
            or any(k not in reasons for k in keys if record[k] is None)):
        raise ValueError('LOG_NULL_REASON')


def _optional_reasons(record):
    if 'null_reasons' in record and (type(record['null_reasons']) is not dict or
            any(type(k) is not str or type(v) is not str or not v for k, v in record['null_reasons'].items())):
        raise ValueError('LOG_NULL_REASON')


def _validate_capacity(capacity):
    _optional_reasons(capacity)
    enums = dict(horizon={'10years', '30years', 'all_time'},
        previous_verification={'VERIFIED', 'UNVERIFIED', 'ABSENT', 'UNKNOWN'},
        previous_freshness={'CURRENT', 'STALE', 'UNKNOWN'},
        preservation_outcome={'NOT_TOUCHED', 'PRESERVED_VERIFIED', 'CHANGED_VERIFIED', 'UNKNOWN', 'NOT_APPLICABLE'},
        export_status={'NOT_RUN', 'SUCCEEDED', 'FAILED'},
        publication_status={'NOT_ATTEMPTED', 'ACKNOWLEDGED', 'FAILED', 'UNKNOWN'},
        readback_status={'NOT_ATTEMPTED', 'VERIFIED', 'MISMATCH', 'FAILED', 'UNKNOWN'},
        observed_generation={'CANDIDATE', 'PREVIOUS', 'EMPTY', 'OTHER', 'UNKNOWN'},
        release_status={'CANDIDATE', 'BLOCKED', 'PUBLISHING', 'CURRENT_VERIFIED', 'PARTIAL_REMOTE', 'REMOTE_UNKNOWN'})
    for key, choices in enums.items():
        if type(capacity[key]) is not str or capacity[key] not in choices:
            raise ValueError('LOG_CAPACITY_ENUM')
    for key in ('saved_candidate_artifact_ids', 'failed_required_exports'):
        if type(capacity[key]) is not list or any(type(v) is not str for v in capacity[key]):
            raise ValueError('LOG_CAPACITY_SCHEMA')
    for key in ('saved_candidate_rows', 'exit_status'):
        if capacity[key] is not None and (type(capacity[key]) is not int or key == 'saved_candidate_rows' and capacity[key] < 0):
            raise ValueError('LOG_CAPACITY_SCHEMA')
    for key in ('candidate_snapshot_id', 'previous_release_id', 'previous_as_of', 'preservation_evidence_ref'):
        if capacity[key] is not None and type(capacity[key]) is not str:
            raise ValueError('LOG_CAPACITY_SCHEMA')
    assessment = capacity['assessment']
    required = {'format', 'status', 'capabilities_ref', 'source_shape', 'required_rows', 'required_columns',
        'required_cells', 'row_limit', 'column_limit', 'cell_limit', 'workbook_cells_before',
        'workbook_cells_after', 'limit_evidence_ref', 'grid_evidence_ref', 'errors'}
    if type(assessment) is not dict or not required <= set(assessment) or set(assessment) - required - {'null_reasons'}:
        raise ValueError('LOG_ASSESSMENT_SCHEMA')
    _optional_reasons(assessment)
    if type(assessment['format']) is not str or type(assessment['status']) is not str or assessment['format'] not in {'csv', 'xlsx', 'google_sheets'} or assessment['status'] not in {'PASS', 'FAIL', 'NOT_VERIFIED'}:
        raise ValueError('LOG_ASSESSMENT_ENUM')
    shape = assessment['source_shape']
    if type(shape) is not dict or set(shape) - {'data_rows', 'data_columns', 'header_rows', 'index_columns', 'null_reasons'} or not {'data_rows', 'data_columns', 'header_rows', 'index_columns'} <= set(shape):
        raise ValueError('LOG_SHAPE_SCHEMA')
    _optional_reasons(shape)
    for key in ('data_rows', 'data_columns', 'header_rows', 'index_columns'):
        if shape[key] is not None and (type(shape[key]) is not int or shape[key] < 0):
            raise ValueError('LOG_SHAPE_SCHEMA')
    if shape['data_columns'] == 0 or shape['header_rows'] is None or shape['index_columns'] is None:
        raise ValueError('LOG_SHAPE_SCHEMA')
    if type(assessment['capabilities_ref']) is not str or not assessment['capabilities_ref']:
        raise ValueError('LOG_ASSESSMENT_SCHEMA')
    for key in ('limit_evidence_ref', 'grid_evidence_ref'):
        if assessment[key] is not None and type(assessment[key]) is not str:
            raise ValueError('LOG_ASSESSMENT_SCHEMA')
    for key in ('required_rows', 'required_columns', 'required_cells', 'row_limit', 'column_limit', 'cell_limit', 'workbook_cells_before', 'workbook_cells_after'):
        value = assessment[key]
        if value is not None and (type(value) is not int or value < 0 or key in {'required_columns', 'row_limit', 'column_limit', 'cell_limit'} and value == 0):
            raise ValueError('LOG_ASSESSMENT_SCHEMA')
    if type(assessment['errors']) is not list:
        raise ValueError('LOG_ASSESSMENT_SCHEMA')
    for error in assessment['errors']:
        _validate_error(error)


def _event(context, level, event, message, fields):
    if type(level) is not str or level not in LEVELS or type(event) is not str or type(message) is not str or type(fields) is not dict:
        raise ValueError('LOG_EVENT_SCHEMA')
    if set(fields) - ({'stage', 'fields', 'null_reasons'} | set(EVENT_NULLABLE)):
        raise ValueError('LOG_EVENT_UNKNOWN_FIELD')
    result = dict(timestamp_utc=_utc(), level=level, event=event, stage='runtime', message=message)
    result.update({k: None for k in EVENT_NULLABLE})
    result.update(run_id=context['run_id'], producer_sha=context['producer_sha'],
                  process=str(os.getpid()), worker=context['worker_id'], fields={})
    result.update(fields)
    if result['run_id'] != context['run_id'] or result['producer_sha'] != context['producer_sha'] or result['worker'] != context['worker_id']:
        raise ValueError('LOG_EVENT_CORRELATION')
    reasons = {k: 'NOT_AVAILABLE_OR_NOT_APPLICABLE' for k in EVENT_NULLABLE if result[k] is None}
    if type(result.get('null_reasons', {})) is not dict:
        raise ValueError('LOG_NULL_REASON')
    reasons.update(result.get('null_reasons', {}))
    result['null_reasons'] = reasons
    expected_keys = set(result)
    result = _sanitize(context, result)
    if set(result) != expected_keys or result['level'] not in LEVELS:
        raise ValueError('LOG_EVENT_SANITIZED_SCHEMA')
    for key in ('stage', 'event', 'message'):
        if type(result[key]) is not str or not result[key]:
            raise ValueError('LOG_EVENT_SCHEMA')
    for key in EVENT_NULLABLE:
        value = result[key]
        if value is None:
            continue
        if key in ('elapsed_ms', 'duration_ms'):
            if type(value) not in (int, float) or value < 0:
                raise ValueError('LOG_EVENT_TIMING')
        elif key == 'interval':
            if type(value) is not int or value <= 0:
                raise ValueError('LOG_EVENT_INTERVAL')
        elif key == 'page':
            if type(value) not in (int, str) or type(value) is int and value < 0:
                raise ValueError('LOG_EVENT_PAGE')
        elif key == 'retries':
            if (type(value) is not dict or set(value) != {'attempt', 'limit'} or
                    type(value['attempt']) is not int or value['attempt'] < 0 or not _positive(value['limit'], True)):
                raise ValueError('LOG_EVENT_RETRIES')
        elif key == 'counts':
            keys = {'rows_received', 'rows_accepted', 'rows_quarantined', 'pages', 'instruments'}
            if type(value) is not dict or not keys <= set(value) or set(value) - keys - {'null_reasons'} or any(value[k] is not None and (type(value[k]) is not int or value[k] < 0) for k in keys):
                raise ValueError('LOG_EVENT_COUNTS')
            _optional_reasons(value)
        elif key == 'error':
            _validate_error(value)
        elif type(value) is not str:
            raise ValueError('LOG_EVENT_SCHEMA')
    additions = result['fields']
    _null_reasons(result, EVENT_NULLABLE)
    if type(additions) is not dict or set(additions) - {'capacity'}:
        raise ValueError('LOG_EVENT_ADDITIONS')
    if additions:
        capacity = additions['capacity']
        if type(capacity) is not dict or not CAPACITY_KEYS <= set(capacity) or set(capacity) - CAPACITY_KEYS - {'null_reasons'} or level != 'ERROR':
            raise ValueError('LOG_CAPACITY_SCHEMA')
        _validate_capacity(capacity)
        if not all(result[k] is not None for k in ('run_id', 'snapshot_id', 'dataset_id', 'artifact_id', 'target_id', 'interval')):
            raise ValueError('LOG_CAPACITY_CORRELATION')
        if capacity['candidate_snapshot_id'] != result['snapshot_id'] or result['error'] is None or result['error']['category'] not in {'CAPACITY', 'CAPACITY_NOT_VERIFIED'}:
            raise ValueError('LOG_CAPACITY_CORRELATION')
        if any(result['error'][key] != result[key] for key in ('run_id', 'snapshot_id', 'dataset_id', 'artifact_id', 'target_id', 'interval')):
            raise ValueError('LOG_CAPACITY_CORRELATION')
        wanted = 'FAIL' if result['error']['category'] == 'CAPACITY' else 'NOT_VERIFIED'
        if capacity['assessment']['status'] != wanted:
            raise ValueError('LOG_CAPACITY_ASSESSMENT_STATUS')
        if capacity['release_status'] != 'BLOCKED' or capacity['exit_status'] == 0:
            raise ValueError('LOG_CAPACITY_STATUS')
    if len(_bytes(result)) > context['config']['max_record_bytes']:
        raise ValueError('LOG_RECORD_TOO_LARGE')
    return result


def _write(handle, data):
    handle.write(data)
    handle.flush()


def _json_file(path, value, limit, *, exclusive=False):
    data = _bytes(value)
    if len(data) > limit:
        raise ValueError('LOG_DOCUMENT_TOO_LARGE')
    with path.open('xb' if exclusive else 'wb') as handle:
        _write(handle, data)


def _inventory(root, boundary):
    result = []
    total = 0
    for path in sorted(_tree_files(root, boundary)):
        if path.name in {'run_summary.json', 'summary.sha256'} or path.name.endswith('.tmp'):
            continue
        with path.open('rb') as handle:
            data = handle.read(96 * 1024**2 + 1)
        total += len(data)
        if total > 96 * 1024**2:
            raise ValueError('LOG_INVENTORY_LIMIT')
        result.append(dict(file=path.relative_to(root).as_posix(), bytes=len(data), sha256=hashlib.sha256(data).hexdigest()))
    return result


def _prepare_root(checkout, config, run_id):
    root = _safe_path(checkout / config['log_root'], checkout)
    root.mkdir(parents=True, exist_ok=True)
    lock = _safe_path(root.with_name(root.name + '.allocation.lock'), checkout)
    # Allocate/prune under an exclusive filesystem reservation. A stale lock
    # is refused for explicit diagnosis, never silently removed.
    with lock.open('xb') as handle:
        _write(handle, b'FLOG_ALLOCATION_V1\n')
    try:
        return _allocate_root(checkout, config, run_id, root)
    finally:
        _safe_path(lock, checkout).unlink()


def _allocate_root(checkout, config, run_id, root):
    existing = []
    used = 0
    for path in root.iterdir():
        _safe_path(path, checkout)
        if not path.is_dir():
            raise ValueError('LOG_ROOT_FOREIGN_ENTRY')
        files = _tree_files(path, checkout)
        size = sum(p.stat().st_size for p in files)
        try:
            summary = _read_summary(path)
        except (OSError, ValueError):
            summary = None
        if summary is None or not summary['storage_sealed']:
            reservation = _reserve(config)
            try:
                context_path = _safe_path(path / 'context.json', checkout)
                if context_path.stat().st_size <= config['context_bytes']:
                    with context_path.open('rb') as handle:
                        previous_config = validate_logging_config(json.load(handle)['safe_config'])
                    reservation = max(reservation, _reserve(previous_config))
            except (OSError, ValueError, KeyError, TypeError):
                pass
            used += max(size, reservation)
        else:
            used += size
            existing.append((path, summary, size))
    existing.sort(key=lambda item: item[1]['finished_at_utc'], reverse=True)
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=config['retention_days'])
    for index, (path, summary, size) in enumerate(existing):
        old = datetime.datetime.fromisoformat(summary['finished_at_utc'].replace('Z', '+00:00')) < cutoff
        if index >= config['retained_runs'] - 1 or old:
            shutil.rmtree(path)  # only validated, sealed runs within this root
            used -= size
    if used + _reserve(config) > config['root_budget_bytes']:
        raise ValueError('LOG_ROOT_BUDGET_UNAVAILABLE')
    run_root = _safe_path(root / run_id, checkout)
    run_root.mkdir(exist_ok=False)
    return run_root


def _producer(bootstrap, sensitive_values):
    result = dict(**bootstrap, _secrets=_secrets(sensitive_values), _lock=threading.Lock(),
        _seq=0, _failed=False, _closed=False, _finish=None,
        _counts={level: 0 for level in LEVELS}, _drops=0,
        _logger=logging.Logger('moex.flog.' + str(bootstrap['_token']), logging.DEBUG))
    result['_logger'].addHandler(logging.NullHandler())
    return result


def configure_logging(checkout_root, *, run_id, environment, producer_sha,
                      config=None, sensitive_values=(), session=None):
    config = validate_logging_config(config)
    for value in (run_id, environment):
        if type(value) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}', value):
            raise ValueError('LOG_ID_INVALID')
    if type(producer_sha) is not str or not re.fullmatch(r'[0-9a-f]{7,40}', producer_sha):
        raise ValueError('LOG_SHA_INVALID')
    checkout = Path(os.path.abspath(checkout_root))
    secrets = _secrets(sensitive_values)
    structural = (*STAGES, *DEFAULTS, *EVENT_NULLABLE, *ERROR_NULLABLE, *CAPACITY_KEYS,
                  'timestamp_utc', 'level', 'event', 'stage', 'message', 'fields', 'null_reasons',
                  'kind', 'logging_context_version', 'versions', 'safe_config', 'environment', 'package')
    if any(secret in value for secret in secrets for value in (run_id, environment, producer_sha,
            *structural, *[v for v in config.values() if type(v) is str])):
        raise ValueError('LOG_ID_SENSITIVE')
    if session is not None:
        if (session['_state'] != 'ACTIVE' or session['config'] != config or session['_checkout'] != checkout
                or session['run_id'] != run_id or session['environment'] != environment
                or session['producer_sha'] != producer_sha or session['_secrets'] != secrets):
            raise ValueError('LOG_SESSION_CHANGED')
        return session
    started = _clock()
    versions, version_reasons = _versions()
    root = _prepare_root(checkout, config, run_id)
    spawn = multiprocessing.get_context('spawn')
    channel = spawn.Queue(config['queue_size'])
    receive, send = spawn.Pipe(duplex=False)
    token = uuid.uuid4().int & ((1 << 64) - 1)
    bootstrap = dict(run_id=run_id, environment=environment, producer_sha=producer_sha,
        config=config, worker_id=None, _checkout=checkout, _run_root=root,
        _queue=channel, _ack=receive, _token=token,
        # Windows x64 sticky byte: writers only store 1, never clear it. No
        # shared semaphore can be left locked by a terminated producer.
        _run_failed=spawn.RawValue('b', 0))
    context = _producer(bootstrap, secrets)
    context.update(_state='ACTIVE', _started=started, _started_utc=_utc(),
        _spawn=spawn, _ack_senders={token: send}, _receivers=[receive],
        _expected={}, _outcomes={}, _worker_finals={}, _listener_error=None,
        _written=0, _rotation=0, _errors=[], _capacity=[],
        _stages=[], _final=None, _stop=threading.Event(), _listener_closed=False,
        _terminal=None, _deadline=None, _registration_frozen=False, _finalizer_lock=threading.Lock())
    context['_logger'].propagate = False
    document = dict(kind='LOGGING_CONTEXT', logging_context_version=1, run_id=run_id,
        environment=environment, package='F-LOG-01', producer_sha=producer_sha,
        started_at_utc=context['_started_utc'], checkout_alias='checkout',
        versions=versions, safe_config=config, null_reasons=version_reasons)
    try:
        _json_file(root / 'context.json', _sanitize(context, document), config['context_bytes'], exclusive=True)
        # Open only explicitly owned files before launching the listener.
        context['_primary'] = (root / 'events.jsonl').open('xb')
        context['_protected'] = (root / 'protected.jsonl').open('xb')
    except BaseException:
        for name in ('_primary', '_protected'):
            if name in context:
                context[name].close()
        channel.cancel_join_thread()
        channel.close()
        receive.close()
        send.close()
        raise
    context['_thread'] = threading.Thread(target=_listen, args=(context,), name='FLOG-writer', daemon=True)
    context['_thread'].start()
    return context


def prepare_worker(session, worker_id):
    if session['_state'] != 'ACTIVE' or session['_registration_frozen'] or type(worker_id) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', worker_id):
        raise ValueError('LOG_WORKER_INVALID')
    if any(secret in worker_id for secret in session['_secrets']):
        raise ValueError('LOG_WORKER_SENSITIVE')
    if worker_id in session['_expected'] or len(session['_expected']) >= session['config']['max_workers']:
        raise ValueError('LOG_WORKER_REGISTRATION')
    receive, send = session['_spawn'].Pipe(duplex=False)
    token = uuid.uuid4().int & ((1 << 64) - 1)
    session['_ack_senders'][token] = send
    session['_receivers'].append(receive)
    session['_expected'][worker_id] = token
    return dict(run_id=session['run_id'], environment=session['environment'],
        producer_sha=session['producer_sha'], config=session['config'], worker_id=worker_id,
        _checkout=session['_checkout'], _run_root=session['_run_root'],
        _queue=session['_queue'], _ack=receive, _token=token, _run_failed=session['_run_failed'])


def configure_worker(bootstrap, *, sensitive_values=()):
    result = _producer(bootstrap, sensitive_values)
    if any(secret in result['worker_id'] or secret in result['run_id'] for secret in result['_secrets']):
        raise ValueError('LOG_WORKER_SENSITIVE')
    result['_logger'].propagate = False
    return result


def _fallback(context, record, sequence):
    context['_failed'] = True
    context['_run_failed'].value = 1
    line = _bytes(dict(producer=context['_token'], sequence=sequence,
                       unconfirmed=True, may_duplicate=True, event=record))
    try:
        sys.stderr.write(record['level'] + ' ' + record['message'] + ' [delivery unconfirmed]\n')
        sys.stderr.flush()
    except (OSError, ValueError):
        pass
    try:
        path = _safe_path(context['_run_root'] / ('fallback-' + str(context['_token']) + '.jsonl'), context['_checkout'])
        if (path.stat().st_size if path.exists() else 0) + len(line) > context['config']['fallback_bytes_per_producer']:
            return
        with path.open('ab') as handle:
            _write(handle, line)
    except (OSError, ValueError):
        pass


def _send(context, kind, record=None, *, critical=True, payload=None):
    deadline = _clock() + context['config']['critical_deadline_s']
    if context.get('_deadline') is not None:
        deadline = min(deadline, context['_deadline'])
    ok, sequence = False, None
    acquired = context['_lock'].acquire(timeout=max(0, deadline - _clock()))
    if acquired:
        try:
            context['_seq'] += 1
            sequence = context['_seq']
            wrapper = dict(version=1, token=context['_token'], sequence=sequence,
                           kind=kind, critical=critical, event=record, payload=payload)
            if not context['_failed']:
                try:
                    if critical:
                        context['_queue'].put(wrapper, timeout=max(0, deadline - _clock()))
                        if context['_ack'].poll(max(0, deadline - _clock())):
                            frame = context['_ack'].recv_bytes(maxlength=64)
                            ok = frame == ACK.pack(1, context['_token'], sequence, 1)
                    else:
                        context['_queue'].put_nowait(wrapper)
                        ok = True
                except (queue.Full, OSError, ValueError, EOFError):
                    pass
        finally:
            if not ok:
                context['_failed'] = True
                context['_run_failed'].value = 1
            context['_lock'].release()
    if not ok:
        context['_failed'] = True
        context['_run_failed'].value = 1
        if record is not None:
            if critical:
                _fallback(context, record, sequence if sequence is not None else 'unsent-' + uuid.uuid4().hex)
            else:
                context['_drops'] += 1
    return dict(confirmed=ok if critical else False, accepted=ok,
                producer=context['_token'], sequence=sequence)


def emit_event(context, level, event, message, fields):
    if context['_closed'] or context.get('_state', 'ACTIVE') != 'ACTIVE' or context.get('_registration_frozen', False):
        raise ValueError('LOG_CONTEXT_CLOSED')
    if event in {'run_completed', 'run_failed', 'run_interrupted', 'worker_completed', 'worker_failed', 'worker_interrupted'}:
        raise ValueError('LOG_LIFECYCLE_RESERVED')
    return _emit_record(context, level, event, message, fields)


def _emit_record(context, level, event, message, fields):
    record = _event(context, level, event, message, fields)
    context['_counts'][level] += 1
    # This is the first LogRecord boundary. It contains only sanitized primitives.
    safe_record = logging.LogRecord(context['_logger'].name, LEVELS[level], '', 0,
                                    record['message'], (), None)
    safe_record.flog_event = record
    context['_logger'].handle(safe_record)
    return _send(context, 'event', record, critical=level == 'ERROR' or event in CRITICAL_EVENTS)


def flush_logging(context):
    if context['_closed'] or context.get('_registration_frozen', False) or context.get('_state', 'ACTIVE') != 'ACTIVE':
        return dict(confirmed=False, accepted=False)
    return _send(context, 'barrier')


def _rotate(session, data):
    handle = session['_primary']
    if handle.tell() + len(data) <= session['config']['rotation_bytes']:
        return
    handle.flush()
    handle.close()
    root = session['_run_root']
    if session['config']['rotation_segments'] == 1:
        _safe_path(root / 'events.jsonl', session['_checkout']).unlink()
    for index in range(session['config']['rotation_segments'] - 1, 0, -1):
        previous = root / ('events.jsonl' if index == 1 else 'events.' + str(index - 1) + '.jsonl')
        destination = root / ('events.' + str(index) + '.jsonl')
        if previous.exists():
            _safe_path(previous, session['_checkout'])
            _safe_path(destination, session['_checkout'])
            os.replace(previous, destination)
    session['_primary'] = (root / 'events.jsonl').open('xb')
    session['_rotation'] += 1


def _write_record(session, wrapper):
    record = wrapper['event']
    data = _bytes(record)
    critical = wrapper['critical']
    if critical or LEVELS[record['level']] >= LEVELS[session['config']['file_level']]:
        _rotate(session, data)
        _write(session['_primary'], data)
        session['_written'] += 1
    if critical:
        if session['_protected'].tell() + len(data) > session['config']['protected_bytes']:
            raise OSError('LOG_PROTECTED_BUDGET')
        _write(session['_protected'], data)
    if critical or LEVELS[record['level']] >= LEVELS[session['config']['console_level']]:
        sys.stderr.write(record['level'] + ' ' + record['message'] + '\n')
        sys.stderr.flush()
    if record['error'] is not None and record['error'] not in session['_errors']:
        session['_errors'].append(record['error'])
    if record['fields'].get('capacity'):
        session['_capacity'].append(record['fields']['capacity'])
    if len(_bytes({'errors': session['_errors'], 'capacity_reports': session['_capacity']})) > session['config']['summary_bytes'] // 2:
        raise ValueError('LOG_SUMMARY_METADATA_LIMIT')
    if record['event'].startswith('run_') and record['event'] in CRITICAL_EVENTS:
        session['_terminal'] = dict(file='protected.jsonl', offset=session['_protected'].tell() - len(data),
                                   bytes=len(data), sha256=hashlib.sha256(data).hexdigest(), event=record['event'])


def _listen(session):
    last = {}
    try:
        while not session['_stop'].is_set():
            try:
                wrapper = session['_queue'].get(timeout=0.05)
            except queue.Empty:
                continue
            token, sequence = wrapper['token'], wrapper['sequence']
            if wrapper['version'] != 1 or token not in session['_ack_senders'] or sequence <= last.get(token, 0):
                raise ValueError('LOG_TRANSPORT_SEQUENCE')
            last[token] = sequence
            if wrapper['event'] is not None:
                _write_record(session, wrapper)
            if wrapper['kind'] == 'worker_final':
                session['_worker_finals'][token] = wrapper['payload']
            if wrapper['critical']:
                for handle in (session['_primary'], session['_protected']):
                    handle.flush()
                session['_ack_senders'][token].send_bytes(ACK.pack(1, token, sequence, 1))
    except BaseException as error:
        # Only a stable type/code is recorded, never the raw sink exception.
        session['_listener_error'] = 'LOG_LISTENER_' + type(error).__name__
    finally:
        for name in ('_primary', '_protected'):
            try:
                session[name].flush()
            except BaseException:
                session['_listener_error'] = 'LOG_FLUSH_FAILED'
            try:
                session[name].close()
            except BaseException:
                session['_listener_error'] = 'LOG_CLOSE_FAILED'
        session['_listener_closed'] = all(session[n].closed for n in ('_primary', '_protected'))


def finish_worker(producer, *, execution_outcome, exit_status):
    if producer['_finish'] is not None:
        return producer['_finish']
    _execution(execution_outcome, exit_status)
    producer['_registration_frozen'] = True
    suffix = {'COMPLETED': 'completed', 'INTERRUPTED': 'interrupted'}.get(execution_outcome, 'failed')
    receipt = _emit_record(producer, 'INFO' if exit_status == 0 else 'ERROR',
        'worker_' + suffix, 'Worker execution ' + execution_outcome, {'outcome': execution_outcome})
    payload = dict(execution_outcome=execution_outcome, exit_status=exit_status,
                   counts=dict(producer['_counts']), drops=producer['_drops'])
    barrier = _send(producer, 'worker_final', payload=payload)
    producer['_closed'] = True
    producer['_finish'] = dict(**payload, confirmed=receipt['confirmed'] and barrier['confirmed'])
    try:
        producer['_ack'].close()
        producer['_queue'].cancel_join_thread()
        producer['_queue'].close()
    except (OSError, ValueError):
        producer['_run_failed'].value = 1
        producer['_finish']['confirmed'] = False
        producer['_finish']['cleanup_error'] = 'LOG_WORKER_CLOSE_FAILED'
    return producer['_finish']


def _execution(outcome, exit_status):
    if type(outcome) is not str or outcome not in {'COMPLETED', 'FAILED', 'INTERRUPTED', 'UNKNOWN'} or type(exit_status) is not int:
        raise ValueError('LOG_EXECUTION_INVALID')
    if outcome in {'FAILED', 'UNKNOWN'} and exit_status == 0 or outcome == 'COMPLETED' and exit_status != 0:
        raise ValueError('LOG_EXECUTION_CONTRADICTION')


def record_worker_outcome(session, worker_id, *, started, exit_status, timed_out=False):
    if session['_state'] != 'ACTIVE' or session['_registration_frozen'] or worker_id not in session['_expected'] or type(started) is not bool or type(timed_out) is not bool:
        raise ValueError('LOG_WORKER_OUTCOME_INVALID')
    if exit_status is not None and type(exit_status) is not int:
        raise ValueError('LOG_WORKER_OUTCOME_INVALID')
    session['_outcomes'][worker_id] = dict(started=started, exit_status=exit_status, timed_out=timed_out)


def check_logging_health(session):
    if session.get('_final') is not None:
        return dict(healthy=session['_final']['delivery_outcome'] == 'COMPLETE', state='FINALIZED')
    healthy = not session['_failed'] and session['_run_failed'].value == 0 and session['_listener_error'] is None and session['_thread'].is_alive()
    return dict(healthy=healthy, state=session['_state'], code=None if healthy else 'LOGGING_INCOMPLETE')


def initialize_main_stages(session):
    if session['_state'] != 'ACTIVE' or session['_registration_frozen'] or session['_stages']:
        raise ValueError('LOG_STAGE_LEDGER_INVALID')
    session['_stages'] = [dict(order=i, stage=name, execution_state='NOT_STARTED',
        elapsed_ms=None, duration_ms=None, error_ref=None, state_reason='NOT_REACHED')
        for i, name in enumerate(STAGES, 1)]


def observe_stage(session, stage, state, *, error=None, reason=None):
    if session['_state'] != 'ACTIVE' or session['_registration_frozen']:
        raise ValueError('LOG_STAGE_LEDGER_CLOSED')
    row = next(item for item in session['_stages'] if item['stage'] == stage)
    now = _clock()
    if state == 'STARTED':
        row.update(execution_state=state, elapsed_ms=max(0, (now - session['_started']) * 1000), state_reason=None)
        row['_started'] = now
    else:
        row.update(execution_state=state, duration_ms=max(0, (now - row.pop('_started')) * 1000), state_reason=None)
        if error is not None:
            error = _sanitize(session, error)
            _validate_error(error)
            if error not in session['_errors']:
                session['_errors'].append(error)
            row['error_ref'] = '#/errors/' + str(session['_errors'].index(error))
    if reason:
        for pending in session['_stages']:
            if pending['execution_state'] == 'NOT_STARTED':
                pending['state_reason'] = reason


def mark_unreached(session, reason):
    if session['_state'] != 'ACTIVE' or session['_registration_frozen']:
        raise ValueError('LOG_STAGE_LEDGER_CLOSED')
    for row in session['_stages']:
        if row['execution_state'] == 'NOT_STARTED' and row['state_reason'] == 'NOT_REACHED':
            row['state_reason'] = reason


def _collect_fallback(session):
    for path in session['_run_root'].glob('fallback-*.jsonl'):
        _safe_path(path, session['_checkout'])
        with path.open('rb') as handle:
            for line in handle:
                wrapper = json.loads(line)
                record = wrapper['event']
                if record['error'] is not None and record['error'] not in session['_errors']:
                    session['_errors'].append(record['error'])
                capacity = record['fields'].get('capacity')
                if capacity and capacity not in session['_capacity']:
                    session['_capacity'].append(capacity)


def _close_transport(session):
    session['_queue'].cancel_join_thread()
    session['_queue'].close()
    for endpoint in (*session['_receivers'], *session['_ack_senders'].values()):
        endpoint.close()


def finalize_logging(session, *, execution_outcome, exit_status,
                     application_error=None, release_result=None):
    if not session['_finalizer_lock'].acquire(blocking=False):
        raise ValueError('LOG_FINALIZATION_ACTIVE')
    try:
        return _finalize_logging(session, execution_outcome=execution_outcome,
            exit_status=exit_status, application_error=application_error, release_result=release_result)
    finally:
        session['_finalizer_lock'].release()


def _finalize_logging(session, *, execution_outcome, exit_status,
                     application_error=None, release_result=None):
    """Summary replacement is the last fallible operation on a successful run."""
    if session['_final'] is not None:
        return session['_final']
    _execution(execution_outcome, exit_status)
    if session['_state'] != 'ACTIVE':
        raise ValueError('LOG_FINALIZER_STATE')
    session['_deadline'] = _clock() + session['config']['shutdown_deadline_s']
    session['_registration_frozen'] = True
    complete = check_logging_health(session)['healthy']
    if application_error is not None:
        application_error = _sanitize(session, application_error)
        _validate_error(application_error)
        if application_error not in session['_errors']:
            session['_errors'].append(application_error)
    if release_result is not None:
        release_result = _sanitize(session, release_result)
    settled = True
    # Registration/outcomes freeze now. Only the parent's final may still emit.
    for worker, token in session['_expected'].items():
        outcome = session['_outcomes'].get(worker)
        if outcome is None or outcome['timed_out'] or outcome['started'] and outcome['exit_status'] is None:
            settled = False
        if outcome is None or not outcome['started'] or outcome['exit_status'] != 0 or outcome['timed_out']:
            complete = False
        # An actual exit alone does not prove queued records were delivered.
        if token not in session['_worker_finals']:
            complete = False
        elif session['_worker_finals'][token]['execution_outcome'] != 'COMPLETED' or session['_worker_finals'][token]['exit_status'] != 0:
            complete = False
    complete = _send(session, 'barrier')['confirmed'] and complete
    suffix = {'COMPLETED': 'completed', 'INTERRUPTED': 'interrupted'}.get(execution_outcome, 'failed')
    final = _emit_record(session, 'INFO' if execution_outcome == 'COMPLETED' else 'ERROR',
        'run_' + suffix, 'Run execution ' + execution_outcome,
        {'outcome': execution_outcome, 'error': application_error})
    complete = final['confirmed'] and complete
    session['_state'] = 'FINALIZING'
    session['_stop'].set()
    session['_thread'].join(max(0, session['_deadline'] - _clock()))
    sealed = settled and not session['_thread'].is_alive() and session['_listener_closed']
    complete = complete and sealed and session['_listener_error'] is None and session['_run_failed'].value == 0
    files = []
    summary_error = None
    try:
        _close_transport(session)
        _collect_fallback(session)
        files = _inventory(session['_run_root'], session['_checkout'])
    except (OSError, ValueError):
        complete = False
        sealed = False
        summary_error = 'LOG_SEAL_FAILED'
    levels = dict(session['_counts'])
    drops = session['_drops']
    missing = []
    for worker, token in session['_expected'].items():
        report = session['_worker_finals'].get(token)
        if report:
            for level in LEVELS:
                levels[level] += report['counts'][level]
            drops += report['drops']
        else:
            missing.append(worker)
    for row in session['_stages']:
        if row['execution_state'] == 'STARTED':
            row.pop('_started', None)
            row.update(execution_state='UNKNOWN', duration_ms=None, state_reason='TERMINAL_NOT_OBSERVED')
    summary = dict(kind='LOGGING_SUMMARY', logging_summary_version=1,
        run_id=session['run_id'], started_at_utc=session['_started_utc'], finished_at_utc=_utc(),
        run_duration_ms=max(0, (_clock() - session['_started']) * 1000),
        observed_stages=session['_stages'], execution_outcome=execution_outcome,
        delivery_outcome='COMPLETE' if complete and drops == 0 else 'INCOMPLETE',
        exit_status=exit_status if exit_status != 0 or complete and drops == 0 else 1,
        release_result=release_result, null_reasons={'release_result': 'NOT_REPORTED_BY_CALLER'} if release_result is None else {},
        event_counters={'written_primary': session['_written'], 'dropped': drops,
                        'scope': 'LOWER_BOUND' if missing else 'ALL_REPORTED_PRODUCERS'},
        logical_event_counts_by_level=levels, errors=session['_errors'], capacity_reports=session['_capacity'],
        expected_producers=[dict(worker_id='parent', final_confirmed=final['confirmed'])] +
            [dict(worker_id=worker, final_report=session['_worker_finals'].get(token)) for worker, token in session['_expected'].items()],
        worker_outcomes=session['_outcomes'],
        terminal_ref=session['_terminal'], retained_files=files, rotation_count=session['_rotation'],
        storage_sealed=sealed, evidence_incomplete=not complete or bool(missing) or drops > 0)
    if not session['_stages']:
        summary['null_reasons']['observed_stages'] = 'NO_MAIN_BOUNDARIES'
    if summary['terminal_ref'] is None:
        summary['null_reasons']['terminal_ref'] = 'TERMINAL_NOT_CONFIRMED'
    if summary['capacity_reports'] and summary['exit_status'] == 0:
        summary['exit_status'] = 1
    if summary_error:
        summary['null_reasons']['authoritative_summary'] = summary_error
    # Sealing failures never claim authoritative zero. Serialization and file
    # close failures can leave a temp file, but it is never authoritative.
    try:
        destination = session['_run_root'] / 'run_summary.json'
        temporary = session['_run_root'] / 'run_summary.json.tmp'
        # Seal the expected bytes BEFORE summary writing/replacement. This also
        # detects later accidental summary mutation without a post-final write.
        digest = hashlib.sha256(_bytes(summary)).hexdigest().encode('ascii') + b'\n'
        with (session['_run_root'] / 'summary.sha256').open('xb') as handle:
            _write(handle, digest)
        _json_file(temporary, summary, session['config']['summary_bytes'], exclusive=True)
        os.replace(temporary, destination)
    except (OSError, ValueError):
        summary.update(delivery_outcome='INCOMPLETE', evidence_incomplete=True,
                       exit_status=exit_status if exit_status else 1)
        summary_error = 'LOG_SUMMARY_FAILED'
    # Nothing below performs I/O. Cache freezes this logical finalization.
    session['_closed'] = True
    session['_state'] = 'FINALIZED'
    session['_final'] = summary
    if summary_error == 'LOG_SUMMARY_FAILED':
        summary['null_reasons']['authoritative_summary'] = summary_error
    return summary


def _read_summary(run_root):
    try:
        return _validated_summary(run_root)
    except (OSError, KeyError, TypeError, ValueError, OverflowError):
        raise ValueError('LOG_SUMMARY_INVALID') from None


def _validated_summary(run_root):
    root = Path(run_root)
    path = root / 'run_summary.json'
    _safe_path(path, root.parent)
    if path.stat().st_size > 1024**2:
        raise ValueError('LOG_SUMMARY_INVALID')
    with path.open('rb') as handle:
        data = handle.read()
    with _safe_path(root / 'summary.sha256', root).open('rb') as handle:
        expected_hash = handle.read(66)
    if expected_hash != hashlib.sha256(data).hexdigest().encode('ascii') + b'\n':
        raise ValueError('LOG_SUMMARY_HASH')
    summary = json.loads(data)
    required = {'kind', 'logging_summary_version', 'run_id', 'started_at_utc', 'finished_at_utc',
        'run_duration_ms', 'observed_stages', 'execution_outcome', 'delivery_outcome', 'exit_status',
        'release_result', 'null_reasons', 'event_counters', 'logical_event_counts_by_level', 'errors',
        'capacity_reports', 'expected_producers', 'worker_outcomes', 'terminal_ref', 'retained_files',
        'rotation_count', 'storage_sealed', 'evidence_incomplete'}
    if (summary.get('kind') != 'LOGGING_SUMMARY' or summary.get('logging_summary_version') != 1
            or set(summary) != required or summary.get('execution_outcome') not in {'COMPLETED', 'FAILED', 'INTERRUPTED', 'UNKNOWN'}
            or summary.get('delivery_outcome') not in {'COMPLETE', 'DEGRADED', 'INCOMPLETE'}
            or summary.get('run_id') != root.name or type(summary.get('storage_sealed')) is not bool
            or type(summary.get('exit_status')) is not int):
        raise ValueError('LOG_SUMMARY_INVALID')
    if _inventory(root, root.parent) != summary.get('retained_files'):
        raise ValueError('LOG_SUMMARY_INVENTORY')
    terminal = summary.get('terminal_ref')
    if terminal:
        with _safe_path(root / terminal['file'], root).open('rb') as handle:
            handle.seek(terminal['offset'])
            data = handle.read(terminal['bytes'])
        if hashlib.sha256(data).hexdigest() != terminal['sha256']:
            raise ValueError('LOG_SUMMARY_TERMINAL')
        event = json.loads(data)
        wanted = {'COMPLETED': 'run_completed', 'INTERRUPTED': 'run_interrupted'}.get(summary['execution_outcome'], 'run_failed')
        if event['run_id'] != summary['run_id'] or event['event'] != wanted or event['outcome'] != summary['execution_outcome']:
            raise ValueError('LOG_SUMMARY_TERMINAL')
    elif summary.get('delivery_outcome') == 'COMPLETE' or summary['exit_status'] == 0:
        raise ValueError('LOG_SUMMARY_TERMINAL')
    if summary['exit_status'] == 0:
        if (summary.get('delivery_outcome') != 'COMPLETE' or not summary['storage_sealed']
                or summary['execution_outcome'] in {'FAILED', 'UNKNOWN'}
                or summary.get('evidence_incomplete') or len(summary.get('worker_outcomes', {})) != len(summary.get('expected_producers', [])) - 1):
            raise ValueError('LOG_SUMMARY_SUCCESS_INVALID')
        for producer in summary['expected_producers'][1:]:
            worker = producer['worker_id']
            outcome = summary['worker_outcomes'].get(worker)
            report = producer['final_report']
            if not outcome or not outcome['started'] or outcome['timed_out'] or outcome['exit_status'] != 0 or not report or report['execution_outcome'] != 'COMPLETED' or report['exit_status'] != 0:
                raise ValueError('LOG_SUMMARY_WORKER')
    return summary


def export_diagnostic_bundle(run_root, destination):
    """Separate, explicit export of immutable, sealed evidence; no env copying."""
    root = Path(os.path.abspath(run_root))
    # Require an owned .f-log ancestor; neither arbitrary paths nor public data.
    boundary = next((p for p in root.parents if p.name == '.f-log'), None)
    if boundary is None:
        raise ValueError('LOG_BUNDLE_ROOT')
    _safe_path(root, boundary)
    summary = _read_summary(root)
    if not summary['storage_sealed']:
        raise ValueError('LOG_BUNDLE_ACTIVE')
    destination = _safe_path(destination, boundary)
    if not destination.is_relative_to(boundary / 'bundles') or destination.is_relative_to(root) or destination.exists():
        raise ValueError('LOG_BUNDLE_DESTINATION')
    allowed = re.compile(r'(?:context\.json|run_summary\.json|summary\.sha256|protected\.jsonl|events(?:\.[1-4])?\.jsonl|fallback-\d+\.jsonl)')
    entries = _tree_files(root, boundary)
    if any(p.parent != root or not allowed.fullmatch(p.name) for p in entries):
        raise ValueError('LOG_BUNDLE_ALLOWLIST')
    if sum(p.stat().st_size for p in entries) > 96 * 1024**2:
        raise ValueError('LOG_BUNDLE_SIZE')
    staging = _safe_path(boundary / 'bundles' / ('snapshot-' + uuid.uuid4().hex), boundary)
    staging.mkdir(parents=True)
    try:
        return _build_bundle(root, boundary, summary, destination, entries, staging)
    finally:
        _tree_files(staging, boundary)
        shutil.rmtree(staging)


def _build_bundle(root, boundary, summary, destination, entries, staging):
    staged = []
    total = 0
    for path in entries:
        with path.open('rb') as handle:
            data = handle.read(96 * 1024**2 + 1)
        total += len(data)
        if total > 96 * 1024**2:
            raise ValueError('LOG_BUNDLE_SIZE')
        target = staging / path.name
        with target.open('xb') as handle:
            _write(handle, data)
        staged.append(dict(file=path.name, bytes=len(data), sha256=hashlib.sha256(data).hexdigest()))
    if _read_summary(root) != summary:
        raise ValueError('LOG_BUNDLE_MUTATED')
    with (staging / 'run_summary.json').open('rb') as handle:
        summary_data = handle.read(1024**2 + 1)
    with (staging / 'summary.sha256').open('rb') as handle:
        seal_data = handle.read(66)
    if summary_data != _bytes(summary) or seal_data != hashlib.sha256(summary_data).hexdigest().encode('ascii') + b'\n':
        raise ValueError('LOG_BUNDLE_MUTATED')
    expected = {item['file']: item for item in summary['retained_files']}
    for item in staged:
        if item['file'] not in {'run_summary.json', 'summary.sha256'} and item != expected.get(item['file']):
            raise ValueError('LOG_BUNDLE_MUTATED')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, 'x', compression=zipfile.ZIP_DEFLATED) as archive:
        for item in staged:
            with (staging / item['file']).open('rb') as handle:
                data = handle.read(96 * 1024**2 + 1)
            if len(data) != item['bytes'] or hashlib.sha256(data).hexdigest() != item['sha256']:
                raise ValueError('LOG_BUNDLE_MUTATED')
            archive.writestr(item['file'], data)
        archive.writestr('bundle_manifest.json', _bytes(dict(kind='DIAGNOSTIC_BUNDLE', version=1,
            run_id=summary['run_id'], evidence_incomplete=summary['evidence_incomplete'],
            rotation_count=summary['rotation_count'], event_counters=summary['event_counters'], files=staged)))
    if destination.stat().st_size > 128 * 1024**2:
        raise ValueError('LOG_BUNDLE_SIZE')
    with destination.open('rb') as handle:
        digest = hashlib.sha256(handle.read()).hexdigest()
    return dict(file=destination.relative_to(boundary).as_posix(), bytes=destination.stat().st_size, sha256=digest,
                evidence_incomplete=summary['evidence_incomplete'])
