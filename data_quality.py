# %%
"""Изолированная B2-проверка свечей; производственный допуск не включён."""

from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json
import re
from time import perf_counter

import numpy as np
import pandas as pd


RAW_COLUMNS = ('open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end')
KEY_FIELDS = ('engine', 'market', 'source_scope', 'board', 'raw_instrument_id',
              'interval', 'begin')
IDENTITY_FIELDS = KEY_FIELDS[:5]
PROFILE = 'ILLUSTRATIVE_JSON_ARRAYS_V1'
TIMESTAMP = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\Z')


# %%
def _same(left, right):
    """Сравнение не преобразует числа и не вызывает bool(pd.NA/массива)."""
    if left is right:
        return True
    if isinstance(left, dict) and isinstance(right, dict):
        return (left.keys() == right.keys()
                and all(_same(left[key], right[key]) for key in left))
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(_same(a, b) for a, b in zip(left, right))
    if type(left) is not type(right):
        return False
    try:
        result = left == right
        return isinstance(result, (bool, np.bool_)) and bool(result)
    except (TypeError, ValueError):
        return False


def _frozen(value):
    # Ключ для группировки сохраняет неподдерживаемые представления как отдельные.
    if isinstance(value, dict):
        return ('dict', tuple((key, _frozen(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return ('array', tuple(_frozen(item) for item in value))
    if value is None or value is pd.NA or value is pd.NaT:
        return ('missing', type(value).__name__)
    if isinstance(value, (str, int, float, bool, np.integer, np.floating, np.bool_)):
        return (type(value).__name__, value)
    return ('unsupported', type(value).__name__)


def _key_values(key):
    return [key.get(field) for field in KEY_FIELDS]


def _key_equal(left, right):
    return isinstance(left, dict) and isinstance(right, dict) and _same(
        _key_values(left), _key_values(right))


def _safe_ref(value):
    return (type(value) is str and bool(value) and len(value) <= 1024
            and not any(character in value for character in ('\\', '\n', '\r', '\x00'))
            and not value.startswith(('/', '.')) and ':' not in value
            and not any(part in ('.', '..') for part in value.split('/')))


def _profile_value(value):
    if value is None or type(value) in (str, int):
        return True
    if type(value) is list:
        return all(_profile_value(item) for item in value)
    return False


def _fingerprint(payload, schema):
    content = json.dumps(payload, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
    return {'basis': 'LOGICAL_ROWS_SHA256',
            'value': hashlib.sha256(content.encode('utf-8')).hexdigest(),
            'schema_version': schema}


def _check(identifier, status, reason=None, evidence_ref=None):
    return {'check_id': identifier, 'status': status, 'reason': None if status == 'PASS' else reason,
            'evidence_ref': evidence_ref}


def _status(statuses):
    if 'FAIL' in statuses:
        return 'FAIL'
    if 'NOT_VERIFIED' in statuses:
        return 'NOT_VERIFIED'
    return 'PASS'


def _nonblank_string(value):
    return type(value) is str and bool(value.strip())


def _as_of_status(value):
    # Проверяем только существующий UTC context, не календарь торгов или часы запуска.
    if not _nonblank_string(value) or 'T' not in value:
        return 'NOT_VERIFIED'
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return 'NOT_VERIFIED'
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return 'NOT_VERIFIED'
    if parsed.utcoffset() != timedelta(0):
        return 'FAIL'
    # fromisoformat может округлить дробь offset меньше микросекунды до нуля.
    offset = re.search(r'[+-]([0-9:.,]+)\Z', value.split('T', 1)[1])
    if offset and any(character in '123456789' for character in offset.group(1)):
        return 'FAIL'
    return 'PASS'


def _identity_valid(record):
    identity = record.get('identity', {})
    if not all(type(identity.get(field)) is str and identity[field]
               for field in ('engine', 'market', 'raw_instrument_id')):
        return False
    if type(record.get('interval')) is not int or record.get('interval') not in (24, 60, 10, 1):
        return False
    if identity.get('source_scope') == 'BOARD':
        return type(identity.get('board')) is str and bool(identity['board'])
    return (identity.get('source_scope') == 'AGGREGATE' and 'board' in identity and identity.get('board') is None
            and bool(identity.get('null_reasons', {}).get('board')))


def _inventory(metadata, evidence):
    records = {}
    problems = []
    identities = set()
    for record in metadata.get('identity_records', []):
        identifier = record.get('record_id')
        identity = record.get('identity', {})
        scoped = tuple(_frozen(identity.get(field)) for field in IDENTITY_FIELDS)
        scoped += (_frozen(record.get('interval')),)
        if (type(identifier) is not str or not identifier or identifier in records or scoped in identities
                or not _identity_valid(record)):
            problems.append('IDENTITY_RECORD_INVALID')
        if identifier not in records:
            records[identifier] = record
        identities.add(scoped)
    inventory = []
    locators = {}
    ambiguous_locators = set()
    source_refs = set()
    for source in evidence.get('retained_sources', []):
        identifier = source.get('identity_record_id')
        provenance = source.get('provenance_ref')
        record = records.get(identifier)
        source_id = (_frozen(identifier), _frozen(provenance))
        if source_id in source_refs:
            problems.append('DUPLICATE_SOURCE_REFERENCE')
        source_refs.add(source_id)
        if record is None or not _safe_ref(provenance):
            problems.append('RAW_SOURCE_IDENTITY_MISMATCH')
        elif provenance not in [item.get('evidence_ref') for item in record.get('provenance', [])]:
            problems.append('RAW_PROVENANCE_MISMATCH')
        for row in source.get('rows', []):
            ordinal = row.get('raw_row_ordinal')
            locator = [identifier, provenance, ordinal]
            locator_key = tuple(_frozen(item) for item in locator)
            item = {'locator': locator, 'row': row, 'record': record}
            inventory.append(item)
            if (type(ordinal) is not int or ordinal < 0
                    or locator_key in locators or locator_key in ambiguous_locators):
                problems.append('RAW_LOCATOR_INVALID')
                ambiguous_locators.add(locator_key)
                locators.pop(locator_key, None)
            else:
                locators[locator_key] = item
            key = row.get('raw_key', {})
            if not all(field in key for field in KEY_FIELDS):
                problems.append('RAW_SCOPED_KEY_INCOMPLETE')
            values = row.get('values', [])
            columns = row.get('columns', [])
            if columns != list(RAW_COLUMNS) or len(values) != len(RAW_COLUMNS):
                problems.append('RAW_ROW_SHAPE_MISMATCH')
            if record is not None:
                expected = {field: record.get('identity', {}).get(field)
                            for field in IDENTITY_FIELDS}
                expected.update(interval=record.get('interval'), begin=key.get('begin'))
                if not _key_equal(key, expected):
                    problems.append('RAW_SCOPED_KEY_MISMATCH')
            if len(values) == len(RAW_COLUMNS) and not _same(key.get('begin'), values[6]):
                problems.append('RAW_BEGIN_MISMATCH')
    return records, inventory, locators, problems


def _lineage(frame_rows, columns, spec, metadata, evidence, records, inventory, locators, problems):
    mismatches = list(problems)
    unknown = []
    schema = metadata.get('schema_version')
    if any(not _nonblank_string(metadata.get(field))
           for field in ('run_id', 'snapshot_id', 'dataset_id', 'schema_version')):
        unknown.append('IMMUTABLE_CONTEXT_NOT_AVAILABLE')
    versions = [spec.get('contract_version'), metadata.get('contract_version')]
    if not all(type(version) is int for version in versions):
        unknown.append('CONTRACT_VERSION_NOT_AVAILABLE')
    elif versions[0] != versions[1]:
        mismatches.append('CONTRACT_VERSION_MISMATCH')
    elif versions[0] != 1:
        unknown.append('CONTRACT_VERSION_NOT_SUPPORTED')
    source = metadata.get('source')
    if not _nonblank_string(source):
        unknown.append('APPROVED_SOURCE_NOT_AVAILABLE')
    elif source != 'MOEX_ISS_RAW':
        mismatches.append('UNAPPROVED_SOURCE')
    cutoff_status = _as_of_status(metadata.get('as_of'))
    if cutoff_status == 'NOT_VERIFIED':
        unknown.append('UTC_AS_OF_NOT_AVAILABLE')
    elif cutoff_status == 'FAIL':
        mismatches.append('AS_OF_NOT_UTC')
    for field in ('dataset_id', 'schema_version'):
        if not _same(spec.get(field), metadata.get(field)):
            mismatches.append('SPEC_CONTEXT_MISMATCH')
    declared_columns = [column.get('name') for column in spec.get('columns', [])]
    column_mapping_valid = (columns == declared_columns
                            and all(_nonblank_string(name) for name in columns)
                            and len(set(columns)) == len(columns)
                            and all(name in columns for name in (*RAW_COLUMNS, 'ticker')))
    if not column_mapping_valid:
        mismatches.append('FRAME_SCHEMA_MISMATCH')
    if any(not _same(record.get('interval'), spec.get('interval')) for record in records.values()):
        mismatches.append('SOURCE_INTERVAL_MISMATCH')
    bindings = metadata.get('row_bindings')
    authority = evidence.get('producer_selection')
    if not isinstance(bindings, dict) or not isinstance(authority, dict):
        unknown.append('INDEPENDENT_SELECTION_NOT_AVAILABLE')
    bindings = bindings if isinstance(bindings, dict) else {}
    authority = authority if isinstance(authority, dict) else {}
    entries = bindings.get('entries', [])
    if not isinstance(entries, list):
        mismatches.append('BINDING_ENTRIES_INVALID')
        entries = []
    if bindings and (bindings.get('row_count') != len(frame_rows)
                     or [entry.get('ordinal') for entry in entries] != list(range(len(frame_rows)))):
        mismatches.append('BINDING_ORDINAL_SET_MISMATCH')
    if authority:
        for field in ('snapshot_id', 'dataset_id', 'schema_version'):
            if not _same(authority.get(field), metadata.get(field)):
                mismatches.append('AUTHORITY_CONTEXT_MISMATCH')
        if not _safe_ref(authority.get('evidence_ref')):
            unknown.append('AUTHORITY_REFERENCE_NOT_AVAILABLE')
    if bindings:
        for field in ('snapshot_id', 'dataset_id', 'schema_version'):
            if not _same(bindings.get(field), metadata.get(field)):
                mismatches.append('BINDING_CONTEXT_MISMATCH')
    if bindings and authority:
        for field in ('snapshot_id', 'dataset_id', 'schema_version', 'entries',
                      'frame_fingerprint', 'binding_fingerprint'):
            if not _same(bindings.get(field), authority.get(field)):
                mismatches.append('INDEPENDENT_SELECTION_MISMATCH')
        if bindings.get('binding_evidence_ref') != authority.get('evidence_ref'):
            mismatches.append('INDEPENDENT_SELECTION_REFERENCE_MISMATCH')
    payload = [schema, columns, frame_rows]
    if 'frame_payload' not in evidence:
        unknown.append('RETAINED_FRAME_NOT_AVAILABLE')
    elif not _same(payload, evidence['frame_payload']):
        mismatches.append('RETAINED_FRAME_CONTENT_MISMATCH')
    supported = evidence.get('serialization_profile') == PROFILE and _profile_value(payload)
    raw_payloads = []
    for item in inventory:
        row = item['row']
        raw_payload = [_key_values(row.get('raw_key', {})), row.get('columns'), row.get('values')]
        raw_payloads.append(raw_payload)
        supported = supported and _profile_value(raw_payload)
    if not supported:
        unknown.append('NORMALIZATION_PROFILE_NOT_AVAILABLE')
    selected = []
    encoded = []
    for entry in entries:
        source = entry.get('source_row_ref', {})
        locator = [entry.get('identity_record_id'), source.get('provenance_ref'), source.get('raw_row_ordinal')]
        item = locators.get(tuple(_frozen(value) for value in locator))
        selected.append(item)
        ordinal = entry.get('ordinal')
        if item is None:
            mismatches.append('BINDING_RAW_LOCATOR_MISMATCH')
            continue
        row, record = item['row'], item['record']
        if (not _key_equal(source.get('raw_key', {}), row.get('raw_key', {}))
                or not _same(source.get('raw_row_fingerprint'), row.get('raw_row_fingerprint'))
                or not _same(entry.get('begin'), row.get('raw_key', {}).get('begin'))):
            mismatches.append('BINDING_RAW_CONTENT_MISMATCH')
        if type(ordinal) is int and 0 <= ordinal < len(frame_rows) and record is not None:
            if column_mapping_valid:
                frame_row = dict(zip(columns, frame_rows[ordinal]))
                if (not _same([frame_row[column] for column in RAW_COLUMNS], row.get('values'))
                        or not _same(frame_row['ticker'], record.get('identity', {}).get('display_ticker'))):
                    mismatches.append('CANDIDATE_RAW_CONTENT_MISMATCH')
        else:
            mismatches.append('BINDING_ORDINAL_INVALID')
        encoded.append([ordinal, entry.get('identity_record_id'), entry.get('begin'),
                        source.get('provenance_ref'), source.get('raw_row_ordinal'),
                        _key_values(source.get('raw_key', {})),
                        source.get('raw_row_fingerprint', {}).get('value')])
    if supported:
        frame_fingerprint = _fingerprint(payload, schema)
        for container in (bindings, authority):
            if container and not _same(container.get('frame_fingerprint'), frame_fingerprint):
                mismatches.append('FRAME_FINGERPRINT_MISMATCH')
        for item, raw_payload in zip(inventory, raw_payloads):
            if not _same(item['row'].get('raw_row_fingerprint'), _fingerprint(raw_payload, schema)):
                mismatches.append('RAW_FINGERPRINT_MISMATCH')
        binding_payload = [metadata.get('snapshot_id'), metadata.get('dataset_id'), schema,
                           frame_fingerprint['value'], encoded]
        fingerprint = _fingerprint(binding_payload, schema)
        for container in (bindings, authority):
            if container and not _same(container.get('binding_fingerprint'), fingerprint):
                mismatches.append('BINDING_FINGERPRINT_MISMATCH')
    checks = []
    if mismatches:
        checks.append(_check('IDENTITY_BINDING_MISMATCH', 'FAIL', 'KNOWN_LINEAGE_CONTRADICTION'))
    if unknown:
        checks.append(_check('IMMUTABLE_LINEAGE_NOT_VERIFIED', 'NOT_VERIFIED',
                             'REQUIRED_CONTEXT_AUTHORITY_OR_NORMALIZATION_UNAVAILABLE'))
    if not checks:
        checks.append(_check('IDENTITY_BINDING', 'PASS', evidence_ref=authority.get('evidence_ref')))
    return _status([check['status'] for check in checks]), checks, selected


# %%
def _number(value):
    if value is None or value is pd.NA or value is pd.NaT:
        return 'FAIL', 'RAW_NUMERIC_MISSING'
    if isinstance(value, (bool, np.bool_)):
        return 'NOT_VERIFIED', 'UNSUPPORTED_NUMERIC_REPRESENTATION'
    if isinstance(value, (int, np.integer)):
        return 'PASS', None
    if isinstance(value, (float, np.floating)):
        return ('PASS', None) if np.isfinite(value) else ('FAIL', 'RAW_NUMERIC_NONFINITE')
    return 'NOT_VERIFIED', 'UNSUPPORTED_NUMERIC_REPRESENTATION'


def _timestamp(value):
    if value is None or value is pd.NA or value is pd.NaT:
        return 'FAIL', 'RAW_TIMESTAMP_INVALID'
    if type(value) is not str:
        return 'NOT_VERIFIED', 'UNSUPPORTED_TIMESTAMP_REPRESENTATION'
    if not TIMESTAMP.fullmatch(value):
        return 'FAIL', 'RAW_TIMESTAMP_INVALID'
    try:
        datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return 'FAIL', 'RAW_TIMESTAMP_INVALID'
    return 'PASS', None


def _raw_findings(inventory, evidence):
    findings = []
    states = []
    for item in inventory:
        row = item['row']
        values = row.get('values', [])
        reasons, statuses = [], []
        if row.get('columns') != list(RAW_COLUMNS) or len(values) != len(RAW_COLUMNS):
            reasons.append('RAW_ROW_SHAPE_INVALID')
            statuses.append('FAIL')
        else:
            numeric = [_number(value) for value in values[:6]]
            temporal = [_timestamp(value) for value in values[6:]]
            statuses.extend(status for status, _ in numeric + temporal)
            reasons.extend(reason for _, reason in numeric + temporal if reason is not None)
            if all(status == 'PASS' for status, _ in numeric[:4]):
                opening, closing, high, low = values[:4]
                if not low <= min(opening, closing) <= max(opening, closing) <= high:
                    reasons.append('OHLC_BOUNDS')
                    statuses.append('FAIL')
            if all(status == 'PASS' for status, _ in temporal) and values[7] < values[6]:
                reasons.append('END_BEFORE_BEGIN')
                statuses.append('FAIL')
            for known in evidence.get('known_observations', []):
                key = row.get('raw_key', {})
                if (known.get('reason') == 'CONFIRMED_ABIO_MULTIYEAR_OBSERVATION'
                        and _safe_ref(known.get('evidence_ref'))
                        and key.get('engine') == 'stock' and key.get('market') == 'shares'
                        and key.get('source_scope') == 'AGGREGATE' and key.get('board') is None
                        and key.get('raw_instrument_id') == 'ABIO' and key.get('interval') == 24
                        and _key_equal(known.get('raw_key', {}), key)
                        and _same(known.get('columns'), row.get('columns'))
                        and _same(known.get('values'), values)):
                    reasons.append('CONFIRMED_ABIO_MULTIYEAR_OBSERVATION')
                    statuses.append('FAIL')
        state = _status(statuses)
        states.append(state)
        findings.append({'locator': deepcopy(item['locator']), 'raw_key': deepcopy(row.get('raw_key', {})),
                         'status': state, 'reasons': list(dict.fromkeys(reasons))})
    return findings, states


def _revision_proven(items, key, metadata, evidence):
    # Связь old/new доказывает только эту пару, а не третий конфликтный вариант.
    if len(items) != 2:
        return False
    variants = {tuple(_frozen(value) for value in item['locator']) for item in items}
    for revision in evidence.get('revision_evidence', []):
        old, new = revision.get('old_locator'), revision.get('new_locator')
        if not isinstance(old, list) or not isinstance(new, list) or len(old) != 3 or len(new) != 3:
            continue
        old_key = tuple(_frozen(value) for value in old)
        new_key = tuple(_frozen(value) for value in new)
        if (revision.get('snapshot_id') == metadata.get('snapshot_id')
                and _key_equal(revision.get('raw_key', {}), key)
                and old_key != new_key and old_key in variants and new_key in variants
                and _safe_ref(revision.get('evidence_ref'))):
            return True
    return False


def _raw_groups(inventory, findings, states, selected, metadata, evidence):
    grouped = {}
    selected_locators = {tuple(_frozen(value) for value in item['locator'])
                         for item in selected if item is not None}
    for index, item in enumerate(inventory):
        token = tuple(_frozen(value) for value in _key_values(item['row'].get('raw_key', {})))
        grouped.setdefault(token, []).append(index)
    groups, blocked = [], set()
    for indices in grouped.values():
        items = [inventory[index] for index in indices]
        first = items[0]['row']
        key = first.get('raw_key', {})
        classification, reasons = 'UNIQUE', []
        if len(items) > 1:
            equal = all(_same(first.get('columns'), item['row'].get('columns'))
                        and _same(first.get('values'), item['row'].get('values')) for item in items[1:])
            if equal:
                classification, reasons = 'DUPLICATE_EXACT', ['DUPLICATE_EXACT']
            elif _revision_proven(items, key, metadata, evidence):
                classification, reasons = 'EVIDENCED_REVISION', ['REVISION_SELECTION_NOT_APPLIED']
            else:
                classification, reasons = 'SOURCE_KEY_CONFLICT', ['REVISION_NOT_PROVEN']
                values = [item['row'].get('values', []) for item in items]
                if all(len(value) == 8 for value in values):
                    if any(not _same(value[7], values[0][7]) for value in values[1:]):
                        reasons.append('END_VARIANT')
                    if any(not _same(value[:4], values[0][:4]) for value in values[1:]):
                        reasons.append('OHLC_VARIANT')
                    if any(not _same(value[4:6], values[0][4:6]) for value in values[1:]):
                        reasons.append('RAW_VALUE_VARIANT')
                if any(tuple(_frozen(value) for value in item['locator']) not in selected_locators for item in items):
                    reasons.append('UNSELECTED_RAW_VARIANT_CONFLICT')
                for index in indices:
                    states[index] = 'FAIL'
                    findings[index]['status'] = 'FAIL'
                    findings[index]['reasons'] = list(dict.fromkeys(
                        findings[index]['reasons'] + ['SOURCE_KEY_CONFLICT']))
            if classification in ('DUPLICATE_EXACT', 'EVIDENCED_REVISION'):
                blocked.update(item['locator'][0] for item in items)
        groups.append({'raw_key': deepcopy(key), 'classification': classification,
                       'variant_count': len(items),
                       'variants': [deepcopy(item['locator']) for item in items], 'reasons': reasons})
    return groups, blocked


def _coverage(metadata, evidence, records, inventory):
    membership = evidence.get('source_membership')
    checks = []
    if not isinstance(membership, list):
        return {'status': 'NOT_VERIFIED', 'checks': [
            _check('SOURCE_MEMBERSHIP', 'NOT_VERIFIED', 'INDEPENDENT_MEMBERSHIP_NOT_AVAILABLE')]}
    by_record = {}
    for item in membership:
        by_record.setdefault(item.get('identity_record_id'), []).append(item)
    outcomes = []
    for identifier, record in records.items():
        proof = by_record.get(identifier, [])
        expected = record.get('coverage_evidence', {})
        status, reason = 'COMPLETE', 'INDEPENDENT_SOURCE_MEMBERSHIP_MATCHES'
        if (len(proof) != 1 or expected.get('expected_rows') is None
                or not _safe_ref(expected.get('evidence_ref'))):
            status, reason = 'NOT_VERIFIED', 'INDEPENDENT_MEMBERSHIP_NOT_AVAILABLE'
        else:
            proof = proof[0]
            keys = proof.get('expected_raw_keys')
            if not isinstance(keys, list):
                status, reason = 'NOT_VERIFIED', 'INDEPENDENT_MEMBERSHIP_NOT_AVAILABLE'
            else:
                received = [item['row'].get('raw_key', {}) for item in inventory
                            if item['locator'][0] == identifier]
                expected_keys = Counter(tuple(_frozen(value) for value in _key_values(key)) for key in keys)
                received_keys = Counter(tuple(_frozen(value) for value in _key_values(key)) for key in received)
                if (not _same(proof.get('identity'), record.get('identity'))
                        or not _same(proof.get('interval'), record.get('interval'))
                        or not _same(proof.get('requested_bounds'), record.get('requested_bounds'))
                        or proof.get('evidence_ref') != expected.get('evidence_ref')
                        or type(expected.get('expected_rows')) is not int
                        or expected.get('expected_rows') != len(keys)
                        or expected_keys != received_keys):
                    status, reason = 'INCOMPLETE', 'EXPECTED_SOURCE_MEMBERSHIP_MISMATCH'
        outcomes.append(status)
        checks.append({'identity_record_id': identifier, 'status': status, 'reason': reason})
    if any(identifier not in records for identifier in by_record):
        outcomes.append('INCOMPLETE')
        checks.append({'identity_record_id': None, 'status': 'INCOMPLETE', 'reason': 'FOREIGN_MEMBERSHIP_RECORD'})
    status = 'INCOMPLETE' if 'INCOMPLETE' in outcomes else 'NOT_VERIFIED' if (
        'NOT_VERIFIED' in outcomes or not outcomes) else 'COMPLETE'
    return {'status': status, 'checks': checks}


def _eligibility(records, selected, inventory, states, blocked, lineage, coverage):
    by_locator = {}
    for item, state in zip(inventory, states):
        by_locator.setdefault(tuple(_frozen(value) for value in item['locator']), state)
    groups = []
    for identifier, record in records.items():
        members = [(ordinal, item) for ordinal, item in enumerate(selected)
                   if item is not None and item['locator'][0] == identifier]
        reasons = []
        status = lineage
        if lineage != 'PASS':
            reasons.append('IDENTITY_BINDING_MISMATCH' if lineage == 'FAIL'
                           else 'IMMUTABLE_LINEAGE_NOT_VERIFIED')
        if coverage['status'] != 'COMPLETE':
            status = _status([status, 'FAIL' if coverage['status'] == 'INCOMPLETE' else 'NOT_VERIFIED'])
            reasons.append('SOURCE_MEMBERSHIP_NOT_COMPLETE')
        if identifier in blocked:
            status = 'FAIL'
            reasons.append('RAW_VARIANT_SELECTION_UNRESOLVED')
        local_states = [by_locator[tuple(_frozen(value) for value in item['locator'])]
                        for _, item in members]
        if 'NOT_VERIFIED' in local_states:
            status = _status([status, 'NOT_VERIFIED'])
            reasons.append('RAW_REPRESENTATION_NOT_VERIFIED')
        clean = [(ordinal, item) for ordinal, item in members
                 if by_locator[tuple(_frozen(value) for value in item['locator'])] == 'PASS']
        # Независимый selection подтверждает происхождение, но не уникальность ключей.
        clean_keys = [_frozen(item['row']['raw_key']) for _, item in clean]
        if len(set(clean_keys)) != len(clean_keys):
            status = 'FAIL'
            reasons.append('CANDIDATE_KEY_DUPLICATE')
        if not clean:
            status = 'FAIL' if members or lineage == 'PASS' else status
            reasons.append('EFFECTIVE_SEQUENCE_EMPTY')
        ordinals = [ordinal for ordinal, _ in sorted(clean, key=lambda pair: pair[1]['row']['values'][6])]
        if status != 'PASS':
            ordinals = []
        groups.append({'identity_record_id': identifier, 'interval': record.get('interval'),
                       'candidate_ordinals': ordinals, 'status': status, 'reasons': reasons})
    status = _status([group['status'] for group in groups]) if groups else 'NOT_VERIFIED'
    return {'status': status, 'groups': groups, 'policy': 'CONTINUE_ACROSS_QUARANTINE',
            'production_admission': 'NOT_ENABLED'}


# %%
def _error(metadata, interval, code):
    error = dict(category='QUALITY', code=code, stage='tech', retryable=False, final=True,
                 message='Не пройдена внутренняя проверка B2 свечей.',
                 run_id=metadata.get('run_id'), snapshot_id=metadata.get('snapshot_id'),
                 dataset_id=metadata.get('dataset_id'), instrument=None, interval=interval,
                 page=None, file=None, artifact_id=None, target_id=None, endpoint=None,
                 attempt=None, exception_type=None)
    error['null_reasons'] = {key: 'NOT_AVAILABLE_OR_NOT_APPLICABLE'
                             for key, value in error.items() if value is None}
    return error


def _counts(received=None, accepted=None, quarantined=None):
    result = dict(rows_received=received, rows_accepted=accepted, rows_quarantined=quarantined,
                  pages=None, instruments=None)
    result['null_reasons'] = {key: 'NOT_MEASURED_FOR_THIS_COUNT_POPULATION'
                              for key, value in result.items() if value is None}
    return result


def _diagnostic(context, diagnostics, metadata, interval, start, event, outcome=None, counts=None, error=None):
    if context is None:
        return
    try:
        import run_logging
        elapsed = max(0.0, (perf_counter() - start) * 1000)
        fields = dict(stage='tech', run_id=metadata.get('run_id'),
                      snapshot_id=metadata.get('snapshot_id'), dataset_id=metadata.get('dataset_id'),
                      interval=interval, duration_ms=elapsed, elapsed_ms=elapsed, outcome=outcome,
                      counts=counts or _counts(), error=error, fields={})
        level = 'ERROR' if error is not None else 'INFO'
        receipt = run_logging.emit_event(context, level, event, 'Внутренняя проверка B2 свечей.', fields)
        if (not isinstance(receipt, dict) or receipt.get('accepted') is not True
                or level == 'ERROR' and receipt.get('confirmed') is not True):
            diagnostics['status'] = 'FAILED'
    except Exception:
        # Диагностика не подменяет результат и не раскрывает исключение логгера.
        diagnostics['status'] = 'FAILED'


def assess_candles(frame, *, spec, metadata, retained_evidence, logging_context=None):
    """Проверить retained raw, исходные bindings и локальную проекцию без записи."""
    start = perf_counter()
    diagnostics = {'status': 'NOT_CONFIGURED' if logging_context is None else 'RECORDED'}
    interval = spec.get('interval')
    _diagnostic(logging_context, diagnostics, metadata, interval, start, 'b2_assessment_started')
    try:
        columns = list(frame.columns)
        frame_rows = [list(row) for row in frame.itertuples(index=False, name=None)]
        records, inventory, locators, problems = _inventory(metadata, retained_evidence)
        lineage, checks, selected = _lineage(frame_rows, columns, spec, metadata, retained_evidence,
                                             records, inventory, locators, problems)
        findings, states = _raw_findings(inventory, retained_evidence)
        groups, blocked = _raw_groups(inventory, findings, states, selected, metadata, retained_evidence)
        raw_status = _status(states)
        raw_quarantined = states.count('FAIL')
        selected_tokens = {tuple(_frozen(value) for value in item['locator'])
                           for item in selected if item is not None}
        selected_raw = sum(tuple(_frozen(value) for value in item['locator']) in selected_tokens
                           for item in inventory)
        raw = dict(status=raw_status, rows_checked=len(inventory), quarantined_rows=raw_quarantined,
                   selected_raw_rows=selected_raw, unselected_raw_rows=len(inventory) - selected_raw,
                   findings=findings, groups=groups, source_verification='NOT_VERIFIED')
        checks.append(_check('RAW_CORPUS_VALIDATION', raw_status, 'SCOPED_RETAINED_RAW_CORPUS'))
        coverage = _coverage(metadata, retained_evidence, records, inventory)
        eligibility = _eligibility(records, selected, inventory, states, blocked, lineage, coverage)
        coverage_status = {'COMPLETE': 'PASS', 'INCOMPLETE': 'FAIL', 'NOT_VERIFIED': 'NOT_VERIFIED'}[coverage['status']]
        checks.append(_check('SOURCE_MEMBERSHIP', coverage_status, 'INDEPENDENT_SOURCE_MEMBERSHIP'))
        checks.append(_check('COMPUTATIONAL_ELIGIBILITY', eligibility['status'], 'LOCAL_SEQUENCE_ONLY'))
        checks.append(_check('FULL_DATASET_VALIDATION', 'NOT_VERIFIED', 'B2_INTERNAL_SUBSET_ONLY'))
        state_by_locator = {}
        for item, state in zip(inventory, states):
            state_by_locator.setdefault(tuple(_frozen(value) for value in item['locator']), state)
        candidate_quarantined = None
        accepted = None
        if lineage == 'PASS':
            candidate_quarantined = sum(state_by_locator[tuple(_frozen(value) for value in item['locator'])] == 'FAIL'
                                        for item in selected if item is not None)
            if eligibility['status'] == 'PASS':
                accepted = sum(len(group['candidate_ordinals']) for group in eligibility['groups'])
            elif len(selected) == candidate_quarantined:
                accepted = 0
        errors = [_error(metadata, interval, check['check_id']) for check in checks if check['status'] == 'FAIL']
        quality = dict(contract_version=metadata.get('contract_version'), run_id=metadata.get('run_id'),
                       snapshot_id=metadata.get('snapshot_id'), dataset_id=metadata.get('dataset_id'),
                       as_of=metadata.get('as_of'), status=_status([check['status'] for check in checks]),
                       checks=checks, rows_checked=len(frame_rows), accepted_rows=accepted,
                       quarantined_rows=candidate_quarantined, quarantine_ref=None, evidence_ref=None, errors=errors)
        quality['null_reasons'] = {'quarantine_ref': 'NO_QUARANTINE_ARTIFACT_WRITTEN',
                                   'evidence_ref': 'NO_VALIDATION_ARTIFACT_WRITTEN'}
        if accepted is None:
            quality['null_reasons']['accepted_rows'] = 'ORIGINAL_SELECTION_OR_EFFECTIVE_PROJECTION_NOT_VERIFIED'
        if candidate_quarantined is None:
            quality['null_reasons']['quarantined_rows'] = 'ORIGINAL_SELECTION_NOT_VERIFIED'
        raw_error = _error(metadata, interval, 'RAW_CORPUS_VALIDATION') if raw_status == 'FAIL' else None
        _diagnostic(logging_context, diagnostics, metadata, interval, start, 'b2_raw_assessed',
                    raw_status, _counts(len(inventory), quarantined=raw_quarantined), raw_error)
        _diagnostic(logging_context, diagnostics, metadata, interval, start, 'b2_candidate_assessed',
                    quality['status'], _counts(len(frame_rows), accepted, candidate_quarantined),
                    errors[0] if errors else None)
        return dict(quality_result=quality, raw_assessment=raw, eligibility=eligibility,
                    coverage_assessment=coverage, diagnostics=diagnostics)
    except BaseException:
        _diagnostic(logging_context, diagnostics, metadata, interval, start, 'b2_assessment_failed',
                    'FAIL', error=_error(metadata, interval, 'B2_ASSESSMENT_EXCEPTION'))
        raise
