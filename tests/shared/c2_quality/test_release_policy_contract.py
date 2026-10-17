"""Synthetic offline decision contract; no runtime or official-source assertions."""

import copy
from datetime import datetime
import json
from pathlib import Path
import re

import pytest


FIXTURE = Path(__file__).resolve().parents[2] / 'fixtures/c2_quality/release_policy_cases_v1.json'
CHECKS = ['IDENTITY_SCHEMA', 'LOCAL_PROFILE', 'SOURCE_SEMANTICS', 'RAW_PROVENANCE',
          'COVERAGE', 'RUN_ISOLATION', 'CANONICAL_CONTENT', 'CANONICAL_BINDINGS',
          'REQUIRED_NONEMPTY']
PROFILES = {'DIVIDEND_V1': ['CSV', 'XLSX'],
            'SYNTHETIC_GOOGLE_CAPACITY_V1': ['CSV', 'XLSX', 'GOOGLE']}
FORMATS = {'CSV': 'csv', 'XLSX': 'xlsx', 'GOOGLE': 'google_sheets'}
COLUMNS = ['ISIN', 'TRADE_CODE', 'dt', 'value', 'currency']
CONTEXT = {'run_id', 'snapshot_id', 'dataset_id', 'as_of'}
STAGE_KEYS = {'context', 'status', 'evidence_ref', 'reason'}
COLLECTION = {'NOT_RUN', 'COMPLETE', 'VALID_EMPTY', 'NOT_AVAILABLE_APPROVED_SOURCE',
              'INCOMPLETE', 'FAILED'}
QUALITY = {'NOT_RUN', 'PASS', 'FAIL', 'NOT_VERIFIED', 'NEEDS_DECISION'}
EVIDENCE = {'VERIFIED', 'HISTORICAL_ONLY', 'SYNTHETIC_ONLY', 'NOT_VERIFIED', 'CONFLICTED'}
REASONS = {'MISSING_EVIDENCE', 'UNVERIFIED_EVIDENCE', 'LOCAL_INVALID', 'SOURCE_UNAVAILABLE',
           'COVERAGE_UNKNOWN', 'PARTIAL_COLLECTION', 'EMPTY_CANDIDATE', 'DECISION_REQUIRED',
           'CAPACITY_EXCEEDED', 'CAPACITY_UNKNOWN', 'WRITE_FAILURE', 'PUBLICATION_FAILURE',
           'READBACK_FAILURE', 'READBACK_MISMATCH', 'LOGGER_FAILURE', 'SUPERVISION_FAILURE',
           'INCOMPLETE_STAGE'}
ERROR_CATEGORIES = {'CAPACITY', 'CAPACITY_NOT_VERIFIED', 'QUOTA_429', 'TIMEOUT', 'DISK_FULL',
                    'MEMORY_ERROR', 'ACCESS', 'QUALITY', 'SOURCE', 'CONFIG', 'IO', 'REMOTE', 'UNKNOWN'}
ERROR_CODES = REASONS | {'MUTATING_TIMEOUT', 'DIAGNOSTIC_FAILURE'}
CONTRACT_CODES = {'SCHEMA', 'ENUM', 'SAFE_ALIAS', 'COUNT', 'CONTEXT', 'FINGERPRINT',
                  'INVENTORY', 'BINDINGS', 'REASON', 'COLLECTION_INVENTORY', 'COVERAGE_INVENTORY',
                  'CHECK_INVENTORY', 'QUALITY_STATE', 'EVIDENCE_INVENTORY', 'EXPORT_INVENTORY',
                  'CAPACITY_PROFILE', 'PUBLICATION_STATE', 'PREVIOUS_STATE', 'ERROR_INVENTORY',
                  'REFERENCE', 'PRIMARY_ERROR', 'REDACTION', 'NULL_REASONS', 'MANIFEST',
                  'CASE_INVENTORY', 'JSON_DUPLICATE_KEY', 'JSON_NONSTANDARD_CONSTANT', 'JSON_SYNTAX'}
NULLABLE_ERROR = {'run_id', 'snapshot_id', 'dataset_id', 'instrument', 'interval', 'page',
                  'file', 'artifact_id', 'target_id', 'endpoint', 'attempt', 'exception_type'}
SAFE = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z')
DIGEST = re.compile(r'[0-9a-f]{64}\Z')
UTC = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\Z')


class ContractError(ValueError):
    """Only a closed code is exposed; never interpolate untrusted fixture values."""


def _require(condition, code='SCHEMA'):
    if not condition:
        raise ContractError(code)


def _keys(value, names, code='SCHEMA'):
    _require(type(value) is dict and set(value) == set(names), code)


def _enum(value, choices):
    _require(type(value) is str and value in choices, 'ENUM')


def _safe(value, nullable=False):
    _require((nullable and value is None) or
             (type(value) is str and SAFE.fullmatch(value) is not None), 'SAFE_ALIAS')


def _count(value, nullable=True):
    _require((nullable and value is None) or (type(value) is int and value >= 0), 'COUNT')


def _utc(value, nullable=False):
    if nullable and value is None:
        return
    _require(type(value) is str and UTC.fullmatch(value) is not None, 'CONTEXT')
    try:
        datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        raise ContractError('CONTEXT') from None


def _digest(value):
    _require(value is None or (type(value) is str and DIGEST.fullmatch(value)), 'FINGERPRINT')


def _context(value, expected=None):
    _keys(value, CONTEXT, 'CONTEXT')
    for name in ('run_id', 'snapshot_id', 'dataset_id'):
        _safe(value[name])
    _utc(value['as_of'])
    _require(expected is None or value == expected, 'CONTEXT')


def _ids(values, code='INVENTORY', nonempty=False):
    _require(type(values) is list and (values or not nonempty), code)
    for value in values:
        _safe(value)
    _require(len(values) == len(set(values)), code)


def _reason(status, reason):
    if status in {'FAIL', 'FAILED', 'NOT_VERIFIED', 'UNKNOWN', 'MISMATCH', 'INCOMPLETE',
                  'NOT_AVAILABLE_APPROVED_SOURCE'}:
        _enum(reason, REASONS)
    elif status in {'NOT_RUN', 'NOT_ATTEMPTED'}:
        _require(reason is None or reason == 'INCOMPLETE_STAGE', 'REASON')
    else:
        _require(reason is None, 'REASON')


def _stage(value, choices, context, extra=()):
    _keys(value, STAGE_KEYS | set(extra))
    _context(value['context'], context)
    _enum(value['status'], choices)
    _safe(value['evidence_ref'], nullable=True)
    _reason(value['status'], value['reason'])


def _pairs(pairs):
    _require(type(pairs) is list)
    for pair in pairs:
        _keys(pair, {'response_id', 'row_ordinal', 'row_fingerprint'})
        _safe(pair['response_id'])
        _count(pair['row_ordinal'], nullable=False)
        _digest(pair['row_fingerprint'])
        _require(pair['row_fingerprint'] is not None, 'FINGERPRINT')
    identities = [(pair['response_id'], pair['row_ordinal']) for pair in pairs]
    _require(len(identities) == len(set(identities)), 'BINDINGS')


def _validate_errors(errors, context):
    _require(type(errors) is list)
    for error in errors:
        _keys(error, NULLABLE_ERROR | {'error_id', 'category', 'code', 'stage', 'retryable',
                                      'final', 'message', 'null_reasons'})
        _safe(error['error_id'])
        _enum(error['category'], ERROR_CATEGORIES)
        _enum(error['code'], ERROR_CODES)
        _enum(error['stage'], {'collection', 'validation', 'preflight', 'artifact', 'export',
                              'publication', 'readback', 'logging', 'supervision'})
        _require(type(error['retryable']) is bool and type(error['final']) is bool)
        _require(error['message'] == 'REDACTED ' + error['code'], 'REDACTION')
        nulls = {name for name in NULLABLE_ERROR if error[name] is None}
        _keys(error['null_reasons'], nulls, 'NULL_REASONS')
        _require(all(reason == 'NOT_AVAILABLE_OR_NOT_APPLICABLE'
                     for reason in error['null_reasons'].values()), 'NULL_REASONS')
        for name in NULLABLE_ERROR - nulls:
            if name in {'interval', 'attempt'} or (name == 'page' and type(error[name]) is int):
                _count(error[name], nullable=False)
            else:
                _safe(error[name])
        for name in ('run_id', 'snapshot_id', 'dataset_id'):
            _require(error[name] is None or error[name] == context[name], 'CONTEXT')
    _ids([error['error_id'] for error in errors], 'ERROR_INVENTORY')


def _validate_input(value, mode):
    _keys(value, {'context', 'candidate_release_id', 'terminal', 'collection', 'coverage',
                  'quality', 'evidence', 'canonical', 'profile_id', 'required_export_ids',
                  'exports', 'logging', 'supervision', 'errors', 'primary_error_id'})
    context = value['context']
    _context(context)
    _safe(value['candidate_release_id'])
    _require(type(value['terminal']) is bool)
    _enum(mode, {'HYPOTHETICAL', 'EVIDENCE_BOUNDARY'})
    collection = value['collection']
    _keys(collection, {'context', 'status', 'scope', 'instruments'})
    _context(collection['context'], context)
    _enum(collection['status'], COLLECTION)
    _ids(collection['scope'], 'COLLECTION_INVENTORY')
    _require(type(collection['instruments']) is list)
    for instrument in collection['instruments']:
        _stage(instrument, COLLECTION, context, {'instrument_id', 'row_count'})
        _safe(instrument['instrument_id'])
        _count(instrument['row_count'])
        if instrument['status'] == 'VALID_EMPTY':
            _require(instrument['row_count'] == 0, 'COUNT')
    instrument_ids = [item['instrument_id'] for item in collection['instruments']]
    _ids(instrument_ids, 'COLLECTION_INVENTORY')
    _require(set(instrument_ids) == set(collection['scope']), 'COLLECTION_INVENTORY')
    coverage = value['coverage']
    _stage(coverage, {'COMPLETE', 'INCOMPLETE', 'NOT_AVAILABLE_APPROVED_SOURCE', 'NOT_VERIFIED'},
           context, {'expected_members', 'received_members', 'membership_evidence_ref'})
    for name in ('expected_members', 'received_members'):
        if coverage[name] is not None:
            _ids(coverage[name], 'COVERAGE_INVENTORY')
    _safe(coverage['membership_evidence_ref'], nullable=True)
    quality = value['quality']
    _keys(quality, {'context', 'observed_status', 'assessment_run', 'decision_pending',
                    'decision_reason', 'checks', 'rows_checked', 'accepted_rows', 'quarantined_rows'})
    _context(quality['context'], context)
    _enum(quality['observed_status'], QUALITY)
    _require(type(quality['assessment_run']) is bool and type(quality['decision_pending']) is bool)
    if quality['decision_pending']:
        _require(quality['decision_reason'] == 'DECISION_REQUIRED', 'REASON')
    else:
        _require(quality['decision_reason'] is None, 'REASON')
    for name in ('rows_checked', 'accepted_rows', 'quarantined_rows'):
        _count(quality[name])
    _require(type(quality['checks']) is list)
    for check in quality['checks']:
        _keys(check, {'check_id', 'status', 'evidence_ref', 'reason'})
        _enum(check['check_id'], CHECKS)
        _enum(check['status'], {'PASS', 'FAIL', 'NOT_VERIFIED'})
        _safe(check['evidence_ref'], nullable=True)
        _reason(check['status'], check['reason'])
    check_ids = [check['check_id'] for check in quality['checks']]
    _ids(check_ids, 'CHECK_INVENTORY')
    if quality['assessment_run']:
        _require(set(check_ids) == set(CHECKS), 'CHECK_INVENTORY')
        _require(quality['observed_status'] != 'NOT_RUN', 'QUALITY_STATE')
    else:
        _require(not check_ids and quality['observed_status'] == 'NOT_RUN' and
                 not quality['decision_pending'] and
                 all(quality[name] is None for name in
                     ('rows_checked', 'accepted_rows', 'quarantined_rows')), 'QUALITY_STATE')
    _require(type(value['evidence']) is list)
    for evidence in value['evidence']:
        _keys(evidence, {'evidence_id', 'context', 'status', 'obligation', 'basis_refs',
                         'hypothetical_assumption', 'artifact_id', 'target_id'})
        _safe(evidence['evidence_id'])
        _context(evidence['context'], context)
        _enum(evidence['status'], EVIDENCE)
        _safe(evidence['obligation'])
        _safe(evidence['artifact_id'], nullable=True)
        _safe(evidence['target_id'], nullable=True)
        _ids(evidence['basis_refs'], 'EVIDENCE_INVENTORY')
        _require(type(evidence['hypothetical_assumption']) is bool)
    evidence_ids = [item['evidence_id'] for item in value['evidence']]
    _ids(evidence_ids, 'EVIDENCE_INVENTORY')
    canonical = value['canonical']
    _keys(canonical, {'context', 'columns', 'row_count', 'content_fingerprint',
                      'binding_fingerprint', 'source_bindings', 'ordered_bindings'})
    _context(canonical['context'], context)
    _require(type(canonical['columns']) is list and
             all(type(column) is str for column in canonical['columns']))
    _count(canonical['row_count'])
    _digest(canonical['content_fingerprint'])
    _digest(canonical['binding_fingerprint'])
    _pairs(canonical['source_bindings'])
    _pairs(canonical['ordered_bindings'])
    _enum(value['profile_id'], PROFILES)
    _ids(value['required_export_ids'], 'EXPORT_INVENTORY', nonempty=True)
    _require(value['required_export_ids'] == PROFILES[value['profile_id']], 'EXPORT_INVENTORY')
    if value['profile_id'] == 'SYNTHETIC_GOOGLE_CAPACITY_V1':
        _require(mode == 'HYPOTHETICAL', 'EXPORT_INVENTORY')
    _require(type(value['exports']) is list)
    for export in value['exports']:
        _validate_export(export, context)
    export_ids = [item['export_id'] for item in value['exports']]
    _ids(export_ids, 'EXPORT_INVENTORY')
    _require(export_ids == value['required_export_ids'], 'EXPORT_INVENTORY')
    _ids([item['artifact_id'] for item in value['exports']], 'EXPORT_INVENTORY')
    for name in ('logging', 'supervision'):
        _stage(value[name], {'NOT_RUN', 'PASS', 'FAIL', 'NOT_VERIFIED'}, context)
    _validate_errors(value['errors'], context)
    error_ids = {item['error_id'] for item in value['errors']}
    _safe(value['primary_error_id'], nullable=True)
    _require(value['primary_error_id'] is None or value['primary_error_id'] in error_ids, 'REFERENCE')
    _require(bool(error_ids) == (value['primary_error_id'] is not None), 'PRIMARY_ERROR')
    for export in value['exports']:
        _require(set(export['error_ids']) <= error_ids, 'REFERENCE')
    refs = []
    for record in [coverage, *collection['instruments'], *quality['checks'],
                   value['logging'], value['supervision']]:
        refs.append(record['evidence_ref'])
    refs.append(coverage['membership_evidence_ref'])
    for evidence in value['evidence']:
        refs.extend(evidence['basis_refs'])
    for export in value['exports']:
        refs.extend(export[name]['evidence_ref'] for name in
                    ('preflight', 'artifact', 'preparation', 'publication', 'readback', 'previous'))
        refs.extend(export['previous'][name] for name in ('quality_evidence_ref', 'freshness_evidence_ref'))
    _require(all(ref is None or ref in evidence_ids for ref in refs), 'REFERENCE')


def _validate_export(value, context):
    _keys(value, {'context', 'export_id', 'artifact_id', 'target_id', 'format', 'required',
                  'preflight', 'artifact', 'preparation', 'publication', 'readback',
                  'previous', 'error_ids'})
    _context(value['context'], context)
    _enum(value['export_id'], FORMATS)
    _require(value['format'] == FORMATS[value['export_id']] and value['required'] is True,
             'EXPORT_INVENTORY')
    _safe(value['artifact_id'])
    _safe(value['target_id'])
    _stage(value['preflight'], {'NOT_RUN', 'PASS', 'FAIL', 'NOT_VERIFIED'}, context, {'limit_kind'})
    _enum(value['preflight']['limit_kind'], {'NONE', 'RESOURCE', 'SHEET_ROWS', 'SHEET_CELLS'})
    if value['format'] == 'csv':
        _require(value['preflight']['limit_kind'] in {'NONE', 'RESOURCE'}, 'CAPACITY_PROFILE')
    _stage(value['artifact'], {'NOT_RUN', 'WRITTEN', 'VERIFIED', 'FAILED'}, context,
           {'canonical_fingerprint', 'binding_fingerprint'})
    _digest(value['artifact']['canonical_fingerprint'])
    _digest(value['artifact']['binding_fingerprint'])
    _stage(value['preparation'], {'NOT_RUN', 'SUCCEEDED', 'FAILED'}, context)
    _stage(value['publication'], {'NOT_ATTEMPTED', 'ACKNOWLEDGED', 'FAILED', 'UNKNOWN'},
           context, {'mutation'})
    _require(type(value['publication']['mutation']) is bool)
    _require(value['publication']['status'] != 'NOT_ATTEMPTED' or
             not value['publication']['mutation'], 'PUBLICATION_STATE')
    _stage(value['readback'], {'NOT_ATTEMPTED', 'VERIFIED', 'MISMATCH', 'FAILED', 'UNKNOWN'},
           context, {'generation', 'release_id', 'snapshot_id', 'as_of',
                     'content_fingerprint', 'binding_fingerprint'})
    readback = value['readback']
    _enum(readback['generation'], {'CANDIDATE', 'PREVIOUS', 'EMPTY', 'OTHER', 'UNKNOWN'})
    for name in ('release_id', 'snapshot_id'):
        _safe(readback[name], nullable=True)
    _utc(readback['as_of'], nullable=True)
    _digest(readback['content_fingerprint'])
    _digest(readback['binding_fingerprint'])
    previous = value['previous']
    _keys(previous, {'captured_for', 'release_id', 'snapshot_id', 'as_of', 'content_fingerprint',
                     'evidence_ref', 'quality_evidence_ref', 'freshness_evidence_ref',
                     'quality', 'freshness', 'preservation'})
    _context(previous['captured_for'], context)
    for name in ('release_id', 'snapshot_id', 'evidence_ref', 'quality_evidence_ref', 'freshness_evidence_ref'):
        _safe(previous[name], nullable=True)
    _utc(previous['as_of'], nullable=True)
    _digest(previous['content_fingerprint'])
    _enum(previous['quality'], {'VERIFIED', 'UNVERIFIED', 'ABSENT', 'UNKNOWN'})
    _enum(previous['freshness'], {'CURRENT', 'STALE', 'UNKNOWN'})
    if previous['quality'] == 'ABSENT':
        _require(all(previous[name] is None for name in
                     ('release_id', 'snapshot_id', 'as_of', 'content_fingerprint')), 'PREVIOUS_STATE')
    _enum(previous['preservation'], {'NOT_TOUCHED', 'PRESERVED_VERIFIED', 'CHANGED_VERIFIED',
                                    'UNKNOWN', 'NOT_APPLICABLE'})
    _ids(value['error_ids'], 'ERROR_INVENTORY')


def _proof(ref, obligation, evidence, mode, official=False, artifact_id=None, target_id=None):
    if ref is None:
        return False
    record = evidence[ref]
    if (record['obligation'] != obligation or record['artifact_id'] != artifact_id or
            record['target_id'] != target_id):
        return False
    assumed = mode == 'HYPOTHETICAL' and record['hypothetical_assumption']
    if official:
        return assumed and record['status'] not in {'NOT_VERIFIED', 'CONFLICTED'}
    return record['status'] == 'VERIFIED' or (
        assumed and record['status'] not in {'NOT_VERIFIED', 'CONFLICTED'})


def _effective_collection(value, evidence, mode):
    collection = value['collection']
    if collection['status'] in {'NOT_RUN', 'FAILED', 'INCOMPLETE', 'NOT_AVAILABLE_APPROVED_SOURCE'}:
        return collection['status']
    statuses = [item['status'] for item in collection['instruments']]
    if not statuses or 'FAILED' in statuses:
        return 'FAILED'
    if 'NOT_AVAILABLE_APPROVED_SOURCE' in statuses:
        return 'NOT_AVAILABLE_APPROVED_SOURCE'
    if any(status not in {'COMPLETE', 'VALID_EMPTY'} for status in statuses):
        return 'INCOMPLETE'
    records = collection['instruments']
    if not all(_proof(item['evidence_ref'], 'COLLECTION', evidence, mode) for item in records):
        return 'INCOMPLETE'
    return 'VALID_EMPTY' if all(status == 'VALID_EMPTY' for status in statuses) else 'COMPLETE'


def _candidate(value, evidence, mode, collection):
    quality = value['quality']
    if not quality['assessment_run']:
        return {'quality_status': 'NOT_RUN', 'decision': 'NOT_ASSESSED', 'blocking_check_ids': []}
    canonical = value['canonical']
    coverage = value['coverage']
    check_statuses = {}
    for check in quality['checks']:
        check_id = check['check_id']
        status = check['status']
        official = check_id in {'SOURCE_SEMANTICS', 'RAW_PROVENANCE', 'COVERAGE'}
        sufficient = _proof(check['evidence_ref'], check_id, evidence, mode, official)
        if check_id == 'SOURCE_SEMANTICS' and sufficient:
            basis = evidence[check['evidence_ref']]['basis_refs']
            obligations = {'DATE_SEMANTICS', 'AMOUNT_SEMANTICS', 'CURRENCY_SEMANTICS'}
            sufficient = (len(basis) == 3 and
                          {evidence[ref]['obligation'] for ref in basis} == obligations and
                          all(_proof(ref, evidence[ref]['obligation'], evidence, mode, official=True)
                              for ref in basis))
        if check_id == 'COVERAGE':
            membership_proved = (
                _proof(coverage['membership_evidence_ref'], 'COVERAGE_MEMBERSHIP', evidence, mode, official=True)
                and _proof(coverage['evidence_ref'], 'COVERAGE', evidence, mode, official=True))
            known_members = coverage['expected_members'] is not None and coverage['received_members'] is not None
            sufficient = (sufficient and coverage['status'] == 'COMPLETE' and
                          known_members and
                          set(coverage['expected_members']) == set(coverage['received_members']) and
                          membership_proved)
            if (coverage['status'] == 'INCOMPLETE' or
                    (known_members and membership_proved and
                     set(coverage['expected_members']) != set(coverage['received_members']))):
                status = 'FAIL'
        if check_id == 'IDENTITY_SCHEMA' and canonical['columns'] != COLUMNS:
            status = 'FAIL'
        if check_id == 'CANONICAL_CONTENT':
            counts = [quality[name] for name in ('rows_checked', 'accepted_rows', 'quarantined_rows')]
            measured = all(count is not None for count in counts)
            instrument_counts = [item['row_count'] for item in value['collection']['instruments']]
            source_measured = bool(instrument_counts) and all(count is not None for count in instrument_counts)
            sufficient = (sufficient and canonical['row_count'] is not None and
                          canonical['content_fingerprint'] is not None and
                          measured and source_measured)
            if canonical['row_count'] is not None and canonical['row_count'] != len(canonical['ordered_bindings']):
                status = 'FAIL'
            if measured and (counts[0] != counts[1] + counts[2] or counts[2] != 0 or
                             (canonical['row_count'] is not None and counts[1] != canonical['row_count'])):
                status = 'FAIL'
            if measured and source_measured and value['collection']['status'] == 'COMPLETE':
                if counts[0] != sum(instrument_counts):
                    status = 'FAIL'
        if check_id == 'CANONICAL_BINDINGS':
            sufficient = sufficient and canonical['binding_fingerprint'] is not None
            if canonical['source_bindings'] != canonical['ordered_bindings']:
                status = 'FAIL'
        if check_id == 'REQUIRED_NONEMPTY':
            if canonical['row_count'] == 0 or collection == 'VALID_EMPTY':
                status = 'FAIL'
            sufficient = sufficient and canonical['row_count'] is not None
        check_statuses[check_id] = 'NOT_VERIFIED' if status == 'PASS' and not sufficient else status
    statuses = list(check_statuses.values())
    if 'FAIL' in statuses or quality['observed_status'] == 'FAIL':
        status = 'FAIL'
    elif quality['decision_pending'] or quality['observed_status'] == 'NEEDS_DECISION':
        status = 'NEEDS_DECISION'
    elif 'NOT_VERIFIED' in statuses or quality['observed_status'] == 'NOT_VERIFIED':
        status = 'NOT_VERIFIED'
    else:
        status = 'PASS'
    decision = 'ALLOW_PREFLIGHT' if status == 'PASS' and collection == 'COMPLETE' else 'BLOCK_NEW_RELEASE'
    if collection == 'NOT_RUN':
        decision = 'NOT_ASSESSED'
    return {'quality_status': status, 'decision': decision,
            'blocking_check_ids': [name for name in CHECKS if check_statuses[name] != 'PASS']}


def _project_export(export, value, evidence, mode):
    name = export['export_id']
    proof = lambda stage: _proof(export[stage]['evidence_ref'], name + '_' + stage.upper(),
                                  evidence, mode, artifact_id=export['artifact_id'], target_id=export['target_id'])
    canonical = value['canonical']
    preflight = export['preflight']['status']
    if preflight == 'PASS' and not proof('preflight'):
        preflight = 'NOT_VERIFIED'
    artifact = export['artifact']['status']
    artifact_known = all(export['artifact'][name] is not None for name in
                         ('canonical_fingerprint', 'binding_fingerprint')) and all(
                             canonical[name] is not None for name in ('content_fingerprint', 'binding_fingerprint'))
    artifact_matches = (export['artifact']['canonical_fingerprint'] == canonical['content_fingerprint'] and
                        export['artifact']['binding_fingerprint'] == canonical['binding_fingerprint'] and
                        canonical['content_fingerprint'] is not None and
                        canonical['binding_fingerprint'] is not None)
    if artifact == 'VERIFIED':
        if proof('artifact') and artifact_known and not artifact_matches:
            artifact = 'FAILED'
        elif not (proof('artifact') and artifact_matches):
            artifact = 'WRITTEN'
    preparation = export['preparation']['status']
    publication = export['publication']['status']
    if publication == 'ACKNOWLEDGED' and not proof('publication'):
        publication = 'UNKNOWN'
    readback = export['readback']
    readback_status = readback['status']
    generation = readback['generation']
    previous = export['previous']
    known_previous = (_proof(previous['evidence_ref'], name + '_PREVIOUS', evidence, mode,
                             target_id=export['target_id']) and
                      all(previous[field] is not None for field in
                          ('release_id', 'snapshot_id', 'as_of', 'content_fingerprint')))
    candidate_identity = (value['candidate_release_id'], value['context']['snapshot_id'],
                          value['context']['as_of'], canonical['content_fingerprint'])
    previous_identity = tuple(previous[field] for field in
                              ('release_id', 'snapshot_id', 'as_of', 'content_fingerprint'))
    observed_identity = tuple(readback[field] for field in
                              ('release_id', 'snapshot_id', 'as_of', 'content_fingerprint'))
    compared = proof('readback')
    if generation == 'CANDIDATE':
        compared = (compared and observed_identity == candidate_identity and
                    readback['binding_fingerprint'] == canonical['binding_fingerprint'] and
                    canonical['content_fingerprint'] is not None and canonical['binding_fingerprint'] is not None)
    elif generation == 'PREVIOUS':
        compared = compared and known_previous and observed_identity == previous_identity
    elif generation == 'EMPTY':
        compared = compared and all(item is None for item in observed_identity) and readback['binding_fingerprint'] is None
    elif generation == 'OTHER':
        compared = compared and all(item is not None for item in observed_identity)
    else:
        compared = False
    if readback_status in {'VERIFIED', 'MISMATCH'} and not compared:
        readback_status = 'UNKNOWN'
    if readback_status not in {'VERIFIED', 'MISMATCH'}:
        generation = 'UNKNOWN'
    preservation = previous['preservation']
    mutation = export['publication']['mutation']
    if mutation and preservation == 'NOT_TOUCHED':
        preservation = 'UNKNOWN'
    if preservation == 'PRESERVED_VERIFIED' and not (
            readback_status == 'VERIFIED' and generation == 'PREVIOUS' and known_previous):
        preservation = 'UNKNOWN'
    if preservation == 'CHANGED_VERIFIED' and not (
            readback_status in {'VERIFIED', 'MISMATCH'} and generation in {'CANDIDATE', 'OTHER'} and
            known_previous and readback['content_fingerprint'] != previous['content_fingerprint']):
        preservation = 'UNKNOWN'
    previous_quality = previous['quality']
    if previous_quality == 'VERIFIED' and not _proof(
            previous['quality_evidence_ref'], name + '_PREVIOUS_QUALITY', evidence, mode,
            target_id=export['target_id']):
        previous_quality = 'UNVERIFIED' if known_previous else 'UNKNOWN'
    if previous_quality == 'ABSENT' and not _proof(
            previous['evidence_ref'], name + '_PREVIOUS', evidence, mode, target_id=export['target_id']):
        previous_quality = 'UNKNOWN'
    freshness = previous['freshness']
    if freshness == 'CURRENT' and not _proof(
            previous['freshness_evidence_ref'], name + '_PREVIOUS_FRESHNESS', evidence, mode,
            target_id=export['target_id']):
        freshness = 'UNKNOWN'
    if preservation == 'NOT_APPLICABLE' and previous_quality != 'ABSENT':
        preservation = 'UNKNOWN'
    fallback = (preservation == 'PRESERVED_VERIFIED' and previous_quality == 'VERIFIED' and
                freshness == 'CURRENT')
    result = {'export_id': name, 'target_id': export['target_id'], 'artifact_id': export['artifact_id'],
              'preflight': preflight, 'artifact': artifact, 'preparation': preparation,
              'publication': publication, 'readback': readback_status, 'observed_generation': generation,
              'previous_verification': previous_quality, 'previous_freshness': freshness,
              'preservation_outcome': preservation, 'fallback_eligible': fallback,
              'error_ids': list(export['error_ids'])}
    ready = (preflight == 'PASS' and artifact == 'VERIFIED' and preparation == 'SUCCEEDED' and
             proof('preparation') and publication == 'ACKNOWLEDGED' and mutation and
             readback_status == 'VERIFIED' and generation == 'CANDIDATE' and not export['error_ids'])
    # These are canonical-content fingerprints, not representation-specific ZIP/CSV byte hashes.
    identity = observed_identity if generation in {'CANDIDATE', 'PREVIOUS', 'OTHER'} else generation
    return result, ready, identity


def _aggregate(statuses, empty, success, failure, uncertain=None):
    if failure in statuses:
        return failure
    if all(status == success for status in statuses):
        return success
    if all(status == empty for status in statuses):
        return empty
    return uncertain if uncertain is not None else empty


def _reduce(value, mode):
    """Pure evaluator: receives facts, never expected outputs or ambient runtime state."""
    _validate_input(value, mode)
    evidence = {item['evidence_id']: item for item in value['evidence']}
    collection = _effective_collection(value, evidence, mode)
    candidate = _candidate(value, evidence, mode, collection)
    projected = [_project_export(item, value, evidence, mode) for item in value['exports']]
    targets = [item[0] for item in projected]
    stages = {'collection': collection, 'validation': candidate['quality_status'],
              'artifact': _aggregate([item['artifact'] for item in targets], 'NOT_RUN', 'VERIFIED', 'FAILED', 'WRITTEN'),
              'preflight': _aggregate([item['preflight'] for item in targets], 'NOT_RUN', 'PASS', 'FAIL', 'NOT_VERIFIED'),
              'export': _aggregate([item['preparation'] for item in targets], 'NOT_RUN', 'SUCCEEDED', 'FAILED'),
              'publication': _aggregate([item['publication'] for item in targets], 'NOT_ATTEMPTED', 'ACKNOWLEDGED', 'FAILED', 'UNKNOWN'),
              'readback': _aggregate([item['readback'] for item in targets], 'NOT_ATTEMPTED', 'VERIFIED', 'FAILED', 'UNKNOWN')}
    if stages['artifact'] != 'FAILED' and any(item['artifact'] == 'NOT_RUN' for item in targets):
        stages['artifact'] = 'NOT_RUN'
    if stages['readback'] != 'FAILED' and any(item['readback'] == 'MISMATCH' for item in targets):
        stages['readback'] = 'MISMATCH'
    safety = all(value[name]['status'] == 'PASS' and
                 _proof(value[name]['evidence_ref'], name.upper(), evidence, mode)
                 for name in ('logging', 'supervision'))
    errors = [error['error_id'] for error in value['errors']]
    mutation = any(item['publication']['mutation'] for item in value['exports'])
    violations = []
    if mutation:
        if candidate['decision'] != 'ALLOW_PREFLIGHT':
            violations.append('MUTATION_BEFORE_ADMISSION')
        if stages['preflight'] != 'PASS':
            violations.append('MUTATION_BEFORE_PREFLIGHT')
        if stages['artifact'] != 'VERIFIED':
            violations.append('MUTATION_BEFORE_ARTIFACT_VERIFICATION')
    admitted = candidate['decision'] == 'ALLOW_PREFLIGHT'
    all_ready = all(item[1] for item in projected)
    content_unknown = any(item['observed_generation'] == 'UNKNOWN' for item in targets)
    identities = [item[2] for item in projected]
    all_conclusive = all(item['readback'] in {'VERIFIED', 'MISMATCH'} and
                         item['observed_generation'] != 'UNKNOWN' for item in targets)
    mixed = all_conclusive and len(set(identities)) > 1
    if not value['terminal']:
        status, exit_status = ('PUBLISHING' if mutation else 'CANDIDATE'), None
    elif mutation and content_unknown:
        status, exit_status = 'REMOTE_UNKNOWN', 1
    elif mixed:
        status, exit_status = 'PARTIAL_REMOTE', 1
    elif admitted and safety and all_ready and not errors and not violations:
        status, exit_status = 'CURRENT_VERIFIED', 0
    else:
        status, exit_status = 'BLOCKED', 1
    failed = []
    for item, projection in zip(value['exports'], projected):
        facts = projection[0]
        proven_failure = (facts['preflight'] in {'FAIL', 'NOT_VERIFIED'} or
                          facts['artifact'] == 'FAILED' or facts['preparation'] == 'FAILED' or
                          facts['publication'] in {'FAILED', 'UNKNOWN'} or
                          facts['readback'] in {'FAILED', 'MISMATCH', 'UNKNOWN'} or item['error_ids'])
        if (value['terminal'] and (not projection[1] or not admitted)) or proven_failure:
            failed.append(item['export_id'])
    return {'context': copy.deepcopy(value['context']), 'evaluation_scope': 'SYNTHETIC_ONLY',
            'official_source_status': 'NOT_VERIFIED', 'candidate': candidate,
            'release': {'status': status, 'stage_outcomes': stages, 'failed_required_exports': failed,
                        'per_target_outcomes': targets, 'retained_error_ids': errors,
                        'primary_error_id': value['primary_error_id'], 'exit_status': exit_status,
                        'policy_violations': violations}}


def _json_pairs(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, 'JSON_DUPLICATE_KEY')
        result[key] = value
    return result


def _json_constant(value):
    raise ContractError('JSON_NONSTANDARD_CONSTANT')


def _parse(text):
    try:
        return json.loads(text, object_pairs_hook=_json_pairs, parse_constant=_json_constant)
    except json.JSONDecodeError:
        raise ContractError('JSON_SYNTAX') from None


def _validate_expected(expected):
    """Validate an authored oracle's shape without deriving any of its decisions."""
    _keys(expected, {'contract_error', 'result'})
    if expected['contract_error'] is not None:
        _enum(expected['contract_error'], CONTRACT_CODES)
        _require(expected['result'] is None)
        return
    result = expected['result']
    _keys(result, {'context', 'evaluation_scope', 'official_source_status', 'candidate', 'release'})
    _context(result['context'])
    _require(result['evaluation_scope'] == 'SYNTHETIC_ONLY' and
             result['official_source_status'] == 'NOT_VERIFIED', 'MANIFEST')
    candidate = result['candidate']
    _keys(candidate, {'quality_status', 'decision', 'blocking_check_ids'})
    _enum(candidate['quality_status'], QUALITY)
    _enum(candidate['decision'], {'ALLOW_PREFLIGHT', 'BLOCK_NEW_RELEASE', 'NOT_ASSESSED'})
    _ids(candidate['blocking_check_ids'], 'CHECK_INVENTORY')
    _require(set(candidate['blocking_check_ids']) <= set(CHECKS), 'CHECK_INVENTORY')
    release = result['release']
    _keys(release, {'status', 'stage_outcomes', 'failed_required_exports', 'per_target_outcomes',
                    'retained_error_ids', 'primary_error_id', 'exit_status', 'policy_violations'})
    _enum(release['status'], {'CANDIDATE', 'BLOCKED', 'PUBLISHING', 'CURRENT_VERIFIED',
                             'PARTIAL_REMOTE', 'REMOTE_UNKNOWN'})
    stage_choices = {'collection': COLLECTION, 'validation': QUALITY,
                     'artifact': {'NOT_RUN', 'WRITTEN', 'VERIFIED', 'FAILED'},
                     'preflight': {'NOT_RUN', 'PASS', 'FAIL', 'NOT_VERIFIED'},
                     'export': {'NOT_RUN', 'SUCCEEDED', 'FAILED'},
                     'publication': {'NOT_ATTEMPTED', 'ACKNOWLEDGED', 'FAILED', 'UNKNOWN'},
                     'readback': {'NOT_ATTEMPTED', 'VERIFIED', 'MISMATCH', 'FAILED', 'UNKNOWN'}}
    _keys(release['stage_outcomes'], stage_choices)
    for name, choices in stage_choices.items():
        _enum(release['stage_outcomes'][name], choices)
    for name in ('failed_required_exports', 'retained_error_ids', 'policy_violations'):
        _ids(release[name])
    _require(set(release['failed_required_exports']) <= set(FORMATS), 'EXPORT_INVENTORY')
    _require(set(release['policy_violations']) <= {'MUTATION_BEFORE_ADMISSION',
             'MUTATION_BEFORE_PREFLIGHT', 'MUTATION_BEFORE_ARTIFACT_VERIFICATION'}, 'ENUM')
    _safe(release['primary_error_id'], nullable=True)
    _require(release['primary_error_id'] is None or
             release['primary_error_id'] in release['retained_error_ids'], 'REFERENCE')
    _require(release['exit_status'] is None or
             (type(release['exit_status']) is int and release['exit_status'] in {0, 1}), 'COUNT')
    _require(type(release['per_target_outcomes']) is list and release['per_target_outcomes'])
    for target in release['per_target_outcomes']:
        _keys(target, {'export_id', 'target_id', 'artifact_id', 'preflight', 'artifact', 'preparation',
                       'publication', 'readback', 'observed_generation', 'previous_verification',
                       'previous_freshness', 'preservation_outcome', 'fallback_eligible', 'error_ids'})
        _enum(target['export_id'], FORMATS)
        for name in ('target_id', 'artifact_id'):
            _safe(target[name])
        for name in ('preflight', 'artifact', 'publication', 'readback'):
            _enum(target[name], stage_choices[name])
        _enum(target['preparation'], stage_choices['export'])
        _enum(target['observed_generation'], {'CANDIDATE', 'PREVIOUS', 'EMPTY', 'OTHER', 'UNKNOWN'})
        _enum(target['previous_verification'], {'VERIFIED', 'UNVERIFIED', 'ABSENT', 'UNKNOWN'})
        _enum(target['previous_freshness'], {'CURRENT', 'STALE', 'UNKNOWN'})
        _enum(target['preservation_outcome'], {'NOT_TOUCHED', 'PRESERVED_VERIFIED', 'CHANGED_VERIFIED',
                                             'UNKNOWN', 'NOT_APPLICABLE'})
        _require(type(target['fallback_eligible']) is bool)
        _ids(target['error_ids'], 'ERROR_INVENTORY')
    _ids([target['export_id'] for target in release['per_target_outcomes']], 'EXPORT_INVENTORY')


def _manifest():
    manifest = _parse(FIXTURE.read_text(encoding='utf-8'))
    _keys(manifest, {'schema_id', 'schema_version', 'policy_id', 'origin', 'mandatory_check_ids',
                     'policy_profiles', 'cases'})
    _require(manifest['schema_id'] == 'mds.c2.release_policy_cases' and
             type(manifest['schema_version']) is int and manifest['schema_version'] == 1 and
             manifest['policy_id'] == 'C2-03C-01/BLOCK_V1' and
             manifest['origin'] == 'SYNTHETIC_ONLY', 'MANIFEST')
    _require(manifest['mandatory_check_ids'] == CHECKS, 'CHECK_INVENTORY')
    _require(manifest['policy_profiles'] == PROFILES, 'EXPORT_INVENTORY')
    _require(type(manifest['cases']) is list and manifest['cases'], 'CASE_INVENTORY')
    for case in manifest['cases']:
        _keys(case, {'case_id', 'scenario_mode', 'input', 'expected'})
        _safe(case['case_id'])
        _enum(case['scenario_mode'], {'HYPOTHETICAL', 'EVIDENCE_BOUNDARY'})
        _validate_expected(case['expected'])
    _ids([case['case_id'] for case in manifest['cases']], 'CASE_INVENTORY', nonempty=True)
    return manifest


CASES = _manifest()['cases']


@pytest.mark.parametrize('case', CASES, ids=[case['case_id'] for case in CASES])
def test_release_policy_cases(case):
    expected = case['expected']
    original = copy.deepcopy(case['input'])
    if expected['contract_error'] is not None:
        with pytest.raises(ContractError, match='^' + re.escape(expected['contract_error']) + '$'):
            _reduce(case['input'], case['scenario_mode'])
        assert expected['result'] is None
    else:
        actual = _reduce(case['input'], case['scenario_mode'])
        assert actual == expected['result']
        assert actual['evaluation_scope'] == 'SYNTHETIC_ONLY'
        assert actual['official_source_status'] == 'NOT_VERIFIED'
    assert case['input'] == original


@pytest.mark.parametrize('literal,code', [
    ('{"same":1,"same":2}', 'JSON_DUPLICATE_KEY'),
    ('{"nested":{"same":1,"same":2}}', 'JSON_DUPLICATE_KEY'),
    ('{"number":NaN}', 'JSON_NONSTANDARD_CONSTANT'),
    ('{"number":Infinity}', 'JSON_NONSTANDARD_CONSTANT'),
    ('{"number":-Infinity}', 'JSON_NONSTANDARD_CONSTANT'),
    ('{"broken":', 'JSON_SYNTAX'),
])
def test_strict_json_negative_cases(literal, code):
    with pytest.raises(ContractError, match='^' + code + '$'):
        _parse(literal)


def _valid_case():
    return copy.deepcopy(next(case for case in CASES if
                              case['expected']['contract_error'] is None and
                              case['expected']['result']['candidate']['quality_status'] == 'PASS'))


@pytest.mark.parametrize('fault,code', [
    ('missing_context', 'CONTEXT'), ('extra_input', 'SCHEMA'),
    ('snapshot_mismatch', 'CONTEXT'), ('invalid_cutoff', 'CONTEXT'),
    ('missing_check', 'CHECK_INVENTORY'), ('duplicate_check', 'CHECK_INVENTORY'),
    ('pass_reason', 'REASON'), ('individual_not_run', 'ENUM'),
    ('bool_count', 'COUNT'), ('missing_export', 'EXPORT_INVENTORY'),
    ('false_required', 'EXPORT_INVENTORY'), ('dangling_evidence', 'REFERENCE'),
    ('csv_sheet_limit', 'CAPACITY_PROFILE'),
])
def test_contract_faults_are_not_quality_refusals(fault, code):
    case = _valid_case()
    value = case['input']
    if fault == 'missing_context':
        value['context'].pop('snapshot_id')
    elif fault == 'extra_input':
        value['extra'] = True
    elif fault == 'snapshot_mismatch':
        value['coverage']['context']['snapshot_id'] = 'OTHER_SNAPSHOT'
    elif fault == 'invalid_cutoff':
        value['context']['as_of'] = '2026-02-30T00:00:00Z'
    elif fault == 'missing_check':
        value['quality']['checks'].pop()
    elif fault == 'duplicate_check':
        value['quality']['checks'].append(copy.deepcopy(value['quality']['checks'][0]))
    elif fault == 'pass_reason':
        value['quality']['checks'][0]['reason'] = 'LOCAL_INVALID'
    elif fault == 'individual_not_run':
        value['quality']['checks'][0]['status'] = 'NOT_RUN'
    elif fault == 'bool_count':
        value['quality']['accepted_rows'] = True
    elif fault == 'missing_export':
        value['required_export_ids'] = ['CSV']
    elif fault == 'false_required':
        value['exports'][0]['required'] = False
    elif fault == 'dangling_evidence':
        value['quality']['checks'][0]['evidence_ref'] = 'MISSING_RECORD'
    elif fault == 'csv_sheet_limit':
        value['exports'][0]['preflight']['limit_kind'] = 'SHEET_ROWS'
    with pytest.raises(ContractError, match='^' + code + '$'):
        _reduce(value, case['scenario_mode'])


@pytest.mark.parametrize('fault,check_id,status', [
    ('missing_date_basis', 'SOURCE_SEMANTICS', 'NOT_VERIFIED'),
    ('unproved_amount_basis', 'SOURCE_SEMANTICS', 'NOT_VERIFIED'),
    ('wrong_currency_basis', 'SOURCE_SEMANTICS', 'NOT_VERIFIED'),
    ('membership_missing', 'COVERAGE', 'NOT_VERIFIED'),
    ('membership_mismatch', 'COVERAGE', 'FAIL'),
    ('membership_mismatch_unproved', 'COVERAGE', 'NOT_VERIFIED'),
    ('accepted_unknown', 'CANONICAL_CONTENT', 'NOT_VERIFIED'),
    ('source_count_unknown', 'CANONICAL_CONTENT', 'NOT_VERIFIED'),
    ('checked_inconsistent', 'CANONICAL_CONTENT', 'FAIL'),
    ('quarantine_not_permitted', 'CANONICAL_CONTENT', 'FAIL'),
    ('source_count_inconsistent', 'CANONICAL_CONTENT', 'FAIL'),
    ('canonical_count_inconsistent', 'CANONICAL_CONTENT', 'FAIL'),
])
def test_required_basis_and_measured_counts(fault, check_id, status):
    case = _valid_case()
    value = case['input']
    value['terminal'] = True
    evidence = {item['evidence_id']: item for item in value['evidence']}
    semantic_check = next(check for check in value['quality']['checks'] if check['check_id'] == 'SOURCE_SEMANTICS')
    semantic = evidence[semantic_check['evidence_ref']]
    if fault == 'missing_date_basis':
        semantic['basis_refs'] = [ref for ref in semantic['basis_refs']
                                  if evidence[ref]['obligation'] != 'DATE_SEMANTICS']
    elif fault == 'unproved_amount_basis':
        next(item for item in evidence.values() if item['obligation'] == 'AMOUNT_SEMANTICS')['status'] = 'NOT_VERIFIED'
    elif fault == 'wrong_currency_basis':
        next(item for item in evidence.values() if item['obligation'] == 'CURRENCY_SEMANTICS')['obligation'] = 'DATE_SEMANTICS'
    elif fault.startswith('membership'):
        if fault != 'membership_missing':
            value['coverage']['received_members'] = ['OTHER_MEMBER']
        if fault != 'membership_mismatch':
            value['coverage']['membership_evidence_ref'] = None
    elif fault == 'accepted_unknown':
        value['quality']['accepted_rows'] = None
    elif fault == 'source_count_unknown':
        value['collection']['instruments'][0]['row_count'] = None
    elif fault == 'checked_inconsistent':
        value['quality']['rows_checked'] += 1
    elif fault == 'quarantine_not_permitted':
        value['quality']['quarantined_rows'] = 1
        value['quality']['rows_checked'] += 1
        value['collection']['instruments'][0]['row_count'] += 1
    elif fault == 'source_count_inconsistent':
        value['collection']['instruments'][0]['row_count'] += 1
    elif fault == 'canonical_count_inconsistent':
        value['canonical']['row_count'] += 1
    result = _reduce(value, case['scenario_mode'])
    assert result['candidate']['quality_status'] == status
    assert check_id in result['candidate']['blocking_check_ids']
    assert result['candidate']['decision'] == 'BLOCK_NEW_RELEASE'
    assert result['release']['status'] == 'BLOCKED'
    assert result['release']['exit_status'] == 1


def test_verified_labels_cannot_create_official_evidence():
    case = _valid_case()
    for item in case['input']['evidence']:
        item['status'] = 'VERIFIED'
        item['hypothetical_assumption'] = False
    result = _reduce(case['input'], 'EVIDENCE_BOUNDARY')
    assert result['candidate'] == {
        'quality_status': 'NOT_VERIFIED', 'decision': 'BLOCK_NEW_RELEASE',
        'blocking_check_ids': ['SOURCE_SEMANTICS', 'RAW_PROVENANCE', 'COVERAGE']}
    assert result['official_source_status'] == 'NOT_VERIFIED'
    assert result['evaluation_scope'] == 'SYNTHETIC_ONLY'


@pytest.mark.parametrize('field,location', [
    ('run_id', 'evidence'), ('snapshot_id', 'canonical'),
    ('dataset_id', 'artifact'), ('as_of', 'previous_capture'),
])
def test_every_caller_context_dimension_is_preserved(field, location):
    case = _valid_case()
    value = case['input']
    contexts = {'evidence': value['evidence'][0]['context'],
                'canonical': value['canonical']['context'],
                'artifact': value['exports'][0]['artifact']['context'],
                'previous_capture': value['exports'][0]['previous']['captured_for']}
    contexts[location][field] = '2026-10-16T00:00:00Z' if field == 'as_of' else 'OTHER_CONTEXT'
    with pytest.raises(ContractError, match='^CONTEXT$'):
        _reduce(value, case['scenario_mode'])


@pytest.mark.parametrize('reason', [[], {}], ids=['list', 'object'])
def test_malformed_pending_reason_is_a_closed_contract_error(reason):
    case = _valid_case()
    case['input']['exports'][0]['preflight']['reason'] = reason
    with pytest.raises(ContractError, match='^REASON$'):
        _reduce(case['input'], case['scenario_mode'])


def test_unsafe_diagnostic_text_is_never_echoed():
    case = copy.deepcopy(next(case for case in CASES if case['input']['errors']))
    case['input']['errors'][0]['message'] = 'RAW_SYNTHETIC_SECRET'
    with pytest.raises(ContractError) as error:
        _reduce(case['input'], case['scenario_mode'])
    assert error.value.args == ('REDACTION',)


def test_authored_expected_outputs_have_a_closed_schema():
    expected = copy.deepcopy(_valid_case()['expected'])
    expected['result']['release']['invented_status'] = 'PASS'
    with pytest.raises(ContractError, match='^SCHEMA$'):
        _validate_expected(expected)


@pytest.mark.parametrize('stage,projected_status,release_status', [
    ('preflight', 'NOT_VERIFIED', 'BLOCKED'),
    ('artifact', 'WRITTEN', 'BLOCKED'),
    ('publication', 'UNKNOWN', 'BLOCKED'),
    ('readback', 'UNKNOWN', 'REMOTE_UNKNOWN'),
])
@pytest.mark.parametrize('binding', ['artifact_id', 'target_id'])
def test_export_proof_cannot_be_reused_for_another_artifact_or_target(
        stage, projected_status, release_status, binding):
    case = copy.deepcopy(next(case for case in CASES if case['expected']['contract_error'] is None and
                              case['expected']['result']['release']['status'] == 'CURRENT_VERIFIED'))
    export = case['input']['exports'][0]
    proof = next(item for item in case['input']['evidence']
                 if item['evidence_id'] == export[stage]['evidence_ref'])
    proof[binding] = 'OTHER_BINDING'
    result = _reduce(case['input'], case['scenario_mode'])
    target = result['release']['per_target_outcomes'][0]
    assert target[stage] == projected_status
    assert result['release']['status'] == release_status
    assert result['release']['exit_status'] == 1


@pytest.mark.parametrize('ref_name', ['evidence_ref', 'quality_evidence_ref', 'freshness_evidence_ref'])
@pytest.mark.parametrize('binding', ['artifact_id', 'target_id'])
def test_previous_proof_requires_the_previous_target_subject(ref_name, binding):
    case = copy.deepcopy(next(case for case in CASES if case['expected']['contract_error'] is None and
                              any(target['fallback_eligible'] for target in
                                  case['expected']['result']['release']['per_target_outcomes'])))
    previous = case['input']['exports'][0]['previous']
    proof = next(item for item in case['input']['evidence']
                 if item['evidence_id'] == previous[ref_name])
    proof[binding] = 'OTHER_BINDING'
    result = _reduce(case['input'], case['scenario_mode'])
    assert result['release']['per_target_outcomes'][0]['fallback_eligible'] is False
    assert result['release']['status'] != 'CURRENT_VERIFIED'
