"""Independent B2 fixture oracles; no production admission, collection or writers."""

import copy
from decimal import Decimal
import hashlib
from io import StringIO
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import talib


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / 'tests/fixtures/b2'
RAW_COLUMNS = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end']
KEY_FIELDS = ['engine', 'market', 'source_scope', 'board', 'raw_instrument_id', 'interval', 'begin']
CANARY = 'B2_SYNTHETIC_CREDENTIAL_CANARY'


@pytest.fixture
def cases():
    return json.loads((FIXTURES / 'cases.json').read_text(encoding='utf-8'))


@pytest.fixture
def candidate():
    import data_quality
    return data_quality


def _frame(case):
    payload = case['retained_evidence']['frame_payload']
    return pd.DataFrame(copy.deepcopy(payload[2]), columns=payload[1], dtype=object)


def _assess(candidate, case, **kwargs):
    return candidate.assess_candles(_frame(case), spec=case['spec'], metadata=case['metadata'],
                                  retained_evidence=case['retained_evidence'], **kwargs)


def _reasons(report):
    return {reason for finding in report['raw_assessment']['findings'] for reason in finding['reasons']}


def _group_reasons(report):
    return {reason for group in report['raw_assessment']['groups'] for reason in group['reasons']}


def _eligibility_reasons(report):
    return {reason for group in report['eligibility']['groups'] for reason in group['reasons']}


def _check(report, identifier):
    matches = [item for item in report['quality_result']['checks'] if item['check_id'] == identifier]
    assert len(matches) == 1
    return matches[0]


def _logical(value, schema):
    # Independent miniature encoder from the literal F0 array grammar, not helper code.
    try:
        serialized = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
        digest = hashlib.sha256(serialized.encode('utf-8')).hexdigest()
    except (TypeError, ValueError):
        # Unsupported normalization is deliberately unverified; raw predicates still run.
        digest = '0' * 64
    return {'basis': 'LOGICAL_ROWS_SHA256', 'value': digest, 'schema_version': schema}


def _key_array(key):
    return [key[field] for field in KEY_FIELDS]


def _bind_fresh_case(case, selected, *, allocate_snapshot=True):
    """Capture a fresh synthetic authority before the assessor sees the input."""
    metadata, evidence = case['metadata'], case['retained_evidence']
    schema = metadata['schema_version']
    records = {record['record_id']: record for record in metadata['identity_records']}
    locators, frame_rows = [], []
    raw_by_locator = {}
    for source in evidence['retained_sources']:
        for row in source['rows']:
            row['raw_row_fingerprint'] = _logical(
                [_key_array(row['raw_key']), row['columns'], row['values']], schema)
            raw_by_locator[(source['identity_record_id'], source['provenance_ref'], row['raw_row_ordinal'])] = row
    # A freshly captured synthetic candidate is a different snapshot. This ID is
    # fixture bookkeeping, not a proposed production content-normalization rule.
    fixture_identity = repr((schema, metadata['dataset_id'],
        [record['identity'] for record in metadata['identity_records']],
        [(locator, row['raw_key'], row['columns'], row['values']) for locator, row in raw_by_locator.items()], selected))
    if allocate_snapshot:
        metadata['snapshot_id'] = 'B2_CAPTURED_' + hashlib.sha256(fixture_identity.encode('utf-8')).hexdigest()[:24]
    for ordinal, locator in enumerate(selected):
        row = raw_by_locator[tuple(locator)]
        record = records[locator[0]]
        locators.append({'ordinal': ordinal, 'identity_record_id': locator[0],
                        'begin': row['raw_key']['begin'],
                        'source_row_ref': {'provenance_ref': locator[1], 'raw_row_ordinal': locator[2],
                                           'raw_key': copy.deepcopy(row['raw_key']),
                                           'raw_row_fingerprint': copy.deepcopy(row['raw_row_fingerprint'])}})
        frame_rows.append(copy.deepcopy(row['values']) + [record['identity']['display_ticker'], None])
    evidence['frame_payload'][2] = frame_rows
    frame_fingerprint = _logical(evidence['frame_payload'], schema)
    encoded_entries = []
    for entry in locators:
        source = entry['source_row_ref']
        encoded_entries.append([entry['ordinal'], entry['identity_record_id'], entry['begin'],
                                source['provenance_ref'], source['raw_row_ordinal'],
                                _key_array(source['raw_key']), source['raw_row_fingerprint']['value']])
    binding_fingerprint = _logical([metadata['snapshot_id'], metadata['dataset_id'], schema,
                                   frame_fingerprint['value'], encoded_entries], schema)
    authority = {'snapshot_id': metadata['snapshot_id'], 'dataset_id': metadata['dataset_id'],
                 'schema_version': schema, 'frame_fingerprint': frame_fingerprint,
                 'entries': locators, 'binding_fingerprint': binding_fingerprint,
                 'evidence_ref': 'B2/independently-captured-fixture-selection'}
    evidence['producer_selection'] = copy.deepcopy(authority)
    bindings = copy.deepcopy(authority)
    bindings['row_count'] = len(locators)
    bindings['binding_evidence_ref'] = bindings.pop('evidence_ref')
    metadata['row_bindings'] = bindings
    metadata['counts'].update(rows_received=len(raw_by_locator), rows_accepted=len(selected), rows_quarantined=0)
    evidence['source_membership'] = []
    for source in evidence['retained_sources']:
        record = records[source['identity_record_id']]
        count = sum(locator[0] == record['record_id'] for locator in selected)
        record['counts'].update(rows_received=len(source['rows']), rows_accepted=count, rows_quarantined=0)
        record['coverage_evidence']['expected_rows'] = len(source['rows'])
        evidence['source_membership'].append({'identity_record_id': record['record_id'],
            'evidence_ref': record['coverage_evidence']['evidence_ref'],
            'identity': copy.deepcopy(record['identity']), 'interval': record['interval'],
            'requested_bounds': copy.deepcopy(record['requested_bounds']),
            'expected_raw_keys': [copy.deepcopy(row['raw_key']) for row in source['rows']]})
    return case


def _one_identity(cases, rows=None, *, selected=None, ticker=None):
    case = copy.deepcopy(cases['e19'])
    record = case['metadata']['identity_records'][0]
    source = case['retained_evidence']['retained_sources'][0]
    case['metadata']['identity_records'] = [record]
    case['metadata']['counts']['instruments'] = 1
    case['metadata']['counts']['pages'] = 1
    case['retained_evidence']['retained_sources'] = [source]
    source['rows'] = copy.deepcopy(source['rows'][:1] if rows is None else rows)
    if ticker is not None:
        record['identity']['raw_instrument_id'] = ticker
        record['identity']['display_ticker'] = ticker
    for ordinal, row in enumerate(source['rows']):
        row['raw_row_ordinal'] = ordinal
        row['raw_key']['raw_instrument_id'] = record['identity']['raw_instrument_id']
        row['raw_key']['begin'] = row['values'][6]
    selected = list(range(len(source['rows']))) if selected is None else selected
    return _bind_fresh_case(case, [[record['record_id'], source['provenance_ref'], ordinal]
                                  for ordinal in selected])


def _changed_row(cases, changes):
    row = copy.deepcopy(cases['e19']['retained_evidence']['retained_sources'][0]['rows'][0])
    for name, value in changes.items():
        row['values'][RAW_COLUMNS.index(name)] = value
    return row


def _capture_declared_frame(case, columns, rows):
    """Freeze a deliberately malformed schema independently of the assessor."""
    metadata, evidence = case['metadata'], case['retained_evidence']
    schema = 'B2_REVIEW_CAPTURED_FRAME_SCHEMA'
    metadata['schema_version'] = case['spec']['schema_version'] = schema
    metadata['snapshot_id'] = 'B2_REVIEW_CAPTURE_' + hashlib.sha256(
        repr((columns, rows)).encode('utf-8')).hexdigest()[:24]
    evidence['frame_payload'] = [schema, copy.deepcopy(columns), copy.deepcopy(rows)]
    old_columns = {column['name']: column for column in case['spec']['columns']}
    case['spec']['columns'] = [copy.deepcopy(old_columns[name]) for name in columns]
    sources = {}
    for source in evidence['retained_sources']:
        for row in source['rows']:
            row['raw_row_fingerprint'] = _logical(
                [_key_array(row['raw_key']), row['columns'], row['values']], schema)
            sources[(source['identity_record_id'], source['provenance_ref'], row['raw_row_ordinal'])] = row
    bindings = metadata['row_bindings']
    bindings.update(snapshot_id=metadata['snapshot_id'], schema_version=schema,
                    row_count=len(rows), frame_fingerprint=_logical(evidence['frame_payload'], schema))
    encoded = []
    for entry in bindings['entries']:
        source = entry['source_row_ref']
        row = sources[(entry['identity_record_id'], source['provenance_ref'], source['raw_row_ordinal'])]
        source['raw_row_fingerprint'] = copy.deepcopy(row['raw_row_fingerprint'])
        encoded.append([entry['ordinal'], entry['identity_record_id'], entry['begin'],
            source['provenance_ref'], source['raw_row_ordinal'], _key_array(source['raw_key']),
            source['raw_row_fingerprint']['value']])
    bindings['binding_fingerprint'] = _logical([metadata['snapshot_id'], metadata['dataset_id'],
        schema, bindings['frame_fingerprint']['value'], encoded], schema)
    selection = copy.deepcopy(bindings)
    selection.pop('row_count')
    selection['evidence_ref'] = selection.pop('binding_evidence_ref')
    evidence['producer_selection'] = selection
    return case


@pytest.mark.parametrize('schema_case', ['duplicate_close', 'missing_volume'])
def test_captured_schema_cannot_hide_ambiguous_or_missing_raw_field_mapping(candidate, cases, schema_case):
    case = _one_identity(cases)
    payload = case['retained_evidence']['frame_payload']
    columns, rows = copy.deepcopy(payload[1]), copy.deepcopy(payload[2])
    if schema_case == 'duplicate_close':
        position = columns.index('close')
        columns.insert(position, 'close')
        rows[0].insert(position, 999)
        assert rows[0][position] != rows[0][position + 1]
        assert rows[0][position + 1] == case['retained_evidence']['retained_sources'][0]['rows'][0]['values'][1]
    else:
        position = columns.index('volume')
        columns.pop(position)
        rows[0].pop(position)
    case = _capture_declared_frame(case, columns, rows)
    before = copy.deepcopy(case)
    assert case['metadata']['row_bindings']['frame_fingerprint'] == case['retained_evidence']['producer_selection']['frame_fingerprint']
    report = _assess(candidate, case)
    assert report['raw_assessment']['status'] == 'PASS'
    assert _check(report, 'IDENTITY_BINDING_MISMATCH')['status'] == 'FAIL'
    assert report['quality_result']['status'] == 'FAIL'
    assert report['quality_result']['accepted_rows'] is None
    assert report['eligibility']['status'] == 'FAIL'
    assert all(group['candidate_ordinals'] == [] for group in report['eligibility']['groups'])
    assert case == before


@pytest.mark.parametrize('outcome', ['positive', 'negative', 'unknown'])
def test_every_emitted_quality_check_uses_f0_reason_nullability(candidate, cases, outcome):
    case = copy.deepcopy(cases['e19'])
    if outcome == 'negative':
        case['metadata'] = copy.deepcopy(cases['e19_negative_metadata'])
    elif outcome == 'unknown':
        case['retained_evidence'].pop('producer_selection')
    report = _assess(candidate, case)
    checks = report['quality_result']['checks']
    assert checks and any(check['status'] == 'PASS' for check in checks)
    assert any(check['status'] == 'NOT_VERIFIED' for check in checks)
    assert any(check['status'] == 'FAIL' for check in checks) == (outcome == 'negative')
    for check in checks:
        assert set(check) == {'check_id', 'status', 'reason', 'evidence_ref'}
        if check['status'] == 'PASS':
            assert check['reason'] is None
        else:
            assert check['status'] in {'FAIL', 'NOT_VERIFIED'}
            assert type(check['reason']) is str and check['reason'].strip()


def _assert_unadmitted_context(candidate, case, expected):
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert report['raw_assessment']['status'] == 'PASS'
    assert report['coverage_assessment']['status'] == 'COMPLETE'
    assert report['eligibility']['status'] == expected
    assert all(group['candidate_ordinals'] == [] for group in report['eligibility']['groups'])
    assert report['quality_result']['status'] == expected
    assert report['quality_result']['accepted_rows'] is None
    assert case == before


@pytest.mark.parametrize('field', ['run_id', 'as_of', 'contract_version', 'source'])
def test_missing_required_metadata_context_is_not_filled_from_source_rows(candidate, cases, field):
    case = _one_identity(cases)
    case['metadata'].pop(field)
    _assert_unadmitted_context(candidate, case, 'NOT_VERIFIED')


@pytest.mark.parametrize('field,value', [
    ('run_id', None), ('run_id', ''), ('run_id', '   '), ('run_id', 7),
    ('source', None), ('source', ''), ('source', '   '), ('source', 7),
    ('contract_version', None), ('contract_version', '1'), ('contract_version', True),
    ('as_of', None), ('as_of', ''), ('as_of', 7),
    ('as_of', '2026-10-04T09:00:00'),
    ('as_of', '2026-02-30T09:00:00Z'),
    ('as_of', '2026-10-04 09:00:00Z'),
    ('as_of', '2026-10-04\u202809:00:00Z'),
])
def test_unsupported_required_metadata_context_has_no_local_eligibility(candidate, cases, field, value):
    case = _one_identity(cases)
    case['metadata'][field] = value
    _assert_unadmitted_context(candidate, case, 'NOT_VERIFIED')


@pytest.mark.parametrize('version', [None, '1', True])
def test_spec_contract_version_is_strict_integer_context(candidate, cases, version):
    case = _one_identity(cases)
    case['spec']['contract_version'] = version
    _assert_unadmitted_context(candidate, case, 'NOT_VERIFIED')


def test_missing_spec_contract_version_is_not_inferred_from_metadata(candidate, cases):
    case = _one_identity(cases)
    case['spec'].pop('contract_version')
    _assert_unadmitted_context(candidate, case, 'NOT_VERIFIED')


@pytest.mark.parametrize('spec_version,metadata_version', [(1, 2), (2, 1)])
def test_known_contract_version_disagreement_is_a_failure(candidate, cases, spec_version, metadata_version):
    case = _one_identity(cases)
    case['spec']['contract_version'] = spec_version
    case['metadata']['contract_version'] = metadata_version
    _assert_unadmitted_context(candidate, case, 'FAIL')


def test_matching_unsupported_contract_version_does_not_claim_version_two_support(candidate, cases):
    case = _one_identity(cases)
    case['spec']['contract_version'] = case['metadata']['contract_version'] = 2
    _assert_unadmitted_context(candidate, case, 'NOT_VERIFIED')


def test_explicit_other_source_is_a_known_approved_source_contradiction(candidate, cases):
    case = _one_identity(cases)
    case['metadata']['source'] = 'B2_UNAPPROVED_SOURCE'
    _assert_unadmitted_context(candidate, case, 'FAIL')


def test_explicit_non_utc_cutoff_is_a_failure_without_timezone_repair(candidate, cases):
    case = _one_identity(cases)
    case['metadata']['as_of'] = '2026-10-04T09:00:00+03:00'
    _assert_unadmitted_context(candidate, case, 'FAIL')


@pytest.mark.parametrize('offset', ['+00:00:00.0000001', '-00:00:00.0000001',
                                  '+000000.0000001', '-000000.0000001'])
def test_explicit_nonzero_submicrosecond_utc_offset_cannot_round_to_approved_zero(candidate, cases, offset):
    case = _one_identity(cases)
    case['metadata']['as_of'] = '2026-10-04T09:00:00' + offset
    _assert_unadmitted_context(candidate, case, 'FAIL')


@pytest.mark.parametrize('field', ['dataset_id', 'schema_version'])
def test_different_known_spec_identity_fails_against_valid_metadata_and_authority(candidate, cases, field):
    case = _one_identity(cases)
    case['spec'][field] = 'B2_DIFFERENT_KNOWN_SPEC_' + field.upper()
    _assert_unadmitted_context(candidate, case, 'FAIL')


@pytest.mark.parametrize('as_of', ['2026-10-04T09:00:00Z', '2026-10-04T09:00:00+00:00',
                                  '2026-10-04T09:00:00.125Z', '2026-10-04T09:00:00.125+00:00',
                                  '2026-10-04T09:00:00.1234567Z',
                                  '2026-10-04T09:00:00+00:00:00.0000000'])
def test_valid_approved_source_and_utc_context_is_preserved(candidate, cases, as_of):
    case = _one_identity(cases)
    case['metadata']['as_of'] = as_of
    case['metadata']['snapshot_id'] = 'B2_UTC_CONTEXT_' + hashlib.sha256(as_of.encode('utf-8')).hexdigest()[:24]
    source = case['retained_evidence']['retained_sources'][0]
    case = _bind_fresh_case(case, [[source['identity_record_id'], source['provenance_ref'], 0]],
                            allocate_snapshot=False)
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert case['metadata']['source'] == 'MOEX_ISS_RAW'
    assert type(case['metadata']['contract_version']) is int and case['metadata']['contract_version'] == 1
    assert type(case['spec']['contract_version']) is int and case['spec']['contract_version'] == 1
    assert report['raw_assessment']['status'] == 'PASS'
    assert report['eligibility']['status'] == 'PASS'
    assert report['quality_result']['accepted_rows'] == 1
    assert report['quality_result']['as_of'] == as_of
    assert report['quality_result']['run_id'] == before['metadata']['run_id']
    assert report['quality_result']['status'] == 'NOT_VERIFIED'
    assert report['raw_assessment']['source_verification'] == 'NOT_VERIFIED'
    assert case == before


def test_e19_fixed_authority_and_candidate_have_distinct_populations(candidate, cases):
    case = copy.deepcopy(cases['e19'])
    report = _assess(candidate, case)
    assert report['raw_assessment']['rows_checked'] == 4
    assert report['raw_assessment']['selected_raw_rows'] == 3
    assert report['raw_assessment']['unselected_raw_rows'] == 1
    assert report['raw_assessment']['quarantined_rows'] == 0
    assert report['raw_assessment']['status'] == 'PASS'
    assert report['quality_result']['rows_checked'] == 3
    assert report['quality_result']['accepted_rows'] == 3
    assert report['quality_result']['quarantined_rows'] == 0
    assert report['eligibility']['status'] == 'PASS'
    assert report['coverage_assessment']['status'] == 'COMPLETE'
    assert _check(report, 'FULL_DATASET_VALIDATION')['status'] == 'NOT_VERIFIED'
    assert report['quality_result']['status'] == 'NOT_VERIFIED'
    assert report['eligibility']['policy'] == 'CONTINUE_ACROSS_QUARANTINE'
    assert report['eligibility']['production_admission'] == 'NOT_ENABLED'
    assert report['raw_assessment']['source_verification'] == 'NOT_VERIFIED'


def test_e19_recomputed_negative_cannot_replace_independent_selection(candidate, cases):
    case = copy.deepcopy(cases['e19'])
    authority = copy.deepcopy(case['retained_evidence']['producer_selection'])
    original_frame = copy.deepcopy(case['retained_evidence']['frame_payload'])
    case['metadata'] = copy.deepcopy(cases['e19_negative_metadata'])
    assert case['metadata']['counts'] == cases['e19']['metadata']['counts']
    assert case['metadata']['row_bindings']['binding_fingerprint'] != authority['binding_fingerprint']
    report = _assess(candidate, case)
    assert report['quality_result']['status'] == 'FAIL'
    assert report['eligibility']['status'] == 'FAIL'
    assert _check(report, 'IDENTITY_BINDING_MISMATCH')['status'] == 'FAIL'
    assert report['quality_result']['accepted_rows'] is None
    assert case['retained_evidence']['producer_selection'] == authority
    assert case['retained_evidence']['frame_payload'] == original_frame


@pytest.mark.parametrize('missing', ['producer_selection', 'serialization_profile'])
def test_missing_immutable_lineage_is_not_verified(candidate, cases, missing):
    case = copy.deepcopy(cases['e19'])
    case['retained_evidence'].pop(missing)
    report = _assess(candidate, case)
    assert report['eligibility']['status'] == 'NOT_VERIFIED'
    assert report['quality_result']['status'] == 'NOT_VERIFIED'
    assert report['quality_result']['accepted_rows'] is None
    assert _check(report, 'IMMUTABLE_LINEAGE_NOT_VERIFIED')['status'] == 'NOT_VERIFIED'


@pytest.mark.parametrize('field', ['snapshot_id', 'dataset_id', 'schema_version'])
@pytest.mark.parametrize('missing', [None, '', '   '])
def test_matching_missing_context_is_unverified_even_with_consistent_frozen_digests(candidate, cases, field, missing):
    case = _one_identity(cases)
    case['metadata'][field] = missing
    if field in {'dataset_id', 'schema_version'}:
        case['spec'][field] = missing
    if field == 'schema_version':
        case['retained_evidence']['frame_payload'][0] = missing
    source = case['retained_evidence']['retained_sources'][0]
    # This malformed authority is independently frozen with the same missing
    # context and recomputed digests; matching absence is still not identity.
    case = _bind_fresh_case(case, [[source['identity_record_id'], source['provenance_ref'], 0]],
                            allocate_snapshot=False)
    before = copy.deepcopy(case)
    assert case['metadata']['row_bindings'][field] == missing
    assert case['retained_evidence']['producer_selection'][field] == missing
    report = _assess(candidate, case)
    assert report['raw_assessment']['status'] == 'PASS'
    assert report['raw_assessment']['quarantined_rows'] == 0
    assert report['eligibility']['status'] == 'NOT_VERIFIED'
    assert report['eligibility']['groups'][0]['candidate_ordinals'] == []
    assert report['quality_result']['status'] == 'NOT_VERIFIED'
    assert report['quality_result']['accepted_rows'] is None
    assert _check(report, 'IMMUTABLE_LINEAGE_NOT_VERIFIED')['status'] == 'NOT_VERIFIED'
    assert case == before


def test_empty_identity_record_id_cannot_become_verified_by_matching_every_reference(candidate, cases):
    case = _one_identity(cases)
    record = case['metadata']['identity_records'][0]
    source = case['retained_evidence']['retained_sources'][0]
    record['record_id'] = ''
    source['identity_record_id'] = ''
    case = _bind_fresh_case(case, [['', source['provenance_ref'], 0]])
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert report['raw_assessment']['status'] == 'PASS'
    assert report['eligibility']['status'] == 'FAIL'
    assert all(group['candidate_ordinals'] == [] for group in report['eligibility']['groups'])
    assert report['quality_result']['status'] == 'FAIL'
    assert report['quality_result']['accepted_rows'] is None
    assert _check(report, 'IDENTITY_BINDING_MISMATCH')['status'] == 'FAIL'
    assert case == before


@pytest.mark.parametrize('mutation', ['snapshot', 'ordinal', 'provenance', 'raw_content', 'binding_count'])
def test_known_binding_contradictions_fail(candidate, cases, mutation):
    case = copy.deepcopy(cases['e19'])
    bindings = case['metadata']['row_bindings']
    if mutation == 'snapshot':
        bindings['snapshot_id'] = 'B2_FOREIGN_SNAPSHOT'
    elif mutation == 'ordinal':
        bindings['entries'][1]['ordinal'] = 0
    elif mutation == 'provenance':
        bindings['entries'][1]['source_row_ref']['provenance_ref'] = 'B2/wrong-source'
    elif mutation == 'raw_content':
        case['retained_evidence']['frame_payload'][2][1][1] = 999
    else:
        bindings['entries'].pop()
    report = _assess(candidate, case)
    assert report['eligibility']['status'] == 'FAIL'
    assert report['quality_result']['status'] == 'FAIL'
    assert _check(report, 'IDENTITY_BINDING_MISMATCH')['status'] == 'FAIL'


@pytest.mark.parametrize('mutation', ['same_snapshot_reordered', 'new_snapshot_without_new_authority'])
def test_recomputed_input_fingerprints_do_not_replace_frozen_snapshot_authority(candidate, cases, mutation):
    case = copy.deepcopy(cases['e19'])
    metadata, evidence = case['metadata'], case['retained_evidence']
    authority = copy.deepcopy(evidence['producer_selection'])
    bindings = metadata['row_bindings']
    if mutation == 'same_snapshot_reordered':
        evidence['frame_payload'][2] = list(reversed(evidence['frame_payload'][2]))
        bindings['entries'] = list(reversed(bindings['entries']))
        for ordinal, entry in enumerate(bindings['entries']):
            entry['ordinal'] = ordinal
    else:
        metadata['snapshot_id'] = 'B2_NEW_ID_WITHOUT_INDEPENDENT_NEW_SELECTION'
        bindings['snapshot_id'] = metadata['snapshot_id']
    schema = metadata['schema_version']
    bindings['frame_fingerprint'] = _logical(evidence['frame_payload'], schema)
    encoded_entries = []
    for entry in bindings['entries']:
        source = entry['source_row_ref']
        encoded_entries.append([entry['ordinal'], entry['identity_record_id'], entry['begin'],
            source['provenance_ref'], source['raw_row_ordinal'], _key_array(source['raw_key']),
            source['raw_row_fingerprint']['value']])
    bindings['binding_fingerprint'] = _logical([metadata['snapshot_id'], metadata['dataset_id'],
        schema, bindings['frame_fingerprint']['value'], encoded_entries], schema)
    before = copy.deepcopy(case)
    assert bindings['binding_fingerprint'] != authority['binding_fingerprint']
    report = _assess(candidate, case)
    assert _check(report, 'IDENTITY_BINDING_MISMATCH')['status'] == 'FAIL'
    assert report['quality_result']['status'] == 'FAIL'
    assert report['quality_result']['accepted_rows'] is None
    assert report['eligibility']['status'] == 'FAIL'
    assert evidence['producer_selection'] == authority
    assert case == before


@pytest.mark.parametrize('index', range(6))
@pytest.mark.parametrize('invalid,reason', [(None, 'RAW_NUMERIC_MISSING'),
                                         (pd.NA, 'RAW_NUMERIC_MISSING'),
                                         (float('nan'), 'RAW_NUMERIC_NONFINITE'),
                                         (float('inf'), 'RAW_NUMERIC_NONFINITE'),
                                         (float('-inf'), 'RAW_NUMERIC_NONFINITE')])
def test_all_six_numeric_fields_reject_missing_or_nonfinite(candidate, cases, index, invalid, reason):
    row = _changed_row(cases, {RAW_COLUMNS[index]: invalid})
    report = _assess(candidate, _one_identity(cases, [row]))
    assert report['raw_assessment']['status'] == 'FAIL'
    assert report['raw_assessment']['quarantined_rows'] == 1
    assert reason in _reasons(report)


@pytest.mark.parametrize('index', range(6))
@pytest.mark.parametrize('unsupported', [True, False, np.bool_(True), np.bool_(False),
                                       '100.00', Decimal('100.00'), 1 + 2j, [1], {'value': 1}])
def test_all_six_numeric_fields_do_not_coerce_unsupported_types(candidate, cases, index, unsupported):
    row = _changed_row(cases, {RAW_COLUMNS[index]: unsupported})
    report = _assess(candidate, _one_identity(cases, [row]))
    assert report['raw_assessment']['status'] == 'NOT_VERIFIED'
    assert 'UNSUPPORTED_NUMERIC_REPRESENTATION' in _reasons(report)


@pytest.mark.parametrize('value', [10 ** 500, np.int64(12), np.float64(12.5), np.float32(12.5)])
def test_finite_python_numpy_numbers_and_huge_integer_do_not_overflow(candidate, cases, value):
    row = _changed_row(cases, {'volume': value})
    case = _one_identity(cases, [row])
    report = _assess(candidate, case)
    assert report['raw_assessment']['status'] == 'PASS'
    assert report['raw_assessment']['quarantined_rows'] == 0
    assert _reasons(report) == set()
    assert case['retained_evidence']['retained_sources'][0]['rows'][0]['values'][5] == value


@pytest.mark.parametrize('index', range(6))
def test_python_floats_are_raw_valid_but_not_a_new_normalization_profile(candidate, cases, index):
    row = _changed_row(cases, {RAW_COLUMNS[index]: float(cases['e19']['retained_evidence']['retained_sources'][0]['rows'][0]['values'][index])})
    report = _assess(candidate, _one_identity(cases, [row]))
    assert report['raw_assessment']['status'] == 'PASS'
    assert report['eligibility']['status'] == 'NOT_VERIFIED'
    assert _check(report, 'IMMUTABLE_LINEAGE_NOT_VERIFIED')['status'] == 'NOT_VERIFIED'


@pytest.mark.parametrize('case_index', range(6))
def test_numeric_rules_preserve_zero_and_do_not_invent_value_policy(candidate, cases, case_index):
    rule = cases['raw_numeric_cases'][case_index]
    case = _one_identity(cases, [_changed_row(cases, rule['changes'])])
    report = _assess(candidate, case)
    assert report['raw_assessment']['status'] == rule['status']
    if rule['reason'] is not None:
        assert rule['reason'] in _reasons(report)
    else:
        assert _reasons(report) == set()


@pytest.mark.parametrize('case_index', range(6))
def test_timestamp_rules_do_not_generalize_abio_to_ordinary_duration(candidate, cases, case_index):
    rule = cases['timestamp_cases'][case_index]
    report = _assess(candidate, _one_identity(cases, [_changed_row(cases, rule['changes'])]))
    assert report['raw_assessment']['status'] == rule['status']
    if rule['reason'] is not None:
        assert rule['reason'] in _reasons(report)
    assert 'CONFIRMED_ABIO_MULTIYEAR_OBSERVATION' not in _reasons(report)


@pytest.mark.parametrize('field', ['begin', 'end'])
@pytest.mark.parametrize('invalid', [None, pd.NaT])
def test_required_timestamps_cannot_be_missing(candidate, cases, field, invalid):
    report = _assess(candidate, _one_identity(cases, [_changed_row(cases, {field: invalid})]))
    assert report['raw_assessment']['status'] == 'FAIL'
    assert 'RAW_TIMESTAMP_INVALID' in _reasons(report)


@pytest.mark.parametrize('field', ['begin', 'end'])
@pytest.mark.parametrize('invalid', [20240101, True, ['2024-01-01 10:00:00'], pd.Timestamp('2024-01-01 10:00:00')])
def test_timestamp_types_are_not_implicitly_stringified(candidate, cases, field, invalid):
    report = _assess(candidate, _one_identity(cases, [_changed_row(cases, {field: invalid})]))
    assert report['raw_assessment']['status'] == 'NOT_VERIFIED'
    assert 'UNSUPPORTED_TIMESTAMP_REPRESENTATION' in _reasons(report)


@pytest.mark.parametrize('difference,reason', [(None, 'DUPLICATE_EXACT'),
                                             ({'end': '2024-01-01 18:46:00'}, 'END_VARIANT'),
                                             ({'close': 102}, 'OHLC_VARIANT'),
                                             ({'volume': 999}, 'RAW_VALUE_VARIANT')])
@pytest.mark.parametrize('reverse', [False, True])
def test_duplicate_keys_distinguish_exact_end_ohlc_and_other_values(candidate, cases, difference, reason, reverse):
    first = _changed_row(cases, {})
    second = _changed_row(cases, difference or {})
    case = _one_identity(cases, [second, first] if reverse else [first, second])
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    groups = report['raw_assessment']['groups']
    assert len(groups) == 1 and groups[0]['variant_count'] == 2
    assert len(groups[0]['variants']) == 2
    if difference is None:
        assert groups[0]['classification'] == 'DUPLICATE_EXACT'
        assert report['raw_assessment']['status'] == 'PASS'
        assert report['raw_assessment']['quarantined_rows'] == 0
    else:
        assert groups[0]['classification'] == 'SOURCE_KEY_CONFLICT'
        assert report['raw_assessment']['status'] == 'FAIL'
        assert report['raw_assessment']['quarantined_rows'] == 2
        assert reason in groups[0]['reasons']
        assert 'REVISION_NOT_PROVEN' in groups[0]['reasons']
    assert report['eligibility']['status'] == 'FAIL'
    assert case == before


@pytest.mark.parametrize('reverse', [False, True])
def test_unselected_conflict_is_not_hidden_by_candidate_selection(candidate, cases, reverse):
    first, second = _changed_row(cases, {}), _changed_row(cases, {'end': '2024-01-01 18:46:00'})
    case = _one_identity(cases, [second, first] if reverse else [first, second], selected=[1 if reverse else 0])
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert report['raw_assessment']['rows_checked'] == 2
    assert report['raw_assessment']['selected_raw_rows'] == 1
    assert report['raw_assessment']['unselected_raw_rows'] == 1
    assert report['raw_assessment']['quarantined_rows'] == 2
    assert report['quality_result']['rows_checked'] == 1
    assert report['quality_result']['quarantined_rows'] == 1
    assert 'UNSELECTED_RAW_VARIANT_CONFLICT' in _group_reasons(report)
    assert report['eligibility']['status'] == 'FAIL'
    assert case == before


@pytest.mark.parametrize('authority', ['absent', 'metadata_claim', 'independent_evidence'])
@pytest.mark.parametrize('reverse', [False, True])
def test_revision_requires_independent_evidence_and_never_selects_keep_last(candidate, cases, authority, reverse):
    first, second = _changed_row(cases, {}), _changed_row(cases, {'close': 102})
    case = _one_identity(cases, [second, first] if reverse else [first, second])
    source = case['retained_evidence']['retained_sources'][0]
    revision = {'snapshot_id': case['metadata']['snapshot_id'],
        'raw_key': copy.deepcopy(source['rows'][0]['raw_key']),
        'old_locator': [source['identity_record_id'], source['provenance_ref'], 1 if reverse else 0],
        'new_locator': [source['identity_record_id'], source['provenance_ref'], 0 if reverse else 1],
        'evidence_ref': 'B2/retained-source-revision-link'}
    if authority == 'metadata_claim':
        case['metadata']['revision_evidence'] = [revision]
    elif authority == 'independent_evidence':
        case['retained_evidence']['revision_evidence'] = [revision]
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert report['eligibility']['status'] == 'FAIL'
    group = report['raw_assessment']['groups'][0]
    if authority == 'independent_evidence':
        assert group['classification'] == 'EVIDENCED_REVISION'
        assert 'REVISION_SELECTION_NOT_APPLIED' in group['reasons']
        assert report['raw_assessment']['status'] == 'PASS'
    else:
        assert group['classification'] == 'SOURCE_KEY_CONFLICT'
        assert 'REVISION_NOT_PROVEN' in group['reasons']
    assert case == before


@pytest.mark.parametrize('classification', ['DUPLICATE_EXACT', 'EVIDENCED_REVISION'])
def test_unresolved_selection_blocks_healthy_key_in_same_computational_group(candidate, cases, classification):
    first = _changed_row(cases, {})
    second = _changed_row(cases, {} if classification == 'DUPLICATE_EXACT' else {'close': 102})
    healthy = copy.deepcopy(cases['e19']['retained_evidence']['retained_sources'][0]['rows'][1])
    case = _one_identity(cases, [first, second, healthy])
    source = case['retained_evidence']['retained_sources'][0]
    if classification == 'EVIDENCED_REVISION':
        case['retained_evidence']['revision_evidence'] = [{
            'snapshot_id': case['metadata']['snapshot_id'],
            'raw_key': copy.deepcopy(source['rows'][0]['raw_key']),
            'old_locator': [source['identity_record_id'], source['provenance_ref'], 0],
            'new_locator': [source['identity_record_id'], source['provenance_ref'], 1],
            'evidence_ref': 'B2/independently-retained-revision-with-healthy-key'}]
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert report['raw_assessment']['status'] == 'PASS'
    assert report['raw_assessment']['quarantined_rows'] == 0
    assert {group['classification'] for group in report['raw_assessment']['groups']} == {classification, 'UNIQUE'}
    assert report['eligibility']['status'] == 'FAIL'
    assert len(report['eligibility']['groups']) == 1
    group = report['eligibility']['groups'][0]
    assert group['status'] == 'FAIL'
    assert group['candidate_ordinals'] == []
    assert report['quality_result']['status'] == 'FAIL'
    assert case == before


def test_same_raw_id_and_begin_remain_separate_on_two_explicit_boards(candidate, cases):
    case = copy.deepcopy(cases['e19'])
    selected = []
    for record, source, board in zip(case['metadata']['identity_records'],
            case['retained_evidence']['retained_sources'], ['B2_BOARD_A', 'B2_BOARD_B']):
        record['identity'].update(source_scope='BOARD', board=board, raw_instrument_id='B2_SHARED_RAW_ID')
        record['identity']['null_reasons'].pop('board')
        for provenance in record['provenance']:
            provenance['source_scope'] = 'BOARD'
        for row in source['rows']:
            row['raw_key'].update(source_scope='BOARD', board=board, raw_instrument_id='B2_SHARED_RAW_ID')
            row['raw_key']['null_reasons'].pop('board')
            selected.append([record['record_id'], source['provenance_ref'], row['raw_row_ordinal']])
    case = _bind_fresh_case(case, selected)
    before = copy.deepcopy(case)
    frame = _frame(case)
    assert frame.iloc[1].tolist() == frame.iloc[2].tolist()
    report = _assess(candidate, case)
    assert report['raw_assessment']['rows_checked'] == 4
    assert report['raw_assessment']['quarantined_rows'] == 0
    assert report['raw_assessment']['status'] == 'PASS'
    assert len(report['raw_assessment']['groups']) == 4
    assert all(group['classification'] == 'UNIQUE' for group in report['raw_assessment']['groups'])
    assert report['eligibility']['status'] == 'PASS'
    assert report['quality_result']['accepted_rows'] == 4
    groups = {group['identity_record_id']: group for group in report['eligibility']['groups']}
    assert groups[case['metadata']['identity_records'][0]['record_id']]['candidate_ordinals'] == [0, 1]
    assert groups[case['metadata']['identity_records'][1]['record_id']]['candidate_ordinals'] == [2, 3]
    assert all(group['status'] == 'PASS' for group in groups.values())
    assert case == before


@pytest.mark.parametrize('changed_content', [False, True])
def test_duplicate_raw_locator_is_known_lineage_failure_not_dictionary_last_wins(candidate, cases, changed_content):
    case = copy.deepcopy(cases['e19'])
    source = case['retained_evidence']['retained_sources'][0]
    duplicate = copy.deepcopy(source['rows'][1])
    if changed_content:
        duplicate['values'][1] = 103
    # This is a negative under the ORIGINAL frozen selection, not a fresh capture.
    source['rows'].append(duplicate)
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert _check(report, 'IDENTITY_BINDING_MISMATCH')['status'] == 'FAIL'
    assert report['eligibility']['status'] == 'FAIL'
    assert report['quality_result']['status'] == 'FAIL'
    assert report['quality_result']['accepted_rows'] is None
    assert case == before


def test_candidate_cannot_repeat_one_clean_raw_key_under_a_trusted_selection(candidate, cases):
    case = _one_identity(cases, selected=[0, 0])
    before = copy.deepcopy(case)
    frame = _frame(case)
    assert len(frame) == 2 and frame.iloc[0].tolist() == frame.iloc[1].tolist()
    report = _assess(candidate, case)
    assert report['raw_assessment']['rows_checked'] == 1
    assert report['raw_assessment']['status'] == 'PASS'
    assert report['raw_assessment']['quarantined_rows'] == 0
    assert report['raw_assessment']['groups'][0]['classification'] == 'UNIQUE'
    assert report['quality_result']['rows_checked'] == 2
    assert report['quality_result']['status'] == 'FAIL'
    assert report['eligibility']['status'] == 'FAIL'
    assert len(report['eligibility']['groups']) == 1
    assert report['eligibility']['groups'][0]['candidate_ordinals'] == []
    assert 'CANDIDATE_KEY_DUPLICATE' in report['eligibility']['groups'][0]['reasons']
    assert case == before


@pytest.mark.parametrize('malformed', ['wrong_key', 'foreign_snapshot', 'same_old_new', 'cross_identity'])
def test_malformed_independent_revision_cannot_authorize_a_winner(candidate, cases, malformed):
    case = _one_identity(cases, [_changed_row(cases, {}), _changed_row(cases, {'close': 102})])
    source = case['retained_evidence']['retained_sources'][0]
    other_source = None
    if malformed == 'cross_identity':
        other_record = copy.deepcopy(cases['e19']['metadata']['identity_records'][1])
        other_source = copy.deepcopy(cases['e19']['retained_evidence']['retained_sources'][1])
        other_row = copy.deepcopy(other_source['rows'][0])
        other_row['values'] = copy.deepcopy(source['rows'][0]['values'])
        other_row['raw_key']['begin'] = other_row['values'][6]
        other_source['rows'] = [other_row]
        case['metadata']['identity_records'].append(other_record)
        case['metadata']['counts'].update(instruments=2, pages=2)
        case['retained_evidence']['retained_sources'].append(other_source)
        case = _bind_fresh_case(case, [[source['identity_record_id'], source['provenance_ref'], 0],
            [source['identity_record_id'], source['provenance_ref'], 1],
            [other_source['identity_record_id'], other_source['provenance_ref'], other_row['raw_row_ordinal']]])
    revision = {'snapshot_id': case['metadata']['snapshot_id'],
        'raw_key': copy.deepcopy(source['rows'][0]['raw_key']),
        'old_locator': [source['identity_record_id'], source['provenance_ref'], 0],
        'new_locator': [source['identity_record_id'], source['provenance_ref'], 1],
        'evidence_ref': 'B2/malformed-independent-revision'}
    if malformed == 'wrong_key':
        revision['raw_key']['begin'] = '2024-01-02 10:00:00'
    elif malformed == 'foreign_snapshot':
        revision['snapshot_id'] = 'B2_FOREIGN_SNAPSHOT'
    elif malformed == 'same_old_new':
        revision['new_locator'] = copy.deepcopy(revision['old_locator'])
    else:
        revision['new_locator'] = [other_source['identity_record_id'], other_source['provenance_ref'],
                                   other_source['rows'][0]['raw_row_ordinal']]
    case['retained_evidence']['revision_evidence'] = [revision]
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert len(report['raw_assessment']['groups']) == (2 if malformed == 'cross_identity' else 1)
    group = next(group for group in report['raw_assessment']['groups']
                 if group['raw_key']['raw_instrument_id'] == source['rows'][0]['raw_key']['raw_instrument_id'])
    assert group['classification'] == 'SOURCE_KEY_CONFLICT'
    assert 'REVISION_NOT_PROVEN' in group['reasons']
    assert report['raw_assessment']['quarantined_rows'] == 2
    assert report['eligibility']['status'] == 'FAIL'
    blocked = next(group for group in report['eligibility']['groups']
                   if group['identity_record_id'] == source['identity_record_id'])
    assert blocked['candidate_ordinals'] == []
    assert case == before


@pytest.mark.parametrize('reverse', [False, True])
def test_two_row_revision_link_does_not_cover_a_third_unproven_variant(candidate, cases, reverse):
    rows = [_changed_row(cases, {}), _changed_row(cases, {'close': 102}),
            _changed_row(cases, {'close': 100})]
    case = _one_identity(cases, list(reversed(rows)) if reverse else rows)
    source = case['retained_evidence']['retained_sources'][0]
    case['retained_evidence']['revision_evidence'] = [{
        'snapshot_id': case['metadata']['snapshot_id'],
        'raw_key': copy.deepcopy(source['rows'][0]['raw_key']),
        'old_locator': [source['identity_record_id'], source['provenance_ref'], 2 if reverse else 0],
        'new_locator': [source['identity_record_id'], source['provenance_ref'], 1],
        'evidence_ref': 'B2/only-two-of-three-variants-have-a-revision-link'}]
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert report['raw_assessment']['status'] == 'FAIL'
    assert report['raw_assessment']['quarantined_rows'] == 3
    group = report['raw_assessment']['groups'][0]
    assert group['variant_count'] == 3
    assert group['classification'] == 'SOURCE_KEY_CONFLICT'
    assert 'REVISION_NOT_PROVEN' in group['reasons']
    assert report['eligibility']['status'] == 'FAIL'
    assert report['eligibility']['groups'][0]['candidate_ordinals'] == []
    assert case == before


def test_unsafe_provenance_ref_is_not_verified_even_when_expected_authority_repeats_it(candidate, cases):
    case = _one_identity(cases)
    source = case['retained_evidence']['retained_sources'][0]
    source['provenance_ref'] = 'a/../b'
    record = case['metadata']['identity_records'][0]
    record['provenance'][0]['evidence_ref'] = source['provenance_ref']
    case = _bind_fresh_case(case, [[record['record_id'], source['provenance_ref'], 0]])
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert report['eligibility']['status'] in {'FAIL', 'NOT_VERIFIED'}
    assert report['eligibility']['status'] != 'PASS'
    assert report['quality_result']['status'] != 'PASS'
    assert report['eligibility']['groups'][0]['candidate_ordinals'] == []
    assert case == before


@pytest.mark.parametrize('mutation', ['mixed_interval', 'board_unknown'])
def test_sort_cannot_resolve_interval_or_board_identity(candidate, cases, mutation):
    case = copy.deepcopy(cases['e19'])
    if mutation == 'mixed_interval':
        case['metadata']['identity_records'][1]['interval'] = 60
    else:
        identity = case['metadata']['identity_records'][0]['identity']
        identity['source_scope'] = 'BOARD'
        identity['board'] = None
    report = _assess(candidate, case)
    assert report['eligibility']['status'] in {'FAIL', 'NOT_VERIFIED'}
    assert report['eligibility']['status'] != 'PASS'
    assert report['eligibility']['production_admission'] == 'NOT_ENABLED'


@pytest.mark.parametrize('membership', ['missing', 'missing_expected_key', 'known_missing_received'])
def test_coverage_is_independent_of_row_acceptance(candidate, cases, membership):
    case = copy.deepcopy(cases['e19'])
    if membership == 'missing':
        case['retained_evidence'].pop('source_membership')
        expected = 'NOT_VERIFIED'
    elif membership == 'missing_expected_key':
        case['retained_evidence']['source_membership'][0]['expected_raw_keys'].pop()
        expected = 'INCOMPLETE'
    else:
        key = copy.deepcopy(case['retained_evidence']['source_membership'][0]['expected_raw_keys'][0])
        key['begin'] = '2024-01-20 10:00:00'
        case['retained_evidence']['source_membership'][0]['expected_raw_keys'].append(key)
        case['metadata']['identity_records'][0]['coverage_evidence']['expected_rows'] = 3
        expected = 'INCOMPLETE'
    report = _assess(candidate, case)
    assert report['coverage_assessment']['status'] == expected
    assert case['metadata']['coverage_status'] == 'COMPLETE'


def _wilder(closes, period=14):
    values = np.asarray(closes, dtype=float)
    expected = np.full(len(values), np.nan)
    if len(values) <= period:
        return expected
    changes = np.diff(values)
    gain = sum(max(change, 0) for change in changes[:period]) / period
    loss = sum(max(-change, 0) for change in changes[:period]) / period
    expected[period] = 100 * gain / (gain + loss) if gain + loss else 0
    for position in range(period + 1, len(values)):
        gain = (gain * (period - 1) + max(changes[position - 1], 0)) / period
        loss = (loss * (period - 1) + max(-changes[position - 1], 0)) / period
        expected[position] = 100 * gain / (gain + loss) if gain + loss else 0
    return expected


def test_frozen_permutations_produce_same_keyed_sequences_and_independent_wilder(candidate, cases):
    keyed_sequences = []
    for frozen in cases['permutation_snapshots']:
        case = copy.deepcopy(frozen)
        frame = _frame(case)
        report = _assess(candidate, case)
        assert report['eligibility']['status'] == 'PASS'
        assert report['coverage_assessment']['status'] == 'COMPLETE'
        expected = {group['identity_record_id']: group for group in frozen['expected_groups']}
        bindings = {entry['ordinal']: entry for entry in case['metadata']['row_bindings']['entries']}
        retained = {(source['identity_record_id'], source['provenance_ref'], row['raw_row_ordinal']): row
            for source in case['retained_evidence']['retained_sources'] for row in source['rows']}
        sequences = {}
        for group in report['eligibility']['groups']:
            reference = expected[group['identity_record_id']]
            assert group['candidate_ordinals'] == reference['candidate_ordinals']
            closes = [frame.iloc[ordinal]['close'] for ordinal in group['candidate_ordinals']]
            assert closes == reference['closes']
            native = talib.RSI(np.asarray(closes, dtype=float), timeperiod=14)
            independent = _wilder(closes)
            assert native.shape == independent.shape
            np.testing.assert_array_equal(np.isnan(native), np.isnan(independent))
            np.testing.assert_allclose(native[14:], independent[14:], rtol=1e-12, atol=1e-12)
            keyed_raw_sequence = []
            for ordinal in group['candidate_ordinals']:
                binding = bindings[ordinal]
                source = binding['source_row_ref']
                row = retained[(binding['identity_record_id'], source['provenance_ref'], source['raw_row_ordinal'])]
                raw_values = frame.iloc[ordinal][RAW_COLUMNS].tolist()
                assert raw_values == row['values']
                assert [type(value) for value in raw_values] == [type(value) for value in row['values']]
                keyed_raw_sequence.append((tuple(_key_array(source['raw_key'])), tuple(raw_values)))
            sequences[group['identity_record_id']] = keyed_raw_sequence
        keyed_sequences.append(sequences)
    assert keyed_sequences[0] == keyed_sequences[1]


def test_exact_owner_policy_case_has_eighteen_original_rows_and_sixteen_continued_bars(candidate, cases):
    case = copy.deepcopy(cases['owner_policy_snapshot'])
    before = copy.deepcopy(case)
    frame = _frame(case)
    source = case['retained_evidence']['retained_sources'][0]
    assert len(frame) == len(source['rows']) == 18
    assert frame['close'].tolist() == list(range(100, 115)) + [115, 180, 110]
    assert _key_array(source['rows'][15]['raw_key']) == _key_array(source['rows'][16]['raw_key'])
    report = _assess(candidate, case)
    assert report['raw_assessment']['rows_checked'] == 18
    assert report['raw_assessment']['quarantined_rows'] == 2
    findings = report['raw_assessment']['findings']
    assert len(findings) == 18
    assert len({tuple(item['locator']) for item in findings}) == 18
    rejected = [item for item in findings if item['status'] == 'FAIL']
    assert {item['locator'][2] for item in rejected} == {15, 16}
    assert all('SOURCE_KEY_CONFLICT' in item['reasons'] for item in rejected)
    assert sum(item['status'] == 'PASS' for item in findings) == 16
    assert report['quality_result']['rows_checked'] == 18
    assert report['quality_result']['quarantined_rows'] == 2
    assert report['quality_result']['accepted_rows'] == 16
    assert report['quality_result']['status'] == 'FAIL'
    assert report['eligibility']['status'] == 'PASS'
    assert report['eligibility']['policy'] == 'CONTINUE_ACROSS_QUARANTINE'
    assert report['coverage_assessment']['status'] == 'COMPLETE'
    assert len(report['eligibility']['groups']) == 1
    group = report['eligibility']['groups'][0]
    assert group['candidate_ordinals'] == list(range(15)) + [17]
    closes = [frame.iloc[ordinal]['close'] for ordinal in group['candidate_ordinals']]
    assert closes == list(range(100, 115)) + [110]
    native = talib.RSI(np.asarray(closes, dtype=float), timeperiod=14)
    independent = _wilder(closes)
    assert native.shape == independent.shape == (16,)
    np.testing.assert_array_equal(np.isnan(native), [True] * 14 + [False, False])
    np.testing.assert_array_equal(np.isnan(independent), [True] * 14 + [False, False])
    np.testing.assert_allclose(native[14:], [100.0, 76.47058823529412], rtol=1e-12, atol=1e-12)
    np.testing.assert_allclose(independent[14:], [100.0, 76.47058823529412], rtol=1e-12, atol=1e-12)
    assert case == before


def test_quarantine_keeps_original_bindings_and_continues_without_reset(candidate, cases):
    frozen = copy.deepcopy(cases['permutation_snapshots'][0])
    source = frozen['retained_evidence']['retained_sources'][0]
    source['rows'][15]['values'][0] = 0
    source['rows'][15]['values'][1] = 0
    selected = [[entry['identity_record_id'], entry['source_row_ref']['provenance_ref'],
                 entry['source_row_ref']['raw_row_ordinal']]
                for entry in frozen['metadata']['row_bindings']['entries']]
    case = _bind_fresh_case(frozen, selected)
    before = copy.deepcopy(case)
    frame = _frame(case)
    report = _assess(candidate, case)
    assert report['raw_assessment']['quarantined_rows'] == 1
    assert report['quality_result']['quarantined_rows'] == 1
    assert report['quality_result']['accepted_rows'] == 39
    assert report['quality_result']['status'] == 'FAIL'
    assert report['eligibility']['status'] == 'PASS'
    affected = next(group for group in report['eligibility']['groups']
                    if group['identity_record_id'] == source['identity_record_id'])
    assert affected['status'] == 'PASS'
    assert len(affected['candidate_ordinals']) == 19
    closes = [frame.iloc[ordinal]['close'] for ordinal in affected['candidate_ordinals']]
    expected = [value for ordinal, value in enumerate(before['expected_groups'][0]['closes']) if ordinal != 15]
    assert closes == expected
    native = talib.RSI(np.asarray(closes, dtype=float), timeperiod=14)
    independent = _wilder(closes)
    np.testing.assert_array_equal(np.isnan(native), np.isnan(independent))
    np.testing.assert_allclose(native[14:], independent[14:], rtol=1e-12, atol=1e-12)
    assert np.isfinite(native[-1])
    assert case == before
    # Deleting the bad row's ORIGINAL binding is still a lineage failure.
    bad_ordinal = next(entry['ordinal'] for entry in case['metadata']['row_bindings']['entries']
                       if entry['identity_record_id'] == source['identity_record_id']
                       and entry['source_row_ref']['raw_row_ordinal'] == 15)
    case['metadata']['row_bindings']['entries'] = [entry for entry in case['metadata']['row_bindings']['entries']
                                                  if entry['ordinal'] != bad_ordinal]
    broken = _assess(candidate, case)
    assert _check(broken, 'IDENTITY_BINDING_MISMATCH')['status'] == 'FAIL'


def test_ordinary_nontrading_gap_does_not_split_the_group(candidate, cases):
    frozen = cases['permutation_snapshots'][0]
    rows = copy.deepcopy(frozen['retained_evidence']['retained_sources'][0]['rows'])
    for ordinal, row in enumerate(rows):
        year = 2024 if ordinal < 10 else 2025
        row['values'][6] = '{}-01-{:02d} 10:00:00'.format(year, ordinal + 1)
        row['values'][7] = '{}-01-{:02d} 18:45:00'.format(year, ordinal + 1)
    case = _one_identity(cases, rows)
    report = _assess(candidate, case)
    assert report['raw_assessment']['status'] == 'PASS'
    assert report['eligibility']['status'] == 'PASS'
    assert len(report['eligibility']['groups']) == 1
    assert report['eligibility']['groups'][0]['candidate_ordinals'] == list(range(20))


def test_inputs_and_fixture_bytes_are_unchanged_without_logging(candidate, cases, capsys):
    path = FIXTURES / 'cases.json'
    raw_before = path.read_bytes()
    case = copy.deepcopy(cases['e19'])
    before = copy.deepcopy(case)
    frame = _frame(case)
    frame.index = pd.Index([91, 17, 44], name='source_position')
    original = frame.copy(deep=True)
    report = candidate.assess_candles(frame, spec=case['spec'], metadata=case['metadata'],
                                    retained_evidence=case['retained_evidence'])
    pd.testing.assert_frame_equal(frame, original, check_exact=True)
    assert case == before
    assert path.read_bytes() == raw_before
    assert report['diagnostics']['status'] == 'NOT_CONFIGURED'
    captured = capsys.readouterr()
    assert captured.out == captured.err == ''


def _json_pointer(value, pointer):
    for token in pointer.split('/')[1:]:
        token = token.replace('~1', '/').replace('~0', '~')
        value = value[int(token)] if isinstance(value, list) else value[token]
    return value


@pytest.fixture
def archive():
    return json.loads((FIXTURES / 'archived_observations.json').read_text(encoding='utf-8'))


def _archive_case(cases, archive, observations, *, ticker=None, known=True):
    rows = []
    for observation in observations:
        row = _changed_row(cases, {})
        row['values'] = copy.deepcopy(observation['values'])
        rows.append(row)
    case = _one_identity(cases, rows, ticker=ticker or observations[0]['ticker'])
    if known:
        signatures = []
        for signature in archive['known_abio_signatures']:
            key = copy.deepcopy(signature['identity'])
            key.update(interval=signature['interval'], begin=signature['values'][6])
            signatures.append({'raw_key': key, 'columns': copy.deepcopy(signature['columns']),
                               'values': copy.deepcopy(signature['values']),
                               'evidence_ref': signature['evidence_ref'],
                               'reason': 'CONFIRMED_ABIO_MULTIYEAR_OBSERVATION'})
        case['retained_evidence']['known_observations'] = signatures
    return case


def test_archived_parsed_fragments_bind_all_thirteen_rows_without_raw_authenticity_claim(archive):
    # Hash the declared outer text capture in LF form on both Windows and Linux.
    # Embedded fragment text is JSON-escaped here and retains its own exact bytes.
    fixture_text = (FIXTURES / 'archived_observations.json').read_text(encoding='utf-8')
    manifest_text = (FIXTURES / 'evidence_manifest.json').read_text(encoding='utf-8')
    fixture_bytes = fixture_text.encode('utf-8')
    manifest_bytes = manifest_text.encode('utf-8')
    for text in (fixture_text, manifest_text):
        with StringIO(text.replace('\n', '\r\n'), newline=None) as crlf_capture:
            assert crlf_capture.read().encode('utf-8') == text.encode('utf-8')
    assert hashlib.sha256(fixture_bytes).hexdigest() == '95e3ecda17bd7aa8b55f8128f24a99c2f3df600109b7b5e8e05392baaee8cf00'
    assert hashlib.sha256(manifest_bytes).hexdigest() == 'e3e335e74db714fb3b1c312201fd1d526c0d404e6d52417e79ac2d661589606e'
    manifest = json.loads(manifest_bytes)
    assert manifest['reconstructed_fixture']['sha256_basis'] == 'UTF8_TEXT_LF'
    assert manifest['reconstructed_fixture']['sha256'] == hashlib.sha256(fixture_bytes).hexdigest()
    assert manifest['reconstructed_fixture']['bytes'] == len(fixture_bytes)
    ledger = manifest['coverage_ledger']
    assert ledger['expected_original_ohlc_observations'] == 79
    assert ledger['reconstructed_parsed_public_observations'] == 10
    assert ledger['unavailable_unenumerated_original_observations'] == 69
    assert ledger['original_ohlc_observations_with_raw_bytes_unavailable'] == 79
    assert ledger['original_ohlc_observations_with_raw_bytes_available'] == 0
    assert ledger['original_unavailable_observation_ids'] is None
    assert ledger['additional_assb_parsed_corroborations'] == 1
    assert ledger['additional_abio_parsed_variants'] == 2
    assert ledger['historical_source_membership_verification'] == 'NOT_VERIFIED'
    observations = archive['public_export_samples'] + archive['source_corroborations'] + archive['abio_variants']
    assert len(observations) == len(manifest['provenance']) == 13
    assert archive['source_verification'] == 'NOT_VERIFIED'
    for observation in observations:
        provenance = manifest['provenance'][observation['provenance_id']]
        fragment = manifest['fragments'][provenance['fragment_id']]
        text = fragment['utf8_text']
        assert hashlib.sha256(text.encode('utf-8')).hexdigest() == fragment['sha256']
        selected = _json_pointer(json.loads(text), provenance['fragment_json_pointer'])
        values = [selected[column] for column in RAW_COLUMNS] if isinstance(selected, dict) else selected
        assert values == observation['values']
        assert [type(value) for value in values] == [type(value) for value in observation['values']]
        assert provenance['original_raw_bytes_sha256'] is None
        assert provenance['source_acquired_at'] is None
        assert provenance['source_verification'] == 'NOT_VERIFIED'
    # These are different retained representations of the same ASSB observation.
    assert archive['public_export_samples'][0]['values'][0] == 0.0
    assert type(archive['public_export_samples'][0]['values'][0]) is float
    assert type(archive['source_corroborations'][0]['values'][0]) is int
    assert archive['source_corroborations'][0]['corroborates'] == archive['public_export_samples'][0]['observation_id']


@pytest.mark.parametrize('observation_index', range(10))
def test_all_ten_reconstructed_public_observations_remain_unrepaired(candidate, cases, archive, observation_index):
    observation = archive['public_export_samples'][observation_index]
    case = _archive_case(cases, archive, [observation])
    before = copy.deepcopy(case)
    fixture_before = (FIXTURES / 'archived_observations.json').read_bytes()
    report = _assess(candidate, case)
    assert report['raw_assessment']['status'] == 'FAIL'
    assert report['raw_assessment']['rows_checked'] == 1
    assert report['raw_assessment']['quarantined_rows'] == 1
    assert 'OHLC_BOUNDS' in _reasons(report)
    assert report['raw_assessment']['source_verification'] == 'NOT_VERIFIED'
    assert case == before
    assert (FIXTURES / 'archived_observations.json').read_bytes() == fixture_before


def test_assb_source_corroboration_preserves_source_numbers(candidate, cases, archive):
    observation = archive['source_corroborations'][0]
    case = _archive_case(cases, archive, [observation])
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert report['raw_assessment']['quarantined_rows'] == 1
    assert 'OHLC_BOUNDS' in _reasons(report)
    assert case == before
    values = case['retained_evidence']['retained_sources'][0]['rows'][0]['values']
    assert values == [0, 0, 0.3, 0.3, 300, 1000, '2013-12-05 00:00:00', '2013-12-05 23:59:59']
    assert [type(value) for value in values[:6]] == [int, int, float, float, int, int]


@pytest.mark.parametrize('variant', [0, 1])
def test_each_known_abio_variant_is_quarantined_without_a_duplicate_partner(candidate, cases, archive, variant):
    case = _archive_case(cases, archive, [archive['abio_variants'][variant]])
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    assert report['raw_assessment']['rows_checked'] == 1
    assert report['raw_assessment']['quarantined_rows'] == 1
    assert 'CONFIRMED_ABIO_MULTIYEAR_OBSERVATION' in _reasons(report)
    assert report['raw_assessment']['groups'][0]['classification'] == 'UNIQUE'
    assert case == before


def test_both_abio_variants_keep_same_key_and_individual_quarantine_reasons(candidate, cases, archive):
    case = _archive_case(cases, archive, archive['abio_variants'])
    before = copy.deepcopy(case)
    report = _assess(candidate, case)
    raw = report['raw_assessment']
    assert raw['rows_checked'] == raw['quarantined_rows'] == 2
    assert len(raw['groups']) == 1
    assert raw['groups'][0]['classification'] == 'SOURCE_KEY_CONFLICT'
    assert 'OHLC_VARIANT' in raw['groups'][0]['reasons']
    assert 'RAW_VALUE_VARIANT' in raw['groups'][0]['reasons']
    assert len(raw['findings']) == 2
    assert all('CONFIRMED_ABIO_MULTIYEAR_OBSERVATION' in item['reasons'] for item in raw['findings'])
    assert len({tuple(item['locator']) for item in raw['findings']}) == 2
    assert case == before


@pytest.mark.parametrize('changed', ['no_authority', 'different_scope', 'different_interval', 'different_value'])
def test_abio_rule_requires_the_complete_authoritative_observation(candidate, cases, archive, changed):
    case = _archive_case(cases, archive, [archive['abio_variants'][0]])
    if changed == 'no_authority':
        case['retained_evidence'].pop('known_observations')
    elif changed == 'different_scope':
        for signature in case['retained_evidence']['known_observations']:
            signature['raw_key']['raw_instrument_id'] = 'ABIO_DIFFERENT_RAW_ID'
    elif changed == 'different_interval':
        for signature in case['retained_evidence']['known_observations']:
            signature['raw_key']['interval'] = 60
    else:
        source = case['retained_evidence']['retained_sources'][0]
        source['rows'][0]['values'][5] += 1
    report = _assess(candidate, case)
    assert 'CONFIRMED_ABIO_MULTIYEAR_OBSERVATION' not in _reasons(report)
    assert report['raw_assessment']['status'] == 'PASS'


def _logging_context(monkeypatch, root, run_id):
    import run_logging
    context = {'config': run_logging.validate_logging_config(), '_checkout': root,
               '_secrets': (CANARY,), 'run_id': run_id,
               'producer_sha': '59f2f57c8972fd4dd40f1843a24503aa906f6300', 'worker_id': None}
    events = []
    def captured_emit(context, level, event, message, fields):
        events.append(run_logging._event(context, level, event, message, fields))
        return {'accepted': True, 'confirmed': level == 'ERROR'}
    monkeypatch.setattr(run_logging, 'emit_event', captured_emit)
    return context, events


def _semantics(report):
    return {key: value for key, value in report.items() if key != 'diagnostics'}


def test_logging_has_distinct_raw_candidate_counts_without_creating_sinks(candidate, cases, monkeypatch, tmp_path, capsys):
    case = copy.deepcopy(cases['e19'])
    context, events = _logging_context(monkeypatch, tmp_path, case['metadata']['run_id'])
    report = _assess(candidate, case, logging_context=context)
    assert report['diagnostics']['status'] == 'RECORDED'
    assert [event['event'] for event in events] == ['b2_assessment_started', 'b2_raw_assessed', 'b2_candidate_assessed']
    assert all(event['stage'] == 'tech' and event['interval'] == 24 for event in events)
    assert all(event['run_id'] == case['metadata']['run_id'] for event in events)
    assert all(event['snapshot_id'] == case['metadata']['snapshot_id'] for event in events)
    assert all(event['dataset_id'] == case['metadata']['dataset_id'] for event in events)
    assert all(event['duration_ms'] >= 0 for event in events)
    raw, selected = events[1:]
    assert raw['counts']['rows_received'] == 4
    assert raw['counts']['rows_accepted'] is None
    assert raw['counts']['rows_quarantined'] == 0
    assert raw['counts']['pages'] is None
    assert 'rows_accepted' in raw['counts']['null_reasons']
    assert 'pages' in raw['counts']['null_reasons']
    assert selected['counts']['rows_received'] == 3
    assert selected['counts']['rows_accepted'] == 3
    assert selected['counts']['rows_quarantined'] == 0
    assert selected['counts']['pages'] is None
    assert raw['fields'] == selected['fields'] == {}
    assert list(tmp_path.iterdir()) == []
    assert capsys.readouterr().out == ''


def test_failure_logging_is_quality_error_and_redacts_untrusted_context(candidate, cases, monkeypatch, tmp_path, capsys):
    case = _one_identity(cases, [_changed_row(cases, {'open': 0, 'close': 0})])
    case['metadata']['dataset_id'] = CANARY
    context, events = _logging_context(monkeypatch, tmp_path, case['metadata']['run_id'])
    report = _assess(candidate, case, logging_context=context)
    assert report['quality_result']['status'] == 'FAIL'
    assert report['diagnostics']['status'] == 'RECORDED'
    errors = [event for event in events if event['level'] == 'ERROR']
    assert errors
    assert all(event['error']['category'] == 'QUALITY' for event in errors)
    assert all(event['outcome'] == 'FAIL' for event in errors)
    assert all(event['error']['final'] is True and event['error']['retryable'] is False for event in errors)
    captured = capsys.readouterr()
    assert CANARY not in json.dumps(events) + captured.out + captured.err
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize('failure', ['exception', 'unconfirmed'])
def test_diagnostic_failure_does_not_change_quality_or_eligibility(candidate, cases, monkeypatch, tmp_path, failure):
    import run_logging
    case = _one_identity(cases, [_changed_row(cases, {'open': 0, 'close': 0})])
    baseline = _assess(candidate, case)
    context, events = _logging_context(monkeypatch, tmp_path, case['metadata']['run_id'])
    def failed_emit(context, level, event, message, fields):
        if failure == 'exception':
            raise RuntimeError('B2_DIAGNOSTIC_' + CANARY)
        return {'accepted': True, 'confirmed': False}
    monkeypatch.setattr(run_logging, 'emit_event', failed_emit)
    report = _assess(candidate, case, logging_context=context)
    assert _semantics(report) == _semantics(baseline)
    assert report['diagnostics']['status'] == 'FAILED'
    assert list(tmp_path.iterdir()) == []


def test_unexpected_primary_error_survives_diagnostic_failure(candidate, cases, monkeypatch, tmp_path, capsys):
    import run_logging
    case = copy.deepcopy(cases['e19'])
    frame = _frame(case)
    context, events = _logging_context(monkeypatch, tmp_path, case['metadata']['run_id'])
    primary = RuntimeError('B2_PRIMARY_' + CANARY)
    def broken_columns(frame):
        raise primary
    def failed_emit(*args, **kwargs):
        raise RuntimeError('B2_DIAGNOSTIC_' + CANARY)
    monkeypatch.setattr(pd.DataFrame, 'columns', property(broken_columns))
    monkeypatch.setattr(run_logging, 'emit_event', failed_emit)
    with pytest.raises(RuntimeError) as caught:
        candidate.assess_candles(frame, spec=case['spec'], metadata=case['metadata'],
                                retained_evidence=case['retained_evidence'], logging_context=context)
    assert caught.value is primary
    captured = capsys.readouterr()
    assert CANARY not in captured.out + captured.err
