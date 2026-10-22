"""Independent synthetic E oracles; no live source or private storage."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
CASES = json.loads((ROOT / 'tests/fixtures/c2_quality/evidence_cases_v1.json').read_bytes())
SCHEMA = (ROOT / 'contracts/c2_runtime_v1.json').read_bytes()
PROFILE = (ROOT / 'settings/dividend_source_profile.json').read_bytes()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


@pytest.fixture
def evidence():
    # Только новый offline helper; application entrypoints и #109 не импортируются.
    specification = importlib.util.spec_from_file_location('c2_evidence_fixture', ROOT / 'dividend_evidence.py')
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def invoke(module, data):
    return module.verify_capture(data['raw'].encode('utf-8'), capture=data['capture'],
        retained=data['retained'], profile_bytes=canonical(data['profile']), schema_bytes=SCHEMA)


@pytest.mark.parametrize('case', CASES['cases'], ids=lambda case: case['case_id'])
def test_fixed_source_evidence_oracles(evidence, case):
    data = copy.deepcopy(case['input'])
    before = copy.deepcopy(data)
    expected = case['expected']
    if expected['error'] is not None:
        with pytest.raises(evidence.EvidenceError) as caught:
            invoke(evidence, data)
        assert str(caught.value) == 'C2_EVIDENCE_INVALID:' + expected['error']
    else:
        result = invoke(evidence, data)
        assert set(result) == {'u_schema_sha256', 'profile_sha256', 'context', 'candidate_release_id',
            'capture_id', 'source_binding', 'raw_integrity', 'selection', 'tokens',
            'origin_status', 'source_semantics_status', 'coverage_status', 'evidence_observations',
            'custody_status', 'release_admission'}
        assert result['u_schema_sha256'] == CASES['u_schema_sha256']
        assert result['profile_sha256'] == CASES['profile_sha256']
        assert result['context'] == data['capture']['context']
        assert result['candidate_release_id'] == data['capture']['candidate_release_id']
        assert result['source_binding'] == data['capture']['source_binding']
        assert result['raw_integrity'] == 'VERIFIED'
        assert result['custody_status'] == 'NOT_VERIFIED'
        assert result['release_admission'] == 'NOT_ASSESSED'
        selected = result['selection']
        for name in ('rows', 'bindings', 'content_sha256', 'bindings_sha256', 'row_count'):
            assert selected[name] == expected[name]
        assert selected['columns'] == ['ISIN', 'TRADE_CODE', 'dt', 'value', 'currency']
        assert selected['identity_origin'] == {'ISIN': 'CALLER_BINDING', 'TRADE_CODE': 'CALLER_BINDING'}
        assert selected['source_counts'] == [{'source_id': 'SYN_SOURCE', 'row_count': expected['row_count']}]
        assert result['tokens'] == expected['tokens']
        for token in result['tokens']:
            raw = data['raw'].encode('utf-8')
            assert raw[token['byte_start']:token['byte_end']].decode() == token['literal']
        for field in ('origin_status', 'source_semantics_status', 'coverage_status'):
            assert result[field] == expected[field]
        assert len(result['evidence_observations']) == 8
        # Returned mutable objects cannot replace the separately retained basis.
        selected['rows'].clear()
        result['source_binding']['source_id'] = 'ALTERED_RESULT_ONLY'
    assert data == before


def test_profile_and_fixture_identity(evidence):
    assert CASES['origin'] == 'SYNTHETIC_ONLY'
    assert len(CASES['cases']) == 56
    assert len({item['case_id'] for item in CASES['cases']}) == 56
    assert hashlib.sha256(canonical(json.loads(PROFILE))).hexdigest() == CASES['profile_sha256']
    assert json.loads(PROFILE)['source_fact_status'] == 'NOT_VERIFIED'
    assert evidence.U_SCHEMA_SHA256 == CASES['u_schema_sha256']


@pytest.mark.parametrize('location', ['capture', 'retained', 'context', 'candidate', 'attempt'])
def test_custom_objects_never_supply_identity(evidence, location):
    class Pretender:
        def __eq__(self, other):
            return True

        def __repr__(self):
            raise AssertionError('private repr must not execute')

    data = copy.deepcopy(CASES['cases'][0]['input'])
    if location in ('capture', 'retained'):
        data[location] = Pretender()
    elif location == 'context':
        data['capture']['context'] = Pretender()
    elif location == 'candidate':
        data['capture']['candidate_release_id'] = Pretender()
    else:
        data['retained']['capture']['attempt'] = Pretender()
    with pytest.raises(evidence.EvidenceError, match='^C2_EVIDENCE_INVALID:SHAPE$'):
        invoke(evidence, data)


@pytest.mark.parametrize('raw', [b'\xff', b'\xef\xbb\xbf{}', b'{"a":"' + bytes([92]) + b'ud800"}'])
def test_invalid_raw_utf8_and_surrogate_refused(evidence, raw):
    data = copy.deepcopy(CASES['cases'][0]['input'])
    for capture in (data['capture'], data['retained']['capture']):
        capture['source_binding']['raw_sha256'] = hashlib.sha256(raw).hexdigest()
        capture['source_binding']['raw_length'] = len(raw)
    with pytest.raises(evidence.EvidenceError, match='^C2_EVIDENCE_INVALID:RAW_JSON$'):
        evidence.verify_capture(raw, capture=data['capture'], retained=data['retained'],
            profile_bytes=PROFILE, schema_bytes=SCHEMA)


def test_profile_duplicate_keys_cannot_pin_themselves(evidence):
    data = copy.deepcopy(CASES['cases'][0]['input'])
    with pytest.raises(evidence.EvidenceError, match='^C2_EVIDENCE_INVALID:PROFILE$'):
        evidence.verify_capture(data['raw'].encode(), capture=data['capture'], retained=data['retained'],
            profile_bytes=b'{"profile_id":"A","profile_id":"B"}', schema_bytes=SCHEMA)


def test_actual_verifier_performs_no_io(evidence, monkeypatch):
    data = copy.deepcopy(CASES['cases'][0]['input'])
    def forbidden(*args, **kwargs):
        raise AssertionError('E attempted file I/O')
    monkeypatch.setattr('builtins.open', forbidden)
    monkeypatch.setattr(Path, 'open', forbidden)
    result = invoke(evidence, data)
    assert result['source_semantics_status'] == 'NOT_VERIFIED'


def test_all_failed_external_evidence_retains_its_status(evidence):
    case = next(item for item in CASES['cases'] if item['case_id'] == 'authority-conflicted')
    result = invoke(evidence, copy.deepcopy(case['input']))
    assert {record['observed_status'] for record in result['evidence_observations']} == {'CONFLICTED'}
    assert {record['effective_status'] for record in result['evidence_observations']} == {'NOT_VERIFIED'}


def numeric_capture(literal):
    data = copy.deepcopy(CASES['cases'][0]['input'])
    data['raw'] = ('{"dividends":{"columns":["registryclosedate","value","currencyid"],'
                   '"data":[["2111-01-01",' + literal + ',"RUB"]]}}')
    raw = data['raw'].encode('utf-8')
    for capture in (data['capture'], data['retained']['capture']):
        capture['source_binding']['raw_sha256'] = hashlib.sha256(raw).hexdigest()
        capture['source_binding']['raw_length'] = len(raw)
    return data


@pytest.mark.parametrize('literal', ['1e-1025', '1e1025', '0.' + '1' * 1024,
                                  '1e-5000', '1e99999999999999999999999999999'],
                         ids=['negative-exponent', 'positive-exponent', 'coefficient',
                              'integer-string-limit', 'oversized-exponent'])
def test_precision_capacity_precedes_decimal_or_power(evidence, monkeypatch, literal):
    # Даже RED не выполняет опасное измерение: sentinel прерывает старый код
    # ДО Decimal/Fraction. GREEN обязан отказать ещё до этого sentinel.
    def forbidden(*args, **kwargs):
        raise AssertionError('capacity was not checked before decimal construction')
    monkeypatch.setattr(evidence, 'Decimal', forbidden)
    with pytest.raises(evidence.EvidenceError, match='^C2_EVIDENCE_INVALID:PRECISION_CAPACITY$'):
        invoke(evidence, numeric_capture(literal))


@pytest.mark.parametrize('literal,reference,numerator,denominator', [
    ('1e-1024', '1E-1024', '-1', '1' + '0' * 1024),
    ('1e1024', '1E+1024', None, None),
    ('1' * 1024, '1' * 1024, '0', '1'),
])
def test_precision_capacity_boundary_keeps_exact_measurement(evidence, literal, reference, numerator, denominator):
    token = invoke(evidence, numeric_capture(literal))['tokens'][0]
    assert token['decimal_reference'] == reference
    if numerator is None:
        assert token['delta_exact'] is None
        assert token['delta_reason'] == 'NONFINITE'
    else:
        assert token['delta_exact'] == {'numerator': numerator, 'denominator': denominator}
        assert token['delta_reason'] is None


@pytest.mark.parametrize('operation,exception,code', [
    ('Decimal', ValueError, 'PRECISION_CAPACITY'),
    ('Fraction', OverflowError, 'PRECISION_CAPACITY'),
    ('_scalar_locations', RecursionError, 'RAW_JSON'),
])
def test_internal_measurement_failures_have_closed_safe_errors(evidence, monkeypatch, operation, exception, code):
    def rejected(*args, **kwargs):
        raise exception('PRIVATE_SOURCE_MUST_NOT_ESCAPE')
    monkeypatch.setattr(evidence, operation, rejected)
    with pytest.raises(evidence.EvidenceError) as caught:
        invoke(evidence, numeric_capture('0.1'))
    assert str(caught.value) == 'C2_EVIDENCE_INVALID:' + code
