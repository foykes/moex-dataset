"""Synthetic caller-proof fixtures; no official source or release acceptance."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / 'tests/fixtures/c2_quality/runtime_policy_cases_v1.json'
CASES = json.loads(FIXTURE.read_text(encoding='utf-8'))
SCHEMA = (ROOT / 'contracts/c2_runtime_v1.json').read_bytes()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def prepared(case):
    # Literal retained basis exists before any candidate edits. Tests never build
    # the expected selection from the input being assessed or Q's output.
    data = copy.deepcopy(CASES['base'])
    for edit in case['edits']:
        parent = data
        for part in edit['path'][:-1]:
            parent = parent[part]
        if edit['op'] == 'remove':
            del parent[edit['path'][-1]]
        else:
            assert edit['op'] == 'set'
            parent[edit['path'][-1]] = copy.deepcopy(edit['value'])
    return data


@pytest.fixture
def quality(monkeypatch):
    # Только два pure модуля, никаких application imports или test-only #109.
    for name in ('dividend_runtime_contract', 'dividend_quality'):
        specification = importlib.util.spec_from_file_location(name, ROOT / (name + '.py'))
        module = importlib.util.module_from_spec(specification)
        monkeypatch.setitem(sys.modules, name, module)
        specification.loader.exec_module(module)
    return module


def test_fixture_identity_and_independently_retained_selection():
    assert CASES['origin'] == 'SYNTHETIC_ONLY'
    assert CASES['expected_authorship'] == 'MANUAL_BEFORE_IMPLEMENTATION_NO_OBSERVED_GOLDEN'
    assert CASES['schema_sha256'] == 'a0daced78e661713d75a5f768df5a354f5caf3c82a9f0a76a7ca53f10ad19619'
    assert hashlib.sha256(encoded(json.loads(SCHEMA))).hexdigest() == CASES['schema_sha256']
    assert len(CASES['check_ids']) == len(set(CASES['check_ids'])) == 9
    assert len(CASES['cases']) == len({case['case_id'] for case in CASES['cases']})
    basis = CASES['base']['basis']
    frame = CASES['base']['message']['canonical']
    assert basis['retained_rows'] == frame['rows']
    assert basis['retained_bindings'] == frame['bindings']
    assert basis['retained_rows'] is not frame['rows']
    assert frame['rows'][0] == frame['rows'][2]  # intentional full repeat
    assert frame['row_count'] == 3


@pytest.mark.parametrize('case', CASES['cases'], ids=lambda case: case['case_id'])
def test_runtime_quality_fixed_independent_oracles(quality, case):
    data = prepared(case)
    before = copy.deepcopy(data)
    raw = encoded(data['message'])
    expected = case['expected']
    if expected['error'] is not None:
        with pytest.raises(ValueError) as caught:
            quality.assess(raw, schema_bytes=SCHEMA, trusted=data['trusted'], basis=data['basis'])
        assert str(caught.value) == expected['error']
        assert caught.value.__cause__ is None
        assert caught.value.__suppress_context__
        assert 'PRIVATE_CANARY_7281' not in str(caught.value)
    else:
        actual = quality.assess(raw, schema_bytes=SCHEMA, trusted=data['trusted'], basis=data['basis'])
        assert actual['assessment']['status'] == expected['status']
        assert {check['check_id']: check['status'] for check in actual['assessment']['checks']} == expected['checks']
        assert [check['check_id'] for check in actual['assessment']['checks']] == CASES['check_ids']
        assert actual['counts'] == expected['counts']
        assert actual['local_findings'] == expected['local_findings']
        assert actual['context'] == data['message']['context']
        assert actual['candidate_release_id'] == data['message']['candidate_release_id']
        assert actual['observed_assessment'] == data['message']['assessment']
        assert actual['u_schema_sha256'] == CASES['schema_sha256']
        assert actual['precision_status'] == 'NOT_VERIFIED'
        assert actual['blocking_check_ids'] == [key for key in CASES['check_ids'] if expected['checks'][key] != 'PASS']
        for check in actual['assessment']['checks']:
            assert set(check) == {'check_id', 'status', 'evidence_ref', 'reason'}
            assert (check['reason'] is None) == (check['status'] == 'PASS')
            if check['status'] == 'PASS':
                assert check['evidence_ref'] is not None
                assert check['evidence_ref'] in {e['evidence_id'] for e in data['trusted']['evidence']}
        assert not ({'decision', 'admission', 'ticket', 'release', 'publication', 'exit_status'} & actual.keys())
    assert data == before
    assert raw == encoded(data['message'])


@pytest.mark.parametrize('raw,expected', [
    (b'{"schema_id":1,"schema_id":2}', 'JSON_DUPLICATE_KEY'),
    (b'{"n":NaN}', 'JSON_CONSTANT'),
    (b'{"n":Infinity}', 'JSON_CONSTANT'),
    (b'{"n":1e309}', 'JSON_NONFINITE'),
    (b'{}trailing', 'JSON_SYNTAX'),
])
def test_q_reuses_u_strict_decoder_without_weakening_it(quality, raw, expected):
    data = copy.deepcopy(CASES['base'])
    with pytest.raises(ValueError, match='^C2_RUNTIME_CONTRACT_INVALID:' + expected + '$'):
        quality.assess(raw, schema_bytes=SCHEMA, trusted=data['trusted'], basis=data['basis'])


def test_exact_schema_change_cannot_be_self_certified(quality):
    data = copy.deepcopy(CASES['base'])
    schema = json.loads(SCHEMA)
    schema['description'] += ' drift'
    data['message']['schema_sha256'] = hashlib.sha256(encoded(schema)).hexdigest()
    with pytest.raises(ValueError, match=':SCHEMA_DIGEST$'):
        quality.assess(encoded(data['message']), schema_bytes=encoded(schema),
                       trusted=data['trusted'], basis=data['basis'])


def test_computed_observations_are_visible_when_anchor_unproved(quality):
    case = next(c for c in CASES['cases'] if c['case_id'] == 'missing-input-anchor')
    data = prepared(case)
    result = quality.assess(encoded(data['message']), schema_bytes=SCHEMA,
                            trusted=data['trusted'], basis=data['basis'])
    assert result['assessment']['status'] == 'NOT_VERIFIED'
    assert result['computed_checks']['LOCAL_PROFILE'] == 'PASS'
    assert result['computed_checks']['CANONICAL_CONTENT'] == 'PASS'
    assert result['computed_checks']['CANONICAL_BINDINGS'] == 'PASS'
    assert result['counts']['rows_checked'] == 3


def test_result_mutation_does_not_rewrite_callers_retained_basis(quality):
    data = copy.deepcopy(CASES['base'])
    before = copy.deepcopy(data)
    result = quality.assess(encoded(data['message']), schema_bytes=SCHEMA,
                            trusted=data['trusted'], basis=data['basis'])
    result['context']['run_id'] = 'MUTATED_RESULT'
    result['observed_assessment']['checks'].append({'unsafe': 'arbitrary'})
    assert data == before


@pytest.mark.parametrize('kind', ['context-object', 'candidate-object',
                                  'context-dict-subclass', 'context-text-subclass'])
def test_basis_rejects_nonprimitive_identity_before_comparison(quality, kind):
    data = copy.deepcopy(CASES['base'])
    compared = []
    class UnsafeIdentity:
        def __eq__(self, other):
            compared.append(True)
            return True
        def __repr__(self):
            return 'PRIVATE_CANARY_7281'
    class ContextDict(dict):
        pass
    class ContextText(str):
        pass
    if kind == 'context-object':
        data['basis']['context'] = UnsafeIdentity()
    elif kind == 'candidate-object':
        data['basis']['candidate_release_id'] = UnsafeIdentity()
    elif kind == 'context-dict-subclass':
        data['basis']['context'] = ContextDict(data['basis']['context'])
    else:
        data['basis']['context']['run_id'] = ContextText(data['basis']['context']['run_id'])
    with pytest.raises(quality.QualityInputError) as caught:
        quality.assess(encoded(data['message']), schema_bytes=SCHEMA,
                       trusted=data['trusted'], basis=data['basis'])
    assert str(caught.value) == 'C2_QUALITY_INPUT_INVALID:BASIS_SHAPE'
    assert caught.value.__cause__ is None and caught.value.__suppress_context__
    assert not compared


def test_assessor_has_no_implicit_io_clock_or_application_dependency(quality, monkeypatch):
    raw = encoded(CASES['base']['message'])
    def forbidden(*args, **kwargs):
        pytest.fail('Q attempted implicit I/O')
    monkeypatch.setattr('builtins.open', forbidden)
    result = quality.assess(raw, schema_bytes=SCHEMA, trusted=CASES['base']['trusted'], basis=CASES['base']['basis'])
    assert result['assessment']['status'] == 'PASS'
    assert not any(name in vars(quality) for name in (
        'requests', 'urllib', 'os', 'Path', 'subprocess', 'datetime', 'time',
        'dividends', 'dohodru_data', 'run_logging', 'release_manifest', 'issue_ticket'))


def test_historical_oracle_is_not_a_production_dependency():
    text = (ROOT / 'dividend_quality.py').read_text(encoding='utf-8')
    assert 'test_release_policy_contract' not in text
    assert 'tests.shared' not in text
