"""Wire compatibility only; synthetic records confer no admission authority."""

import copy
import hashlib
import importlib.util
import json
import math
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / 'tests/fixtures/c2_quality/runtime_contract_v1.json'
SCHEMA = ROOT / 'contracts/c2_runtime_v1.json'
CASES = json.loads(FIXTURE.read_text(encoding='utf-8'))


@pytest.fixture
def codec():
    # Импортируется только новый чистый codec. Application и oracle #109 не нужны.
    spec = importlib.util.spec_from_file_location(
        'c2_runtime_contract_fixture', ROOT / 'dividend_runtime_contract.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def patched(value, edits):
    result = copy.deepcopy(value)
    for edit in edits:
        parent = result
        for part in edit['path'][:-1]:
            parent = parent[part]
        if edit['op'] == 'remove':
            del parent[edit['path'][-1]]
        else:
            assert edit['op'] == 'replace'
            parent[edit['path'][-1]] = copy.deepcopy(edit['value'])
    return result


def test_schema_identity_has_independent_frozen_digest(codec):
    raw = SCHEMA.read_bytes()
    schema = json.loads(raw)
    assert schema['$id'] == 'mds.c2.runtime/1'
    assert schema['properties']['schema_id']['const'] == 'mds.c2.runtime'
    assert schema['properties']['schema_version']['const'] == 1
    assert hashlib.sha256(canonical(schema)).hexdigest() == CASES['schema_sha256']
    assert codec.SCHEMA_SHA256 == CASES['schema_sha256']
    assert codec.schema_digest(raw) == CASES['schema_sha256']
    # Checkout CRLF и форматирование не создают новую schema identity.
    assert codec.schema_digest(json.dumps(schema, indent=4).encode()) == CASES['schema_sha256']


@pytest.mark.parametrize('field,value', [
    ('alias', 'SYN_RUN_1\n'),
    ('digest', 'a' * 64 + '\n'),
    ('utc', '2026-10-19T20:00:00Z\n'),
    ('integer', '123\n'),
])
def test_standard_schema_and_codec_refuse_terminal_linefeed(codec, field, value):
    # JSON Schema ищет pattern, а не вызывает fullmatch. Конец перед финальным
    # LF нельзя принять только потому, что procedural codec строже schema.
    message = copy.deepcopy(CASES['message'])
    if field == 'alias':
        message['context']['run_id'] = value
    elif field == 'digest':
        message['canonical']['content_sha256'] = value
    elif field == 'utc':
        message['context']['as_of'] = value
    else:
        message['canonical']['rows'][0][3] = {'kind': 'INT', 'value': value}
    schema_bytes = SCHEMA.read_bytes()
    schema = json.loads(schema_bytes)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    assert list(validator.iter_errors(CASES['message'])) == []
    assert list(validator.iter_errors(message)), field
    with pytest.raises(codec.ContractError, match=':SHAPE$'):
        codec.decode(canonical(message), schema_bytes=schema_bytes, trusted=CASES['trusted'])


def test_retained_synthetic_source_and_canonical_hashes_are_independent():
    message = CASES['message']
    source = message['source_bindings'][0]
    body = CASES['raw_utf8'].encode('utf-8')
    assert source['raw_length'] == len(body)
    assert source['raw_sha256'] == hashlib.sha256(body).hexdigest()
    frame = message['canonical']
    assert frame['content_sha256'] == hashlib.sha256(canonical([
        frame['columns'], frame['rows']])).hexdigest()
    assert frame['bindings_sha256'] == hashlib.sha256(canonical(frame['bindings'])).hexdigest()
    assert len(CASES['cases']) == 49


def test_roundtrip_preserves_claims_without_promoting_them(codec):
    message, trusted = copy.deepcopy(CASES['message']), copy.deepcopy(CASES['trusted'])
    raw = canonical(message)
    result = codec.decode(raw, schema_bytes=SCHEMA.read_bytes(), trusted=trusted)
    assert result == message
    assert codec.encode(result, schema_bytes=SCHEMA.read_bytes(), trusted=trusted) == raw
    assert message == CASES['message'] and trusted == CASES['trusted']
    assert result['evidence'][0]['status'] == 'SYNTHETIC_ONLY'
    assert result['assessment']['status'] == 'NOT_VERIFIED'
    assert 'admission' not in result and 'ticket' not in result
    assert set(result['context']) == {'run_id', 'snapshot_id', 'dataset_id', 'as_of'}
    assert result['candidate_release_id'] == 'SYN_CANDIDATE_1'


@pytest.mark.parametrize('case', CASES['cases'], ids=lambda case: case['case_id'])
def test_closed_contract_and_independent_trust_boundary(codec, case):
    message = patched(CASES['message'], case['message_edits'])
    trusted = patched(CASES['trusted'], case['trusted_edits'])
    raw = canonical(message)
    before = copy.deepcopy((message, trusted))
    with pytest.raises(codec.ContractError) as caught:
        codec.decode(raw, schema_bytes=SCHEMA.read_bytes(), trusted=trusted)
    assert str(caught.value) == 'C2_RUNTIME_CONTRACT_INVALID:' + case['expected_code']
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__
    assert (message, trusted) == before
    # Исключение не содержит raw payload, alias, частный путь или произвольный repr.
    assert 'PRIVATE_CANARY_7281' not in str(caught.value)


@pytest.mark.parametrize('raw,code', [
    (b'{"schema_id":"x","schema_id":"y"}', 'JSON_DUPLICATE_KEY'),
    (b'{"nested":{"x":1,"x":2}}', 'JSON_DUPLICATE_KEY'),
    (b'{"value":NaN}', 'JSON_CONSTANT'),
    (b'{"value":Infinity}', 'JSON_CONSTANT'),
    (b'{"value":-Infinity}', 'JSON_CONSTANT'),
    (b'{"value":1e309}', 'JSON_NONFINITE'),
    (b'\xef\xbb\xbf{}', 'JSON_SYNTAX'),
    (b'{"value":"\xff"}', 'JSON_SYNTAX'),
    (b'{"value":"\\ud800"}', 'JSON_TEXT'),
    (b'[] trailing', 'JSON_SYNTAX'),
])
def test_strict_json_and_redacted_errors(codec, raw, code):
    with pytest.raises(codec.ContractError, match='^C2_RUNTIME_CONTRACT_INVALID:' + code + '$'):
        codec.decode(raw, schema_bytes=SCHEMA.read_bytes(), trusted=CASES['trusted'])


def test_schema_drift_refused_even_when_payload_repeats_new_digest(codec):
    schema = json.loads(SCHEMA.read_bytes())
    schema['description'] += ' PRIVATE_CANARY_7281'
    modified = canonical(schema)
    message = copy.deepcopy(CASES['message'])
    message['schema_sha256'] = hashlib.sha256(modified).hexdigest()
    with pytest.raises(codec.ContractError, match=':SCHEMA_DIGEST$'):
        codec.decode(canonical(message), schema_bytes=modified, trusted=CASES['trusted'])


def test_common_mode_canonical_mutation_cannot_rebind_retained_selection(codec):
    message = copy.deepcopy(CASES['message'])
    message['canonical']['rows'][0][3] = {'kind': 'INT', 'value': '999'}
    message['canonical']['content_sha256'] = hashlib.sha256(canonical([
        message['canonical']['columns'], message['canonical']['rows']])).hexdigest()
    with pytest.raises(codec.ContractError, match=':CANONICAL_BINDING$'):
        codec.decode(canonical(message), schema_bytes=SCHEMA.read_bytes(), trusted=CASES['trusted'])


def test_common_mode_evidence_swap_cannot_rebind_retained_registry(codec):
    message = copy.deepcopy(CASES['message'])
    evidence = message['evidence'][0]
    evidence['evidence_id'] = 'SYN_FORGED_EVIDENCE'
    message['assessment']['checks'][0]['evidence_ref'] = evidence['evidence_id']
    with pytest.raises(codec.ContractError, match=':EVIDENCE_BINDING$'):
        codec.decode(canonical(message), schema_bytes=SCHEMA.read_bytes(), trusted=CASES['trusted'])


@pytest.mark.parametrize('role', ['SOURCE_WORKER', 'E_VERIFIER', 'Q_ASSESSOR', 'K_IDENTITY'])
def test_role_is_compared_with_separate_caller_binding(codec, role):
    message, trusted = copy.deepcopy(CASES['message']), copy.deepcopy(CASES['trusted'])
    message['producer']['role'] = trusted['producer']['role'] = role
    assert codec.decode(canonical(message), schema_bytes=SCHEMA.read_bytes(),
                        trusted=trusted)['producer']['role'] == role


def test_separately_bound_nominal_pass_does_not_create_official_evidence(codec):
    message = copy.deepcopy(CASES['message'])
    message['assessment']['status'] = 'PASS'
    message['assessment']['checks'][0].update(status='PASS', reason=None)
    result = codec.decode(canonical(message), schema_bytes=SCHEMA.read_bytes(), trusted=CASES['trusted'])
    assert result['assessment']['status'] == 'PASS'
    assert result['evidence'][0]['status'] == 'SYNTHETIC_ONLY'
    assert set(result) == set(message)  # no effective quality / admission result


def test_unavailable_canonical_and_previous_are_explicit_not_fabricated(codec):
    message, trusted = copy.deepcopy(CASES['message']), copy.deepcopy(CASES['trusted'])
    message['canonical'] = trusted['canonical'] = None
    message['null_reasons'] = {'canonical': 'NOT_AVAILABLE'}
    result = codec.decode(canonical(message), schema_bytes=SCHEMA.read_bytes(), trusted=trusted)
    assert result['canonical'] is None
    assert result['previous']['previous_release_id'] is None
    assert result['previous']['presence'] == 'NOT_VERIFIED'


def test_nullable_evidence_validity_remains_unknown(codec):
    message, trusted = copy.deepcopy(CASES['message']), copy.deepcopy(CASES['trusted'])
    for record in (message['evidence'][0], trusted['evidence'][0]):
        record['valid_until'] = None
        record['null_reasons'] = {'valid_until': 'NOT_AVAILABLE'}
    result = codec.decode(canonical(message), schema_bytes=SCHEMA.read_bytes(), trusted=trusted)
    assert result['evidence'][0]['valid_until'] is None
    assert result['assessment']['status'] == 'NOT_VERIFIED'


def test_duplicate_canonical_rows_are_preserved(codec):
    message, trusted = copy.deepcopy(CASES['message']), copy.deepcopy(CASES['trusted'])
    frame = message['canonical']
    frame['rows'].append(copy.deepcopy(frame['rows'][0]))
    frame['bindings'].append({'canonical_ordinal': 1, 'source_id': 'SYN_SOURCE_1', 'source_ordinal': 1})
    frame['row_count'] = 2
    frame['content_sha256'] = hashlib.sha256(canonical([frame['columns'], frame['rows']])).hexdigest()
    frame['bindings_sha256'] = hashlib.sha256(canonical(frame['bindings'])).hexdigest()
    # Здесь новый независимый basis задан caller как отдельный положительный case.
    trusted['canonical'] = {key: copy.deepcopy(value) for key, value in frame.items()
                            if key not in {'rows', 'bindings'}}
    assert codec.decode(canonical(message), schema_bytes=SCHEMA.read_bytes(),
                        trusted=trusted)['canonical']['rows'] == frame['rows']


def test_encode_and_decode_have_the_same_validation_boundary(codec):
    for case in CASES['cases']:
        message = patched(CASES['message'], case['message_edits'])
        trusted = patched(CASES['trusted'], case['trusted_edits'])
        with pytest.raises(codec.ContractError) as caught:
            codec.encode(message, schema_bytes=SCHEMA.read_bytes(), trusted=trusted)
        assert str(caught.value).endswith(':' + case['expected_code']), case['case_id']


def test_cyclic_python_input_fails_with_closed_safe_code(codec):
    message = copy.deepcopy(CASES['message'])
    message['PRIVATE_CANARY_7281'] = message
    with pytest.raises(codec.ContractError, match=':JSON_TYPE$'):
        codec.encode(message, schema_bytes=SCHEMA.read_bytes(), trusted=CASES['trusted'])


@pytest.mark.parametrize('value,expected', [
    (None, {'kind': 'NULL', 'value': None}),
    (False, {'kind': 'BOOL', 'value': False}),
    (True, {'kind': 'BOOL', 'value': True}),
    (9007199254740993, {'kind': 'INT', 'value': '9007199254740993'}),
    (-17, {'kind': 'INT', 'value': '-17'}),
    (0.1, {'kind': 'FLOAT64', 'value': '0x1.999999999999ap-4'}),
    (-0.0, {'kind': 'FLOAT64', 'value': '-0x0.0p+0'}),
    ('0.10000000000000001', {'kind': 'TEXT', 'value': '0.10000000000000001'}),
    ('\u0001RUB', {'kind': 'TEXT', 'value': '\u0001RUB'}),
])
def test_typed_cells_roundtrip_without_precision_or_semantic_coercion(codec, value, expected):
    assert codec.pack_value(value) == expected
    actual = codec.unpack_value(expected)
    assert type(actual) is type(value) and actual == value
    if type(value) is float:
        assert actual.hex() == value.hex()


@pytest.mark.parametrize('value,tag', [(float('nan'), 'NAN'),
    (float('inf'), 'POS_INF'), (float('-inf'), 'NEG_INF')])
def test_nonfinite_runtime_cells_use_descriptors_not_bare_json(codec, value, tag):
    cell = {'kind': 'NONFINITE', 'value': tag}
    assert codec.pack_value(value) == cell
    restored = codec.unpack_value(cell)
    assert (math.isnan(restored) if tag == 'NAN' else restored == value)
    assert json.loads(codec.encode_cell(cell)) == cell


@pytest.mark.parametrize('cell', [
    {'kind': 'INT', 'value': True}, {'kind': 'INT', 'value': '01'},
    {'kind': 'INT', 'value': '-0'}, {'kind': 'FLOAT64', 'value': '0.1'},
    {'kind': 'FLOAT64', 'value': 'inf'}, {'kind': 'FLOAT64', 'value': '0X1.0P+0'},
    {'kind': 'NULL', 'value': 'null'}, {'kind': 'BOOL', 'value': 1},
    {'kind': 'TEXT', 'value': None}, {'kind': 'NONFINITE', 'value': 'Infinity'},
    {'kind': 'TEXT', 'value': 'x', 'unknown': 1},
])
def test_invalid_typed_cells_are_not_coerced(codec, cell):
    with pytest.raises(codec.ContractError, match=':CELL$'):
        codec.unpack_value(cell)


def test_codec_has_no_clock_io_admission_or_application_dependency(codec, monkeypatch):
    # Schema bytes и независимый registry передаются caller, а не читаются codec.
    raw, schema = canonical(CASES['message']), SCHEMA.read_bytes()

    def forbidden(*args, **kwargs):
        pytest.fail('Codec attempted implicit file I/O')

    monkeypatch.setattr('builtins.open', forbidden)
    assert codec.decode(raw, schema_bytes=schema, trusted=CASES['trusted']) == CASES['message']
    assert not any(name in vars(codec) for name in (
        'dividends', 'dohodru_data', 'release_manifest', 'run_logging',
        'datetime_now', 'time', 'requests', 'urllib', 'subprocess', 'issue_ticket'))
