"""Offline workspace declaration contract; no writer or OS authority is created."""

import copy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCHEMA = ROOT / 'contracts/release_workspace_v1.json'
FIXTURE = ROOT / 'tests/fixtures/d4/workspace_contract_v1.json'
IDENTITY = 'mds.release.workspace/1'
CONTEXT_KEYS = {'run_id', 'snapshot_id', 'dataset_id', 'as_of'}
KINDS = ('SOURCE', 'CONFIGURATION', 'PREVIOUS', 'CANDIDATE_A', 'CANDIDATE_B',
         'CANDIDATE_C', 'CAPTURE_SPOOL', 'SEALED_EVIDENCE', 'CURRENT')
PREPARERS = ['PREPARER_A', 'PREPARER_B', 'PREPARER_C']
READERS = PREPARERS + ['SOURCE_WORKER', 'E_VERIFIER', 'Q_ASSESSOR', 'D4']
ACCESS = {
    'SOURCE': ('INPUT_OWNER', READERS, []),
    'CONFIGURATION': ('INPUT_OWNER', READERS, []),
    'PREVIOUS': ('INPUT_OWNER', PREPARERS + ['E_VERIFIER', 'Q_ASSESSOR', 'D4'], []),
    'CANDIDATE_A': ('A', PREPARERS + ['E_VERIFIER', 'Q_ASSESSOR', 'D4'], ['PREPARER_A']),
    'CANDIDATE_B': ('B', ['PREPARER_B', 'E_VERIFIER', 'Q_ASSESSOR', 'D4'], ['PREPARER_B']),
    'CANDIDATE_C': ('C2', ['PREPARER_C', 'E_VERIFIER', 'Q_ASSESSOR', 'D4'], ['PREPARER_C']),
    'CAPTURE_SPOOL': ('F_SOURCE', ['SOURCE_WORKER', 'E_VERIFIER'], ['SOURCE_WORKER']),
    'SEALED_EVIDENCE': ('E', ['E_VERIFIER', 'Q_ASSESSOR', 'D4'], ['E_VERIFIER']),
    'CURRENT': ('D_PUBLISH', [], []),
}
RESULT = {'workspace_status': 'VALID_DECLARATION', 'candidate_state': 'PREPARED',
          'publication_status': 'NOT_ATTEMPTED', 'authority': 'DECLARATION_ONLY',
          'evaluation_scope': 'SYNTHETIC_ONLY', 'native_containment': 'NOT_VERIFIED',
          'official_source_status': 'NOT_VERIFIED'}


class ContractError(ValueError):
    """Сообщение содержит только закрытый code, без исходных paths/values."""


def _require(condition, code):
    if not condition:
        raise ContractError(code)


def _pairs(pairs):
    value = {}
    for key, item in pairs:
        _require(key not in value, 'JSON_DUPLICATE_KEY')
        value[key] = item
    return value


def _constant(_):
    raise ContractError('JSON_NONSTANDARD_CONSTANT')


def _decode(text):
    try:
        return json.loads(text, object_pairs_hook=_pairs, parse_constant=_constant)
    except (json.JSONDecodeError, UnicodeError, TypeError):
        raise ContractError('JSON_SYNTAX') from None


def _digest(value):
    # Канонический JSON, а не checkout bytes: LF/CRLF не меняют identity контракта.
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True,
                         separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _shape(value, specification, schema):
    """Небольшой test-only interpreter ровно используемого JSON Schema subset."""
    if '$ref' in specification:
        name = specification['$ref'].removeprefix('#/$defs/')
        _require(specification['$ref'] == '#/$defs/' + name, 'SCHEMA_DEFINITION')
        return _shape(value, schema['$defs'][name], schema)
    if 'anyOf' in specification:
        for choice in specification['anyOf']:
            try:
                _shape(value, choice, schema)
                return
            except ContractError:
                pass
        raise ContractError('SCHEMA')
    if 'const' in specification:
        _require(type(value) is type(specification['const']) and
                 value == specification['const'], 'SCHEMA')
    if 'enum' in specification:
        _require(any(type(value) is type(item) and value == item
                     for item in specification['enum']), 'SCHEMA')
    kind = specification.get('type')
    types = {'object': dict, 'array': list, 'string': str, 'integer': int,
             'boolean': bool, 'null': type(None)}
    if kind:
        _require(type(value) is types[kind], 'SCHEMA')
    if kind == 'object':
        _require(set(specification['required']) <= set(value), 'SCHEMA')
        if specification.get('additionalProperties') is False:
            _require(set(value) <= set(specification['properties']), 'SCHEMA')
        for name, item in value.items():
            _shape(item, specification['properties'][name], schema)
    elif kind == 'array':
        _require(len(value) >= specification.get('minItems', 0), 'SCHEMA')
        _require(len(value) <= specification.get('maxItems', len(value)), 'SCHEMA')
        if specification.get('uniqueItems'):
            _require(len({_digest(item) for item in value}) == len(value), 'SCHEMA')
        for item in value:
            _shape(item, specification['items'], schema)
    elif kind == 'string':
        _require(len(value) >= specification.get('minLength', 0), 'SCHEMA')
        _require(len(value) <= specification.get('maxLength', len(value)), 'SCHEMA')
        if 'pattern' in specification:
            _require(re.fullmatch(specification['pattern'], value) is not None, 'SCHEMA')
    elif kind == 'integer':
        _require(value >= specification.get('minimum', value), 'SCHEMA')


def _utc(value):
    try:
        datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
    except (ValueError, TypeError):
        raise ContractError('CONTEXT') from None


def _context(value, expected=None):
    _require(type(value) is dict and set(value) == CONTEXT_KEYS, 'CONTEXT')
    _utc(value['as_of'])
    _require(expected is None or value == expected, 'CONTEXT')


def _header(value, digest):
    _require(value['schema_id'] == 'mds.release.workspace' and
             type(value['schema_version']) is int and value['schema_version'] == 1,
             'VERSION')
    _require(value['schema_digest'] == digest, 'SCHEMA_DIGEST')


def _components(path, platform, *, absolute):
    # Это лексическая проверка. resolve(), stat() и чтение реальных roots запрещены:
    # synthetic declaration не доказывает отсутствие symlink/TOCTOU на native host.
    _require(type(path) is str and bool(path) and '\\' not in path, 'PATH')
    if absolute:
        if platform == 'WINDOWS':
            _require(re.match(r'^[A-Z]:/', path) is not None, 'PATH')
            prefix, remainder = path[:2].casefold(), path[3:]
        else:
            _require(path.startswith('/') and not path.startswith('//'), 'PATH')
            prefix, remainder = '/', path[1:]
    else:
        _require(not path.startswith('/') and ':' not in path, 'PATH')
        prefix, remainder = '', path
    parts = remainder.split('/')
    _require(all(part not in {'', '.', '..'} and
                 re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9._-]*', part) and
                 not part.endswith(('.', ' ')) for part in parts), 'PATH')
    if platform == 'WINDOWS':
        reserved = {'CON', 'PRN', 'AUX', 'NUL'} | {
            name + str(number) for name in ('COM', 'LPT') for number in range(1, 10)}
        _require(all(part.split('.')[0].upper() not in reserved for part in parts), 'PATH')
        parts = [part.casefold() for part in parts]
    return (prefix, *parts)


def _generation(value):
    _utc(value['as_of'])


def _basis(group, registry, context):
    by_id = {item['basis_id']: item for item in registry['basis_records']}
    _require(len(by_id) == len(registry['basis_records']), 'BASIS_INVENTORY')
    used = []
    for name, kind in (('as_of', 'AS_OF'), ('acquisition', 'ACQUISITION'),
                       ('coverage', 'COVERAGE')):
        record = group[name]
        if record['status'] == 'NOT_VERIFIED':
            _require(record['basis_ref'] is None and record['reason'] == 'NOT_VERIFIED',
                     'BASIS_NULL')
            if name == 'acquisition':
                _require(record['acquired_at'] is None, 'BASIS_NULL')
            if name == 'coverage':
                _require(record['scope_id'] is None and record['as_of'] is None, 'BASIS_NULL')
            continue
        _require(record['basis_ref'] is not None and record['reason'] is None, 'BASIS_NULL')
        _require(record['basis_ref'] in by_id, 'REFERENCE')
        retained = by_id[record['basis_ref']]
        _require(retained['kind'] == kind, 'BASIS_KIND')
        _context(retained['context'], context)
        used.append(record['basis_ref'])
        if name == 'acquisition':
            _utc(record['acquired_at'])
        elif name == 'coverage':
            _require(record['scope_id'] is not None and record['as_of'] == context['as_of'],
                     'COVERAGE_BASIS')
    _require(len(used) == len(set(used)), 'BASIS_INDEPENDENCE')


def _previous(previous, registry, context, candidate_id):
    _context(previous['captured_for'], context)
    identity = [previous[name] for name in ('release_id', 'snapshot_id', 'as_of')]
    if previous['state'] == 'NOT_VERIFIED':
        _require(identity == [None, None, None] and previous['evidence_ref'] is None and
                 previous['reason'] == 'NOT_VERIFIED', 'PREVIOUS_STATE')
        return
    _require(previous['evidence_ref'] is not None and previous['reason'] is None,
             'PREVIOUS_STATE')
    records = {item['basis_id']: item for item in registry['basis_records']}
    _require(previous['evidence_ref'] in records, 'REFERENCE')
    basis = records[previous['evidence_ref']]
    _context(basis['context'], context)
    if previous['state'] == 'ABSENT':
        _require(identity == [None, None, None] and basis['kind'] == 'PREVIOUS_ABSENCE' and
                 basis['generation'] is None, 'PREVIOUS_STATE')
    else:
        _require(all(value is not None for value in identity), 'PREVIOUS_STATE')
        _utc(previous['as_of'])
        _require(previous['release_id'] != candidate_id and
                 previous['snapshot_id'] != context['snapshot_id'], 'GENERATION')
        _require(basis['kind'] == 'PREVIOUS_CONTENT' and basis['generation'] == {
            name: previous[name] for name in ('release_id', 'snapshot_id', 'as_of')},
            'GENERATION')


def _evaluate(schema, workspace, retained_registry):
    """Reference reducer для tests. expected сюда не передаётся; I/O отсутствует."""
    digest = _digest(schema)
    for record, definition in ((workspace, 'workspace'), (retained_registry, 'registry')):
        _require(type(record) is dict, 'SCHEMA')
        _require({'schema_id', 'schema_version', 'schema_digest'} <= set(record), 'SCHEMA')
        _header(record, digest)
        _shape(record, schema['$defs'][definition], schema)
    context = workspace['context']
    _context(context)
    _context(retained_registry['context'], context)
    _require(workspace['candidate_release_id'] == retained_registry['candidate_release_id'],
             'GENERATION')
    _require(workspace['registry_ref'] == retained_registry['registry_id'], 'REGISTRY_BINDING')
    platform = workspace['platform']
    _require(platform == retained_registry['platform'], 'PLATFORM')
    roots = retained_registry['roots']
    by_id = {root['root_id']: root for root in roots}
    _require(len(by_id) == len(roots) and {root['kind'] for root in roots} == set(KINDS),
             'ROOT_INVENTORY')
    _require(set(workspace['root_ids']) == set(by_id), 'ROOT_INVENTORY')
    paths = []
    for root in roots:
        _require(root['alias_status'] == 'PLAIN_DECLARATION', 'ROOT_ALIAS')
        owner, readers, writers = ACCESS[root['kind']]
        _require(root['owner'] == owner and root['read_roles'] == readers and
                 root['write_roles'] == writers, 'ROOT_AUTHORITY')
        parts = _components(root['path'], platform, absolute=True)
        for previous in paths:
            common = min(len(previous), len(parts))
            _require(previous[:common] != parts[:common], 'ROOT_OVERLAP')
        paths.append(parts)
    for record in retained_registry['basis_records']:
        _context(record['context'], context)
        if record['generation'] is not None:
            _generation(record['generation'])
    _basis(workspace['bases'], retained_registry, context)
    _previous(workspace['previous'], retained_registry, context,
              workspace['candidate_release_id'])
    operations = workspace['operations']
    _require(len({item['operation_id'] for item in operations}) == len(operations),
             'OPERATION_INVENTORY')
    for operation in operations:
        _context(operation['context'], context)
        _require(operation['candidate_release_id'] == workspace['candidate_release_id'],
                 'GENERATION')
        _require(operation['root_id'] in by_id, 'REFERENCE')
        root = by_id[operation['root_id']]
        _components(operation['relative_path'], platform, absolute=False)
        _require(root['kind'] != 'CURRENT', 'CURRENT_ACCESS')
        allowed = root['write_roles' if operation['access'] == 'WRITE' else 'read_roles']
        _require(operation['actor'] in allowed, 'ROOT_ACCESS')
        generation = operation['generation']
        if root['kind'].startswith('CANDIDATE_'):
            _require(generation == {'release_id': workspace['candidate_release_id'],
                'snapshot_id': context['snapshot_id'], 'as_of': context['as_of']} and
                operation['generation_reason'] is None, 'GENERATION')
        elif root['kind'] == 'PREVIOUS':
            _require(workspace['previous']['state'] == 'PRESENT' and generation == {
                name: workspace['previous'][name] for name in ('release_id', 'snapshot_id', 'as_of')}
                and operation['generation_reason'] is None, 'GENERATION')
        else:
            _require(generation is None and operation['generation_reason'] == 'NOT_APPLICABLE',
                     'GENERATION_NULL')
    return dict(RESULT)


def _fixture():
    value = _decode(FIXTURE.read_text(encoding='utf-8'))
    _require(set(value) == {'schema_id', 'schema_version', 'origin', 'contract_identity',
        'contract_digest', 'registries', 'workspaces', 'cases'}, 'FIXTURE')
    _require(value['schema_id'] == 'mds.release.workspace_cases' and
             type(value['schema_version']) is int and value['schema_version'] == 1 and
             value['origin'] == 'SYNTHETIC_ONLY' and value['contract_identity'] == IDENTITY,
             'FIXTURE')
    ids = [case['case_id'] for case in value['cases']]
    _require(len(ids) == len(set(ids)) and bool(ids), 'FIXTURE')
    return value


def _case_inputs(fixture, case):
    workspace = copy.deepcopy(fixture['workspaces'][case['profile']])
    registry = copy.deepcopy(fixture['registries'][case['profile']])
    # Patch descriptors только для manually reviewed negative vectors. Они не
    # вычисляют expected outcomes и не дают fixture произвольный eval/callable.
    for change in case['changes']:
        target = {'workspace': workspace, 'registry': registry}[change['target']]
        for part in change['path'][:-1]:
            target = target[part]
        last = change['path'][-1]
        if change['op'] == 'remove':
            del target[last]
        else:
            _require(change['op'] == 'set', 'FIXTURE')
            target[last] = copy.deepcopy(change['value'])
    return workspace, registry


CASES = _fixture()['cases']


@pytest.mark.parametrize('case', CASES, ids=lambda item: item['case_id'])
def test_workspace_contract(case):
    fixture = _fixture()
    schema = _decode(SCHEMA.read_text(encoding='utf-8'))
    assert _digest(schema) == fixture['contract_digest']
    workspace, registry = _case_inputs(fixture, case)
    expected = case['expected']
    if expected['contract_error'] is not None:
        with pytest.raises(ContractError) as caught:
            _evaluate(schema, workspace, registry)
        assert str(caught.value) == expected['contract_error']
    else:
        assert _evaluate(schema, workspace, registry) == expected['result']


@pytest.mark.parametrize('text,code', [
    ('{"a":1,"a":2}', 'JSON_DUPLICATE_KEY'),
    ('{"a":{"b":1,"b":2}}', 'JSON_DUPLICATE_KEY'),
    ('{"value":NaN}', 'JSON_NONSTANDARD_CONSTANT'),
    ('{"value":Infinity}', 'JSON_NONSTANDARD_CONSTANT'),
    ('{"value":-Infinity}', 'JSON_NONSTANDARD_CONSTANT'),
    ('{"truncated":', 'JSON_SYNTAX'),
])
def test_closed_json_decoder(text, code):
    with pytest.raises(ContractError) as caught:
        _decode(text)
    assert str(caught.value) == code


def test_schema_and_fixture_are_frozen_independent_contracts():
    fixture = _fixture()
    schema = _decode(SCHEMA.read_text(encoding='utf-8'))
    assert schema['$id'] == IDENTITY
    assert schema['x-contract']['identity'] == IDENTITY
    assert _digest(schema) == fixture['contract_digest']
    assert schema['x-contract']['runtime_authority_created'] is False
    assert schema['x-contract']['native_containment_proven'] is False
    assert len(fixture['cases']) == 83


def test_expected_outputs_are_not_inputs_to_reducer():
    fixture = _fixture()
    schema = _decode(SCHEMA.read_text(encoding='utf-8'))
    case = copy.deepcopy(fixture['cases'][0])
    workspace, registry = _case_inputs(fixture, case)
    before = _evaluate(schema, workspace, registry)
    case['expected'] = {'contract_error': 'CURRENT_ACCESS'}
    assert _evaluate(schema, workspace, registry) == before
    assert workspace == fixture['workspaces'][case['profile']]
    assert registry == fixture['registries'][case['profile']]


def test_error_does_not_disclose_untrusted_path():
    fixture = _fixture()
    schema = _decode(SCHEMA.read_text(encoding='utf-8'))
    workspace = copy.deepcopy(fixture['workspaces']['linux'])
    registry = copy.deepcopy(fixture['registries']['linux'])
    private_path = '/private-secret/../payload'
    workspace['operations'][0]['relative_path'] = private_path
    with pytest.raises(ContractError) as caught:
        _evaluate(schema, workspace, registry)
    assert str(caught.value) == 'PATH'
    assert private_path not in str(caught.value)
