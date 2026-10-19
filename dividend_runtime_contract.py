"""Closed C2 wire codec. No source proof, admission, persistence or publication."""

# %%
from datetime import datetime
import hashlib
import json
import math
import re


SCHEMA_ID = 'mds.c2.runtime'
SCHEMA_VERSION = 1
SCHEMA_SHA256 = '2b1ac3102d695d6d8f86a4ff3146af015d71a210f352295f4a30da976400d73b'


class ContractError(ValueError):
    """A closed code only; never interpolate a payload or an exception."""


def _fail(code):
    raise ContractError('C2_RUNTIME_CONTRACT_INVALID:' + code) from None


def _require(condition, code):
    if not condition:
        _fail(code)


def _domain(value):
    # JSON допускает bool, но не как подтип integer; Python-specific объекты
    # и одинокие surrogate запрещены до сериализации и проверки формы.
    kind = type(value)
    if kind is str:
        try:
            value.encode('utf-8')
        except UnicodeError:
            _fail('JSON_TEXT')
    elif kind is float:
        _require(math.isfinite(value), 'JSON_NONFINITE')
    elif kind is list:
        for item in value:
            _domain(item)
    elif kind is dict:
        for key, item in value.items():
            _require(type(key) is str, 'JSON_TYPE')
            _domain(key)
            _domain(item)
    else:
        _require(value is None or kind in (int, bool), 'JSON_TYPE')


def _pairs(items):
    result = {}
    for key, value in items:
        _require(key not in result, 'JSON_DUPLICATE_KEY')
        result[key] = value
    return result


def _constant(value):
    _fail('JSON_CONSTANT')


def _load(raw):
    _require(type(raw) is bytes, 'JSON_TYPE')
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=_pairs,
                           parse_constant=_constant)
        _domain(value)
    except ContractError:
        raise
    except (UnicodeError, ValueError, RecursionError):
        _fail('JSON_SYNTAX')
    return value


def _canonical(value):
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False,
                          separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (UnicodeError, ValueError, RecursionError):
        _fail('JSON_TYPE')


def _checked_domain(value):
    try:
        _domain(value)
    except RecursionError:
        _fail('JSON_TYPE')


def schema_digest(schema_bytes):
    """Validate the frozen semantic schema digest; never read a default path."""
    schema = _load(schema_bytes)
    digest = hashlib.sha256(_canonical(schema)).hexdigest()
    _require(digest == SCHEMA_SHA256, 'SCHEMA_DIGEST')
    return digest


def _shape(value, rule, definitions):
    # Только подмножество JSON Schema, реально используемое frozen schema.
    # Exact digest не позволяет незаметно добавить неподдерживаемое keyword.
    if '$ref' in rule:
        return _shape(value, definitions[rule['$ref'].split('/')[-1]], definitions)
    if 'oneOf' in rule:
        matches = 0
        for alternative in rule['oneOf']:
            try:
                _shape(value, alternative, definitions)
                matches += 1
            except ContractError:
                pass
        _require(matches == 1, 'SHAPE')
        return
    if 'const' in rule:
        _require(type(value) is type(rule['const']) and value == rule['const'], 'SHAPE')
    if 'enum' in rule:
        _require(any(type(value) is type(item) and value == item
                     for item in rule['enum']), 'SHAPE')
    kind = rule.get('type')
    types = {'object': dict, 'array': list, 'string': str,
             'integer': int, 'boolean': bool, 'null': type(None)}
    if kind is not None:
        _require(type(value) is types[kind], 'SHAPE')
    if kind == 'object':
        properties = rule['properties']
        _require(set(rule.get('required', ())) <= value.keys(), 'SHAPE')
        _require(set(value) <= properties.keys(), 'SHAPE')
        for key, item in value.items():
            _shape(item, properties[key], definitions)
    elif kind == 'array':
        _require(len(value) >= rule.get('minItems', 0), 'SHAPE')
        _require('maxItems' not in rule or len(value) <= rule['maxItems'], 'SHAPE')
        for ordinal, item in enumerate(value):
            prefix = rule.get('prefixItems', [])
            item_rule = prefix[ordinal] if ordinal < len(prefix) else rule.get('items')
            if item_rule is not None:
                _shape(item, item_rule, definitions)
    elif kind == 'string' and 'pattern' in rule:
        _require(re.fullmatch(rule['pattern'], value) is not None, 'SHAPE')
    elif kind == 'integer':
        _require('minimum' not in rule or value >= rule['minimum'], 'SHAPE')


def _utc(value):
    try:
        _require(type(value) is str and re.fullmatch(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z', value)
            is not None, 'UTC')
        return datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        _fail('UTC')


def _context(value, expected):
    _utc(value['as_of'])
    _require(value == expected, 'CONTEXT')


def _nulls(value, fields):
    # Отсутствующий ключ ловит schema. Present-null требует ровно свою причину;
    # причина не может прикрывать существующее ненулевое значение.
    needed = {key for key in fields if value[key] is None}
    _require(set(value['null_reasons']) == needed, 'NULL_REASON')


def _inventory(items, field, code):
    values = [item[field] for item in items]
    _require(len(values) == len(set(values)), code)
    return set(values)


def _cell(value):
    _require(type(value) is dict and set(value) == {'kind', 'value'}, 'CELL')
    kind, item = value['kind'], value['value']
    _require(type(kind) is str, 'CELL')
    valid = False
    if kind == 'NULL':
        valid = item is None
    elif kind == 'BOOL':
        valid = type(item) is bool
    elif kind == 'INT':
        valid = type(item) is str and re.fullmatch(r'(0|-?[1-9][0-9]*)', item) is not None
    elif kind == 'TEXT':
        valid = type(item) is str
        if valid:
            _domain(item)
    elif kind == 'NONFINITE':
        valid = type(item) is str and item in {'NAN', 'POS_INF', 'NEG_INF'}
    elif kind == 'FLOAT64' and type(item) is str:
        try:
            actual = float.fromhex(item)
            valid = math.isfinite(actual) and actual.hex() == item
        except (ValueError, OverflowError):
            pass
    _require(valid, 'CELL')


def pack_value(value):
    """Describe an already decoded Python scalar; never infer a raw token."""
    kind = type(value)
    if value is None:
        return {'kind': 'NULL', 'value': None}
    if kind is bool:
        return {'kind': 'BOOL', 'value': value}
    if kind is int:
        try:
            return {'kind': 'INT', 'value': str(value)}
        except ValueError:
            _fail('CELL')
    if kind is str:
        _domain(value)
        return {'kind': 'TEXT', 'value': value}
    if kind is float:
        if math.isfinite(value):
            return {'kind': 'FLOAT64', 'value': value.hex()}
        tag = 'NAN' if math.isnan(value) else ('POS_INF' if value > 0 else 'NEG_INF')
        return {'kind': 'NONFINITE', 'value': tag}
    _fail('CELL')


def unpack_value(cell):
    """Restore a described scalar; this is not the C2 semantic quality guard."""
    _cell(cell)
    kind, value = cell['kind'], cell['value']
    if kind == 'INT':
        try:
            return int(value)
        except ValueError:
            _fail('CELL')
    if kind == 'FLOAT64':
        return float.fromhex(value)
    if kind == 'NONFINITE':
        return {'NAN': float('nan'), 'POS_INF': float('inf'),
                'NEG_INF': float('-inf')}[value]
    return value


def encode_cell(cell):
    _cell(cell)
    return _canonical(cell)


def _validate(message, schema_bytes, trusted):
    schema_digest(schema_bytes)
    schema = _load(schema_bytes)
    _require(type(message) is dict, 'SHAPE')
    if {'schema_id', 'schema_version', 'schema_sha256'} <= message.keys():
        _require(message['schema_id'] == SCHEMA_ID and
                 type(message['schema_version']) is int and
                 message['schema_version'] == SCHEMA_VERSION and
                 message['schema_sha256'] == SCHEMA_SHA256, 'VERSION')
    _checked_domain(trusted)
    definitions = schema['$defs']
    _shape(message, schema, definitions)
    _shape(trusted, definitions['trusted'], definitions)
    context = trusted['context']
    _context(context, context)
    _context(message['context'], context)
    _require(message['candidate_release_id'] == trusted['candidate_release_id'], 'CONTEXT')
    _require(message['producer'] == trusted['producer'], 'PRODUCER')
    _nulls(message, ('canonical',))
    evaluated = _utc(trusted['evaluation_as_of'])
    start, end = (_utc(trusted['acquisition_window'][key]) for key in ('start', 'end'))
    _require(start <= end, 'SOURCE_FRESHNESS')

    sources = message['source_bindings']
    source_ids = _inventory(sources, 'source_id', 'SOURCE_INVENTORY')
    for source in sources:
        _nulls(source, ('contract_evidence_ref', 'coverage_evidence_ref'))
    _require(sources == trusted['source_bindings'], 'SOURCE_BINDING')
    for source in sources:
        acquired = _utc(source['acquired_at'])
        _require(start <= acquired <= end and acquired <= evaluated, 'SOURCE_FRESHNESS')

    evidence = message['evidence']
    evidence_ids = _inventory(evidence, 'evidence_id', 'EVIDENCE_INVENTORY')
    for record in evidence:
        _context(record['context'], context)
        _require(record['candidate_release_id'] == message['candidate_release_id'], 'CONTEXT')
        _nulls(record, ('source_id', 'valid_until'))
    # Независимый retained registry приходит отдельным аргументом от caller.
    # Совместное изменение payload evidence и всех ссылок не меняет этот basis.
    _require(evidence == trusted['evidence'], 'EVIDENCE_BINDING')
    for record in evidence:
        _require(record['source_id'] is None or record['source_id'] in source_ids, 'REFERENCE')
        created = _utc(record['created_at'])
        _require(created <= evaluated, 'EVIDENCE_FRESHNESS')
        if record['valid_until'] is not None:
            _require(created <= evaluated <= _utc(record['valid_until']), 'EVIDENCE_FRESHNESS')

    references = [source[key] for source in sources
                  for key in ('contract_evidence_ref', 'coverage_evidence_ref')]
    previous = message['previous']
    _nulls(previous, ('previous_release_id', 'previous_snapshot_id',
                      'previous_as_of', 'evidence_ref'))
    if previous['presence'] == 'ABSENT':
        _require(previous['evidence_ref'] is not None and all(previous[key] is None
            for key in ('previous_release_id', 'previous_snapshot_id', 'previous_as_of')), 'PREVIOUS')
    if previous['presence'] == 'PRESENT':
        _require(all(previous[key] is not None for key in
            ('previous_release_id', 'previous_snapshot_id', 'previous_as_of', 'evidence_ref')), 'PREVIOUS')
    if previous['previous_as_of'] is not None:
        _utc(previous['previous_as_of'])
    _require(previous == trusted['previous'], 'PREVIOUS')
    references.append(previous['evidence_ref'])

    canonical = message['canonical']
    if canonical is None:
        _require(trusted['canonical'] is None, 'CANONICAL_BINDING')
    else:
        _context(canonical['context'], context)
        rows, bindings = canonical['rows'], canonical['bindings']
        _require(len(rows) == canonical['row_count'] == len(bindings), 'CANONICAL_BINDING')
        for row in rows:
            for cell in row:
                _cell(cell)
        for ordinal, binding in enumerate(bindings):
            _require(binding['canonical_ordinal'] == ordinal and
                     binding['source_id'] in source_ids, 'CANONICAL_BINDING')
        content = hashlib.sha256(_canonical([canonical['columns'], rows])).hexdigest()
        binding_digest = hashlib.sha256(_canonical(bindings)).hexdigest()
        _require(content == canonical['content_sha256'] and
                 binding_digest == canonical['bindings_sha256'], 'CANONICAL_BINDING')
        descriptor = {key: value for key, value in canonical.items()
                      if key not in {'rows', 'bindings'}}
        _require(descriptor == trusted['canonical'], 'CANONICAL_BINDING')

    checks = message['assessment']['checks']
    _inventory(checks, 'check_id', 'CHECK_INVENTORY')
    for check in checks:
        _require((check['reason'] is None) == (check['status'] == 'PASS'), 'NULL_REASON')
        references.append(check['evidence_ref'])
    _require(all(reference is None or reference in evidence_ids
                 for reference in references), 'REFERENCE')


def decode(raw, *, schema_bytes, trusted):
    """Decode against caller-retained identities; return data, not admission."""
    message = _load(raw)
    _validate(message, schema_bytes, trusted)
    return message


def encode(message, *, schema_bytes, trusted):
    """Validate before emitting canonical UTF-8 bytes, without mutating inputs."""
    _checked_domain(message)
    _validate(message, schema_bytes, trusted)
    return _canonical(message)
