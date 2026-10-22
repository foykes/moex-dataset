"""Offline verification of retained raw and source bindings; no capture authority."""

# %%
import copy
from datetime import datetime
from decimal import Decimal, DecimalException
from fractions import Fraction
import hashlib
import json
import re

import dividend_runtime_contract as runtime


U_SCHEMA_SHA256 = 'a0daced78e661713d75a5f768df5a354f5caf3c82a9f0a76a7ca53f10ad19619'
COLUMNS = ['ISIN', 'TRADE_CODE', 'dt', 'value', 'currency']
SOURCE_FIELDS = ['registryclosedate', 'value', 'currencyid']
OBLIGATIONS = ('ORIGIN', 'SOURCE_SEMANTICS', 'DATE_SEMANTICS', 'AMOUNT_UNITS',
               'AMOUNT_GROSS_NET', 'AMOUNT_SIGN', 'CURRENCY_SEMANTICS', 'COVERAGE')
STATUSES = {'VERIFIED', 'HISTORICAL_ONLY', 'SYNTHETIC_ONLY', 'NOT_VERIFIED', 'CONFLICTED'}
MAX_COEFFICIENT_DIGITS = 1024
MAX_ABSOLUTE_EXPONENT = 1024


class EvidenceError(ValueError):
    """Closed safe error; source values, document text and paths never appear."""


def _require(condition, code):
    if not condition:
        raise EvidenceError('C2_EVIDENCE_INVALID:' + code) from None


def _domain(value, code='SHAPE', *, source=False):
    # In-process retained input также проверяется до equality/hash. Объект с
    # собственным __eq__/repr не может подменить ни identity, ни trusted basis.
    kind = type(value)
    if kind is dict:
        _require(all(type(key) is str for key in value), code)
        for key, item in value.items():
            _domain(key, code, source=source)
            _domain(item, code, source=source)
    elif kind is list:
        for item in value:
            _domain(item, code, source=source)
    elif kind is str:
        try:
            value.encode('utf-8')
        except UnicodeError:
            _require(False, code)
    else:
        _require(value is None or kind in (bool, int) or source and kind is float, code)


def _keys(value, names, code='SHAPE'):
    _require(type(value) is dict and set(value) == set(names), code)


def _alias(value, nullable=False):
    _require(nullable and value is None or type(value) is str and
             re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}', value) is not None, 'SHAPE')


def _digest(value, nullable=False):
    _require(nullable and value is None or type(value) is str and
             re.fullmatch(r'[0-9a-f]{64}', value) is not None, 'SHAPE')


def _utc(value):
    _require(type(value) is str and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z', value) is not None, 'SHAPE')
    try:
        return datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        _require(False, 'SHAPE')


def _context(value):
    _keys(value, ('run_id', 'snapshot_id', 'dataset_id', 'as_of'))
    for key in ('run_id', 'snapshot_id', 'dataset_id'):
        _alias(value[key])
    _utc(value['as_of'])


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def _hash(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _json(raw, code, *, source=False):
    _require(type(raw) is bytes and not raw.startswith(b'\xef\xbb\xbf'), code)
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, code)
            result[key] = value
        return result
    def constant(value):
        _require(False, code)
    try:
        text = raw.decode('utf-8')
        value = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
        _domain(value, code, source=source)
        return value, text
    except (UnicodeError, ValueError, RecursionError):
        _require(False, code)


def _profile(raw, retained, capture):
    value, _ = _json(raw, 'PROFILE')
    _keys(value, ('profile_id', 'profile_version', 'u_schema_sha256', 'mapping',
                  'raw_format', 'source_fact_status', 'columns', 'source_fields',
                  'required_evidence'), 'PROFILE')
    _require(value['profile_id'] == 'C2_E_OFFLINE_JSON_V1' and
             type(value['profile_version']) is int and value['profile_version'] == 1 and
             value['u_schema_sha256'] == U_SCHEMA_SHA256 and
             value['mapping'] == 'MOEX_DIVIDENDS_JSON_V1' and
             value['raw_format'] == 'STANDARD_JSON_UTF8' and
             value['source_fact_status'] == 'NOT_VERIFIED' and
             value['columns'] == COLUMNS and value['source_fields'] == SOURCE_FIELDS and
             value['required_evidence'] == list(OBLIGATIONS), 'PROFILE')
    digest = _hash(value)
    _require(digest == retained['profile_sha256'] == capture['profile_sha256'], 'PROFILE')
    return digest


def _capture(value):
    _keys(value, ('capture_id', 'context', 'candidate_release_id', 'source_binding',
                  'request_id', 'instrument', 'attempt', 'complete', 'profile_sha256'))
    _context(value['context'])
    for key in ('capture_id', 'candidate_release_id', 'request_id'):
        _alias(value[key])
    _digest(value['profile_sha256'])
    _require(type(value['attempt']) is int and value['attempt'] >= 1 and type(value['complete']) is bool, 'SHAPE')
    _keys(value['instrument'], ('isin', 'secid'))
    for cell in value['instrument'].values():
        _require(type(cell) is str and bool(cell), 'SHAPE')
    source = value['source_binding']
    _keys(source, ('source_id', 'acquisition_id', 'source_kind', 'raw_sha256', 'raw_length',
                   'acquired_at', 'contract_evidence_ref', 'coverage_evidence_ref', 'null_reasons'))
    for key in ('source_id', 'acquisition_id'):
        _alias(source[key])
    _require(type(source['source_kind']) is str and source['source_kind'] in {'MOEX_ISS', 'DOHOD', 'SYNTHETIC_ONLY'}, 'SHAPE')
    _digest(source['raw_sha256'])
    _require(type(source['raw_length']) is int and source['raw_length'] >= 0, 'SHAPE')
    _utc(source['acquired_at'])
    nullable = ('contract_evidence_ref', 'coverage_evidence_ref')
    for key in nullable:
        _alias(source[key], nullable=True)
    reasons = source['null_reasons']
    _require(type(reasons) is dict and set(reasons) == {key for key in nullable if source[key] is None}
             and all(type(reason) is str and reason in {'NOT_AVAILABLE', 'NOT_APPLICABLE'} for reason in reasons.values()), 'SHAPE')


def _evidence(retained, capture):
    requirements, records = retained['requirements'], retained['evidence']
    _require(type(requirements) is list and type(records) is list, 'SHAPE')
    required, evidence = {}, {}
    for item in requirements:
        _keys(item, ('obligation', 'evidence_ref', 'subject_sha256', 'assessor_id', 'profile_sha256'))
        _require(type(item['obligation']) is str and item['obligation'] in OBLIGATIONS, 'EVIDENCE_INVENTORY')
        _require(item['obligation'] not in required, 'EVIDENCE_INVENTORY')
        for key in ('evidence_ref', 'assessor_id'):
            _alias(item[key], nullable=True)
        for key in ('subject_sha256', 'profile_sha256'):
            _digest(item[key], nullable=True)
        fields = [item[key] for key in ('evidence_ref', 'subject_sha256', 'assessor_id', 'profile_sha256')]
        _require(all(value is None for value in fields) or all(value is not None for value in fields), 'EVIDENCE_BINDING')
        required[item['obligation']] = item
    _require(set(required) == set(OBLIGATIONS), 'EVIDENCE_INVENTORY')
    source = capture['source_binding']
    refs = [item['evidence_ref'] for item in requirements if item['evidence_ref'] is not None]
    _require(len(refs) == len(set(refs)), 'EVIDENCE_INVENTORY')
    for record in records:
        _keys(record, ('evidence_id', 'context', 'candidate_release_id', 'source_id', 'acquisition_id',
                       'subject_text', 'status', 'assessor_role', 'assessor_id', 'profile_sha256',
                       'created_at', 'valid_until', 'applicable_as_of_start', 'applicable_as_of_end'))
        for key in ('evidence_id', 'candidate_release_id', 'source_id', 'acquisition_id', 'assessor_id'):
            _alias(record[key])
        _context(record['context'])
        _digest(record['profile_sha256'])
        _require(type(record['status']) is str and record['status'] in STATUSES and type(record['subject_text']) is str, 'SHAPE')
        _utc(record['created_at'])
        for key in ('valid_until', 'applicable_as_of_start', 'applicable_as_of_end'):
            if record[key] is not None:
                _utc(record[key])
        _require(record['evidence_id'] not in evidence, 'EVIDENCE_INVENTORY')
        evidence[record['evidence_id']] = record
    _require(set(refs) == set(evidence), 'EVIDENCE_BINDING')
    _require(source['contract_evidence_ref'] == required['SOURCE_SEMANTICS']['evidence_ref'] and
             source['coverage_evidence_ref'] == required['COVERAGE']['evidence_ref'], 'EVIDENCE_BINDING')
    observed, effective = [], {}
    evaluated, as_of = _utc(retained['evaluation_as_of']), _utc(capture['context']['as_of'])
    for name in OBLIGATIONS:
        binding = required[name]
        record = evidence.get(binding['evidence_ref'])
        status, reason = 'NOT_VERIFIED', 'MISSING_EVIDENCE'
        if record is not None:
            # Документ и declared VERIFIED не удостоверяют сами себя: ожидаемые
            # subject/profile/assessor взяты из отдельного parent-retained basis.
            _require(record['context'] == capture['context'] and
                record['candidate_release_id'] == capture['candidate_release_id'] and
                record['source_id'] == source['source_id'] and
                record['acquisition_id'] == source['acquisition_id'] and
                record['assessor_role'] == 'E_VERIFIER' and
                record['assessor_id'] == binding['assessor_id'] and
                record['profile_sha256'] == binding['profile_sha256'] and
                hashlib.sha256(record['subject_text'].encode('utf-8')).hexdigest() == binding['subject_sha256'],
                'EVIDENCE_BINDING')
            reason = 'UNVERIFIED_EVIDENCE'
            if record['status'] == 'VERIFIED' and source['source_kind'] != 'SYNTHETIC_ONLY':
                validity = (record['valid_until'], record['applicable_as_of_start'], record['applicable_as_of_end'])
                if all(value is not None for value in validity):
                    if (_utc(record['created_at']) <= evaluated <= _utc(record['valid_until']) and
                            _utc(record['applicable_as_of_start']) <= as_of <= _utc(record['applicable_as_of_end'])):
                        status, reason = 'VERIFIED', None
                    else:
                        reason = 'STALE_OR_INAPPLICABLE'
                else:
                    reason = 'UNKNOWN_VALIDITY'
        effective[name] = status
        observed.append(dict(obligation=name, evidence_ref=binding['evidence_ref'],
            observed_status=None if record is None else record['status'], effective_status=status, reason=reason))
    semantics = 'VERIFIED' if all(effective[name] == 'VERIFIED' for name in OBLIGATIONS[1:7]) else 'NOT_VERIFIED'
    return observed, effective['ORIGIN'], semantics, effective['COVERAGE']


def _scalar_locations(text):
    # После strict JSON проверки этот проход только находит locators. Он не
    # ищет token через str.find: одинаковое число в другом поле не подходит.
    decoder = json.JSONDecoder()
    locations = {}
    def whitespace(index):
        while index < len(text) and text[index] in ' \t\r\n':
            index += 1
        return index
    def walk(index, path):
        index = whitespace(index)
        opening = text[index]
        if opening in '[{':
            closing = ']' if opening == '[' else '}'
            index, ordinal = whitespace(index + 1), 0
            while text[index] != closing:
                if opening == '{':
                    key, index = decoder.raw_decode(text, index)
                    index = whitespace(index) + 1
                else:
                    key = ordinal
                index = whitespace(walk(index, (*path, key)))
                ordinal += 1
                if text[index] == ',':
                    index = whitespace(index + 1)
                else:
                    break
            return index + 1
        _, end = decoder.raw_decode(text, index)
        locations[path] = (index, end)
        return end
    walk(0, ())
    offsets = [0]
    for character in text:
        offsets.append(offsets[-1] + len(character.encode('utf-8')))
    return locations, offsets


def _measure(literal, value, typed):
    # Это предел вычислителя exact delta, а не допустимой суммы MOEX. Проверка
    # выполняется до Decimal/Fraction и возведения 10 в степень. Без неё малый
    # decoded float 0.0 мог скрыть очень дорогой исходный decimal exponent.
    coefficient, _, exponent = literal.lower().partition('e')
    digits = coefficient.lstrip('-').replace('.', '')
    _require(len(digits) <= MAX_COEFFICIENT_DIGITS, 'PRECISION_CAPACITY')
    exponent_digits = exponent.lstrip('+-').lstrip('0') or '0'
    _require(len(exponent_digits) <= 4, 'PRECISION_CAPACITY')
    _require(int(exponent_digits) <= MAX_ABSOLUTE_EXPONENT, 'PRECISION_CAPACITY')
    try:
        exact = Decimal(literal)
        reference = str(exact)
        if typed['kind'] == 'NONFINITE':
            return reference, None, 'NONFINITE'
        difference = Fraction(value) - Fraction(exact)
        delta = dict(numerator=str(difference.numerator), denominator=str(difference.denominator))
        return reference, delta, None
    except (DecimalException, ValueError, OverflowError):
        # Сохраняется закрытая безопасная ошибка даже при отличающемся runtime
        # conversion limit; raw token и текст внутреннего исключения не выходят.
        _require(False, 'PRECISION_CAPACITY')


def _selection(document, text, capture):
    _require(type(document) is dict and type(document.get('dividends')) is dict, 'SOURCE_SHAPE')
    block = document['dividends']
    columns, data = block.get('columns'), block.get('data')
    _require(type(columns) is list and all(type(name) is str for name in columns) and
             len(set(columns)) == len(columns) and all(name in columns for name in SOURCE_FIELDS)
             and type(data) is list, 'SOURCE_SHAPE')
    selected = [columns.index(name) for name in SOURCE_FIELDS]
    try:
        locations, offsets = _scalar_locations(text)
    except (RecursionError, ValueError):
        _require(False, 'RAW_JSON')
    rows, bindings, tokens = [], [], []
    for ordinal, row in enumerate(data):
        _require(type(row) is list and len(row) == len(columns), 'SOURCE_SHAPE')
        values = [row[index] for index in selected]
        _require(all(type(value) in (str, int, float, bool) or value is None for value in values), 'SOURCE_SHAPE')
        # ISIN/SECID не объявляются найденными в response: это заранее связанная
        # request association. Её официальный смысл этим helper не доказан.
        instrument = capture['instrument']
        typed = [runtime.pack_value(value) for value in (instrument['isin'], instrument['secid'], *values)]
        rows.append(typed)
        bindings.append(dict(canonical_ordinal=ordinal, source_id=capture['source_binding']['source_id'], source_ordinal=ordinal))
        start, end = locations['dividends', 'data', ordinal, selected[1]]
        literal = text[start:end]
        kind = ('STRING' if literal.startswith('"') else 'NULL' if literal == 'null' else
                'BOOL' if literal in ('true', 'false') else 'NUMBER')
        reference, delta, reason = None, None, 'NON_NUMERIC'
        if kind == 'NUMBER':
            # Decimal строится из исходного token независимо от decoded value.
            # Fraction исключает округление delta текущим Decimal context.
            reference, delta, reason = _measure(literal, values[1], typed[3])
        tokens.append(dict(source_ordinal=ordinal, source_column=selected[1], field='value',
            byte_start=offsets[start], byte_end=offsets[end], literal=literal, kind=kind,
            decimal_reference=reference, decoded=typed[3], delta_exact=delta, delta_reason=reason))
    selection = dict(columns=list(COLUMNS), rows=rows, bindings=bindings, row_count=len(rows),
        content_sha256=_hash([COLUMNS, rows]), bindings_sha256=_hash(bindings),
        source_counts=[dict(source_id=capture['source_binding']['source_id'], row_count=len(rows))],
        identity_origin=dict(ISIN='CALLER_BINDING', TRADE_CODE='CALLER_BINDING'))
    return selection, tokens


def verify_capture(raw_bytes, *, capture, retained, profile_bytes, schema_bytes):
    """Verify explicit retained inputs; no file, network, clock or authority I/O."""
    _require(runtime.SCHEMA_ID == 'mds.c2.runtime' and runtime.SCHEMA_VERSION == 1 and
             runtime.SCHEMA_SHA256 == U_SCHEMA_SHA256, 'U_INTERFACE')
    runtime.schema_digest(schema_bytes)
    try:
        _domain(capture)
        _domain(retained)
    except RecursionError:
        _require(False, 'SHAPE')
    _keys(retained, ('capture', 'profile_sha256', 'evaluation_as_of', 'acquisition_window', 'requirements', 'evidence'))
    _capture(capture)
    _capture(retained['capture'])
    _digest(retained['profile_sha256'])
    _require(capture == retained['capture'], 'CAPTURE_BINDING')
    profile_digest = _profile(profile_bytes, retained, capture)
    source = capture['source_binding']
    _require(source['source_kind'] in {'MOEX_ISS', 'SYNTHETIC_ONLY'}, 'UNSUPPORTED_MAPPING')
    _require(type(raw_bytes) is bytes, 'SHAPE')
    _require(len(raw_bytes) == source['raw_length'] and hashlib.sha256(raw_bytes).hexdigest() == source['raw_sha256'], 'RAW_IDENTITY')
    _require(capture['complete'], 'INCOMPLETE_CAPTURE')
    window = retained['acquisition_window']
    _keys(window, ('start', 'end'))
    start, end, acquired, evaluated = (_utc(window['start']), _utc(window['end']),
        _utc(source['acquired_at']), _utc(retained['evaluation_as_of']))
    _require(start <= acquired <= end and acquired <= evaluated, 'ACQUISITION')
    observations, origin, semantics, coverage = _evidence(retained, capture)
    # Raw identity, metadata и profile проверены ДО source decoder. Wire U
    # запрещает nonfinite JSON, но source token 1e309 остаётся стандартным JSON:
    # его decoded infinity фиксируется как наблюдение, не исправляется.
    document, text = _json(raw_bytes, 'RAW_JSON', source=True)
    selection, tokens = _selection(document, text, capture)
    return dict(u_schema_sha256=U_SCHEMA_SHA256, profile_sha256=profile_digest,
        context=copy.deepcopy(capture['context']), candidate_release_id=capture['candidate_release_id'],
        capture_id=capture['capture_id'], source_binding=copy.deepcopy(source), raw_integrity='VERIFIED',
        selection=selection, tokens=tokens, origin_status=origin, source_semantics_status=semantics,
        coverage_status=coverage, evidence_observations=observations,
        custody_status='NOT_VERIFIED', release_admission='NOT_ASSESSED')
