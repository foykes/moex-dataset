"""Pure candidate assessment over the frozen U codec and retained caller proofs."""

# %%
from datetime import date
import hashlib
import json
import re

import dividend_runtime_contract as runtime


U_SCHEMA_SHA256 = 'a0daced78e661713d75a5f768df5a354f5caf3c82a9f0a76a7ca53f10ad19619'
CHECK_IDS = ('IDENTITY_SCHEMA', 'LOCAL_PROFILE', 'SOURCE_SEMANTICS', 'RAW_PROVENANCE',
             'COVERAGE', 'RUN_ISOLATION', 'CANONICAL_CONTENT', 'CANONICAL_BINDINGS',
             'REQUIRED_NONEMPTY')
COLUMNS = ['ISIN', 'TRADE_CODE', 'dt', 'value', 'currency']
SOURCE_OBLIGATIONS = ('SOURCE_SEMANTICS', 'DATE_SEMANTICS', 'AMOUNT_UNITS',
                      'AMOUNT_GROSS_NET', 'AMOUNT_SIGN', 'CURRENCY_SEMANTICS',
                      'RAW_PROVENANCE', 'COVERAGE')
GLOBAL_OBLIGATIONS = ('CANDIDATE_INPUT', 'COVERAGE_MEMBERSHIP', 'RUN_ISOLATION')


class QualityInputError(ValueError):
    """Only a closed code; input values and arbitrary repr are never exposed."""


def _require(condition, code):
    if not condition:
        raise QualityInputError('C2_QUALITY_INPUT_INVALID:' + code) from None


def _keys(value, names):
    _require(type(value) is dict and set(value) == set(names), 'BASIS_SHAPE')


def _alias(value, nullable=False):
    _require(nullable and value is None or type(value) is str and
             re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}', value) is not None,
             'BASIS_SHAPE')


def _digest(value):
    _require(value is None or type(value) is str and
             re.fullmatch(r'[0-9a-f]{64}', value) is not None, 'BASIS_SHAPE')


def _count(value):
    _require(value is None or type(value) is int and value >= 0, 'BASIS_SHAPE')


def _hash(value):
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True,
                     separators=(',', ':'), allow_nan=False).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


def _validate_basis(basis, message, trusted):
    # Этот отдельный аргумент удерживает coordinator/E. Нельзя подставить сюда
    # строки, lineage или proof map, восстановленные из проверяемого payload.
    _keys(basis, ('basis_version', 'context', 'candidate_release_id', 'retained_rows',
                  'retained_bindings', 'source_counts', 'expected_source_ids', 'proofs'))
    _require(type(basis['basis_version']) is int and basis['basis_version'] == 1, 'BASIS_VERSION')
    # In-process caller также обязан передать обычные primitives. Проверяем их
    # до equality: произвольный __eq__ не может выдать себя за retained identity.
    _keys(basis['context'], ('run_id', 'snapshot_id', 'dataset_id', 'as_of'))
    for value in basis['context'].values():
        _require(type(value) is str, 'BASIS_SHAPE')
    _alias(basis['candidate_release_id'])
    _require(basis['context'] == trusted['context'] and
             basis['candidate_release_id'] == trusted['candidate_release_id'], 'BASIS_CONTEXT')
    sources = {source['source_id']: source for source in trusted['source_bindings']}
    rows, bindings = basis['retained_rows'], basis['retained_bindings']
    if rows is not None:
        _require(type(rows) is list, 'BASIS_SHAPE')
        for row in rows:
            _require(type(row) is list and len(row) == 5, 'BASIS_SHAPE')
            for cell in row:
                runtime.encode_cell(cell)
    if bindings is not None:
        _require(type(bindings) is list, 'BASIS_SHAPE')
        for ordinal, binding in enumerate(bindings):
            _keys(binding, ('canonical_ordinal', 'source_id', 'source_ordinal'))
            _require(type(binding['canonical_ordinal']) is int and
                     binding['canonical_ordinal'] == ordinal, 'BASIS_SELECTION')
            _alias(binding['source_id'])
            _require(binding['source_id'] in sources, 'BASIS_SOURCE')
            _count(binding['source_ordinal'])
            _require(binding['source_ordinal'] is not None, 'BASIS_SELECTION')
        if rows is not None:
            _require(len(rows) == len(bindings), 'BASIS_SELECTION')
    counts = basis['source_counts']
    _require(type(counts) is list, 'BASIS_SHAPE')
    count_ids = []
    for item in counts:
        _keys(item, ('source_id', 'row_count'))
        _alias(item['source_id'])
        _count(item['row_count'])
        count_ids.append(item['source_id'])
    _require(len(count_ids) == len(set(count_ids)) and set(count_ids) == set(sources), 'BASIS_INVENTORY')
    expected = basis['expected_source_ids']
    if expected is not None:
        _require(type(expected) is list, 'BASIS_SHAPE')
        for identity in expected:
            _alias(identity)
        _require(len(expected) == len(set(expected)), 'BASIS_INVENTORY')

    records = basis['proofs']
    _require(type(records) is list, 'BASIS_SHAPE')
    proofs, refs = {}, []
    evidence_ids = {record['evidence_id'] for record in message['evidence']}
    for record in records:
        _keys(record, ('obligation', 'source_id', 'acquisition_id', 'evidence_ref',
                       'subject_sha256', 'assessor_id', 'profile_sha256'))
        _alias(record['obligation'])
        for name in ('source_id', 'acquisition_id', 'evidence_ref', 'assessor_id'):
            _alias(record[name], nullable=True)
        for name in ('subject_sha256', 'profile_sha256'):
            _digest(record[name])
        source_id = record['source_id']
        if source_id is not None:
            _require(source_id in sources and record['acquisition_id'] ==
                     sources[source_id]['acquisition_id'], 'BASIS_SOURCE')
        else:
            _require(record['acquisition_id'] is None, 'BASIS_SOURCE')
        key = (record['obligation'], source_id)
        _require(key not in proofs, 'BASIS_INVENTORY')
        proofs[key] = record
        ref = record['evidence_ref']
        _require(ref is None or ref in evidence_ids, 'BASIS_REFERENCE')
        if ref is not None:
            refs.append(ref)
    inventory = {(name, None) for name in GLOBAL_OBLIGATIONS}
    inventory.update((name, source_id) for source_id in sources for name in SOURCE_OBLIGATIONS)
    _require(set(proofs) == inventory and len(refs) == len(set(refs)), 'BASIS_INVENTORY')
    claims = message['assessment']
    ids = [check['check_id'] for check in claims['checks']]
    _require((claims['status'] == 'NOT_RUN' and not ids) or
             (claims['status'] != 'NOT_RUN' and len(ids) == 9 and set(ids) == set(CHECK_IDS)),
             'CHECK_INVENTORY')
    return sources, proofs


def _proof(record, evidence, *, subject=None):
    ref = record['evidence_ref']
    if ref is None:
        return False
    actual = evidence[ref]
    # VERIFIED — недостаточно без совпадения independently retained subject,
    # source, E identity/profile и явной действительности на evaluation_as_of.
    # Саму физическую custody/официальность проверяет E, не этот pure assessor.
    return (record['subject_sha256'] is not None and record['profile_sha256'] is not None
            and record['assessor_id'] is not None and
            (subject is None or record['subject_sha256'] == subject) and
            actual['status'] == 'VERIFIED' and actual['valid_until'] is not None and
            actual['source_id'] == record['source_id'] and
            actual['subject_sha256'] == record['subject_sha256'] and
            actual['assessor_role'] == 'E_VERIFIER' and
            actual['assessor_id'] == record['assessor_id'] and
            actual['profile_sha256'] == record['profile_sha256'])


def _local_findings(rows):
    findings = []
    for ordinal, row in enumerate(rows):
        dt, amount, currency = row[2:]
        rule, field = None, 'dt'
        if dt['kind'] != 'TEXT':
            rule = 'DATE_TYPE'
        elif re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', dt['value']) is None:
            rule = 'DATE_SHAPE'
        else:
            value = dt['value']
            try:
                date(int(value[:4]), int(value[5:7]), int(value[8:]))
            except ValueError:
                rule = 'DATE_CALENDAR'
        if rule is None:
            field = 'value'
            if amount['kind'] == 'NONFINITE':
                rule = 'VALUE_NONFINITE'
            elif amount['kind'] not in ('INT', 'FLOAT64'):
                rule = 'VALUE_TYPE'
        if rule is None:
            field = 'currency'
            if currency['kind'] != 'TEXT':
                rule = 'CURRENCY_TYPE'
            elif not currency['value'].strip():
                rule = 'CURRENCY_BLANK'
            elif re.search(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', currency['value']):
                rule = 'CURRENCY_CONTROL'
        if rule is not None:
            findings.append({'row': ordinal, 'field': field, 'rule': rule})
    return findings


def assess(raw, *, schema_bytes, trusted, basis):
    """Assess one candidate; inputs are explicit, result confers no authority."""
    _require(runtime.SCHEMA_ID == 'mds.c2.runtime' and runtime.SCHEMA_VERSION == 1 and
             runtime.SCHEMA_SHA256 == U_SCHEMA_SHA256, 'U_INTERFACE')
    message = runtime.decode(raw, schema_bytes=schema_bytes, trusted=trusted)
    sources, proofs = _validate_basis(basis, message, trusted)
    evidence = {record['evidence_id']: record for record in message['evidence']}
    selected_rows, selected_bindings = basis['retained_rows'], basis['retained_bindings']
    anchor = proofs['CANDIDATE_INPUT', None]
    anchor_ok = selected_rows is not None and selected_bindings is not None and _proof(
        anchor, evidence, subject=_hash(['mds.c2.quality.input/1', basis['context'],
            basis['candidate_release_id'], COLUMNS, selected_rows, selected_bindings,
            trusted['source_bindings']]))
    computed = {name: 'NOT_VERIFIED' for name in CHECK_IDS}
    sufficient = {name: anchor_ok for name in CHECK_IDS}
    consulted = {name: [anchor['evidence_ref']] if anchor['evidence_ref'] is not None else []
                 for name in CHECK_IDS}
    count_values = [item['row_count'] for item in basis['source_counts']]
    source_rows = sum(count_values) if count_values and all(v is not None for v in count_values) else None
    frame = message['canonical']
    counts = dict(rows_checked=None, accepted_rows=None, quarantined_rows=None,
                  locally_valid_rows=None, locally_invalid_rows=None, source_rows=source_rows)
    findings = []
    if frame is not None:
        rows, bindings = frame['rows'], frame['bindings']
        findings = _local_findings(rows)
        # accepted_rows здесь — измеренные строки уже переданного canonical,
        # а не разрешение выпуска. Q никого не удаляет/не quarantines.
        counts.update(rows_checked=len(rows), accepted_rows=len(rows), quarantined_rows=0,
                      locally_valid_rows=len(rows) - len(findings), locally_invalid_rows=len(findings))
        computed['IDENTITY_SCHEMA'] = 'PASS'  # exact U context/columns/typed cells
        computed['LOCAL_PROFILE'] = 'FAIL' if findings else 'PASS'
        computed['REQUIRED_NONEMPTY'] = 'PASS' if rows else 'FAIL'
        if selected_rows is not None:
            computed['CANONICAL_CONTENT'] = 'PASS' if rows == selected_rows else 'FAIL'
            if source_rows is None and computed['CANONICAL_CONTENT'] == 'PASS':
                computed['CANONICAL_CONTENT'] = 'NOT_VERIFIED'
            elif source_rows is not None and len(rows) != source_rows:
                computed['CANONICAL_CONTENT'] = 'FAIL'
            # Равный общий итог не доказывает сохранность каждой response.
            # Известное расхождение A=1/B=2 против lineage A=2/B=1 — FAIL,
            # даже если другой source count неизвестен. Ordinal не становится
            # natural key; ни уникальность, ни непрерывный диапазон не вводятся.
            for source_count in basis['source_counts']:
                measured = source_count['row_count']
                actual = sum(binding['source_id'] == source_count['source_id']
                             for binding in bindings)
                if measured is not None and measured != actual:
                    computed['CANONICAL_CONTENT'] = 'FAIL'
        if selected_rows is not None and selected_bindings is not None:
            # Сравнивается ordered row-to-source association, не два множества.
            # Замена row вместе с self-consistent hashes не меняет retained basis.
            computed['CANONICAL_BINDINGS'] = ('PASS' if rows == selected_rows and
                bindings == selected_bindings else 'FAIL')

    official_sources = bool(sources) and all(source['source_kind'] != 'SYNTHETIC_ONLY'
                                            for source in sources.values())
    source_ok = {name: official_sources for name in ('SOURCE_SEMANTICS', 'RAW_PROVENANCE', 'COVERAGE')}
    for source_id, source in sources.items():
        for check, obligations in (
                ('SOURCE_SEMANTICS', SOURCE_OBLIGATIONS[:6]),
                ('RAW_PROVENANCE', ('RAW_PROVENANCE',)), ('COVERAGE', ('COVERAGE',))):
            for name in obligations:
                record = proofs[name, source_id]
                if record['evidence_ref'] is not None:
                    consulted[check].append(record['evidence_ref'])
                ok = _proof(record, evidence, subject=source['raw_sha256'] if name == 'RAW_PROVENANCE' else None)
                if name == 'SOURCE_SEMANTICS':
                    ok = ok and record['evidence_ref'] == source['contract_evidence_ref']
                if name == 'COVERAGE':
                    ok = ok and record['evidence_ref'] == source['coverage_evidence_ref']
                source_ok[check] = source_ok[check] and ok
    for check in source_ok:
        computed[check] = 'PASS' if source_ok[check] else 'NOT_VERIFIED'

    membership = proofs['COVERAGE_MEMBERSHIP', None]
    expected = basis['expected_source_ids']
    if membership['evidence_ref'] is not None:
        consulted['COVERAGE'].append(membership['evidence_ref'])
    membership_ok = expected is not None and _proof(membership, evidence, subject=_hash([
        'mds.c2.quality.membership/1', basis['context'], basis['candidate_release_id'], expected]))
    if not membership_ok:
        computed['COVERAGE'] = 'NOT_VERIFIED'
    elif set(expected) != set(sources):
        computed['COVERAGE'] = 'FAIL'
    isolation = proofs['RUN_ISOLATION', None]
    if isolation['evidence_ref'] is not None:
        consulted['RUN_ISOLATION'].append(isolation['evidence_ref'])
    computed['RUN_ISOLATION'] = 'PASS' if _proof(isolation, evidence) else 'NOT_VERIFIED'

    claims = {check['check_id']: check for check in message['assessment']['checks']}
    checks = []
    for name in CHECK_IDS:
        status = computed[name]
        reason = None
        if status == 'PASS' and not sufficient[name]:
            status = 'NOT_VERIFIED'
        # Входные FAIL/uncertainty не стираются новым механическим вычислением.
        # Nominal PASS не заменяет ни retained anchor, ни внешние source proofs.
        claim = claims.get(name)
        if claim is not None and (claim['status'] == 'FAIL' or
                                  claim['status'] == 'NOT_VERIFIED' and status != 'FAIL'):
            status, reason = claim['status'], claim['reason']
        if status != 'PASS' and reason is None:
            reason = ('LOCAL_INVALID' if status == 'FAIL' else
                      'MISSING_EVIDENCE' if not consulted[name] else 'UNVERIFIED_EVIDENCE')
        checks.append(dict(check_id=name, status=status,
                           evidence_ref=anchor['evidence_ref'] if status == 'PASS' else None, reason=reason))
    statuses = [check['status'] for check in checks]
    observed = message['assessment']['status']
    status = ('FAIL' if 'FAIL' in statuses or observed == 'FAIL' else
              'NEEDS_DECISION' if observed == 'NEEDS_DECISION' else
              'NOT_VERIFIED' if 'NOT_VERIFIED' in statuses or observed == 'NOT_VERIFIED' else 'PASS')
    return dict(policy_id='C2_Q_V1', u_schema_sha256=U_SCHEMA_SHA256,
                context=dict(message['context']), candidate_release_id=message['candidate_release_id'],
                assessment=dict(status=status, checks=checks), observed_assessment=message['assessment'],
                counts=counts, computed_checks=computed, local_findings=findings,
                basis_evidence_refs=consulted,
                blocking_check_ids=[check['check_id'] for check in checks if check['status'] != 'PASS'],
                precision_status='NOT_VERIFIED')
