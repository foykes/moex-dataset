"""Independent P1 admission, lineage and numerical regressions; offline only."""

import base64
import binascii
import copy
import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import shutil
import types
import zlib

import numpy as np
import pandas as pd
import pytest
import talib


ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = ROOT / 'tests/fixtures/b2/calculator_cases.json'
RAW_COLUMNS = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end']
KEY_FIELDS = ['engine', 'market', 'source_scope', 'board', 'raw_instrument_id', 'interval', 'begin']
CANARY = 'B2_P1_SYNTHETIC_PRIVATE_CANARY'
LEGACY_BASE = '223be5ebd552e3369d4b6b2488c6838798711458'
LEGACY_BLOB = 'ec1c1f194980e66c0e451129b52d01df51a4653d'
LEGACY_BYTE_LENGTH = 46632
LEGACY_SHA256 = '0168008093fed9104045b291cc893f552294bcba497ea7a7ea90de01595bf565'
LEGACY_COMPRESSED_LENGTH = 9042
LEGACY_COMPRESSED_SHA256 = 'd1077ee504b74dc7108de114e756109ca0a14a13dcd917943bad6079ee85212f'
LEGACY_ENCODED_LENGTH = 12056


@pytest.fixture
def calculator_cases():
    return json.loads(FIXTURE_PATH.read_text(encoding='utf-8'))


@pytest.fixture
def candidate():
    import tech
    return tech


@pytest.fixture
def quality_cases():
    return json.loads((ROOT / 'tests/fixtures/b2/cases.json').read_text(encoding='utf-8'))


def _sequence(cases, *, ticker=None, bars=None, price_shift=0):
    """Independent integer source with changing direction, range and volume."""
    spec = cases['sequence']
    start = datetime.datetime.strptime(spec['start'], '%Y-%m-%d %H:%M:%S')
    rows = []
    for ordinal in range(spec['bars'] if bars is None else bars):
        close = 100 + (ordinal * 7) % 37 + (ordinal // 11) % 9 + price_shift
        opening = close + ordinal % 5 - 2
        high = max(opening, close) + 3 + ordinal % 3
        low = min(opening, close) - 2 - ordinal % 4
        volume = 1000 + ((ordinal * 13) % 97) * 10
        begin = start + datetime.timedelta(days=ordinal)
        end = begin + datetime.timedelta(hours=6)
        rows.append([opening, close, high, low, close * volume, volume,
                     begin.strftime('%Y-%m-%d %H:%M:%S'),
                     end.strftime('%Y-%m-%d %H:%M:%S'),
                     spec['ticker'] if ticker is None else ticker])
    return pd.DataFrame(rows, columns=RAW_COLUMNS + ['ticker'])


def _logical(payload, schema):
    # Literal F0 miniature encoding, independent of data_quality/tech internals.
    try:
        content = json.dumps(payload, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
        digest = hashlib.sha256(content.encode('utf-8')).hexdigest()
    except (TypeError, ValueError):
        # This cannot make unsupported normalization verified.
        digest = '0' * 64
    return dict(basis='LOGICAL_ROWS_SHA256', value=digest, schema_version=schema)


def _key_array(key):
    return [key[field] for field in KEY_FIELDS]


def _capture(quality_cases, source_frames, *, selection=None, extras=None):
    """Freeze new synthetic producer authority before any negative mutation."""
    template = quality_cases['e19']
    spec = copy.deepcopy(template['spec'])
    metadata = copy.deepcopy(template['metadata'])
    schema = 'B2_P1_INDEPENDENT_RAW9'
    spec['schema_version'] = metadata['schema_version'] = schema
    columns = RAW_COLUMNS + ['ticker']
    original_columns = {column['name']: column for column in spec['columns']}
    spec['columns'] = [copy.deepcopy(original_columns[name]) for name in columns]
    records, sources, lookup = [], [], {}
    for record_number, source_frame in enumerate(source_frames):
        record = copy.deepcopy(template['metadata']['identity_records'][0])
        record_id = 'B2_P1_RECORD_' + str(record_number)
        provenance_ref = 'B2/P1/source-' + str(record_number)
        record['record_id'] = record_id
        record['identity']['raw_instrument_id'] = 'B2_P1_RAW_' + str(record_number)
        record['identity']['display_ticker'] = str(source_frame.iloc[0]['ticker'])
        record['interval'] = 24
        record['provenance'][0]['evidence_ref'] = provenance_ref
        record['coverage_evidence'] = dict(expected_rows=len(source_frame),
            evidence_ref='B2/P1/membership-' + str(record_number),
            reason='Independently captured synthetic source-key occurrence inventory.')
        record['counts'].update(rows_received=len(source_frame), rows_accepted=None,
                                rows_quarantined=0, pages=1, instruments=1)
        def fixed_bounds():
            return {side: dict(value=value, state='KNOWN', precision='SECOND',
                               timezone='Europe/Moscow', evidence_ref='B2/P1/fixed-synthetic-scope', reason=None)
                    for side, value in (('start', min(source_frame['begin'])),
                                        ('end', max(source_frame['end'])))}
        record['requested_bounds'] = fixed_bounds()
        record['available_bounds'] = fixed_bounds()
        record['actual_bounds'] = fixed_bounds()
        source_rows = []
        for raw_ordinal, row in enumerate(source_frame[RAW_COLUMNS].itertuples(index=False, name=None)):
            values = copy.deepcopy(list(row))
            key = {field: copy.deepcopy(record['identity'][field]) for field in KEY_FIELDS[:5]}
            key.update(interval=24, begin=values[6])
            raw = dict(raw_row_ordinal=raw_ordinal, raw_key=key, columns=list(RAW_COLUMNS),
                       values=values, raw_row_fingerprint=_logical([_key_array(key), RAW_COLUMNS, values], schema))
            source_rows.append(raw)
            lookup[(record_number, raw_ordinal)] = raw
        records.append(record)
        sources.append(dict(identity_record_id=record_id, provenance_ref=provenance_ref, rows=source_rows))
    if selection is None:
        selection = [(record_number, raw_ordinal)
                     for record_number, source in enumerate(sources)
                     for raw_ordinal in range(len(source['rows']))]
    selection = list(selection)
    frame_rows, entries = [], []
    for ordinal, (record_number, raw_ordinal) in enumerate(selection):
        raw = lookup[(record_number, raw_ordinal)]
        record, source = records[record_number], sources[record_number]
        frame_rows.append(copy.deepcopy(raw['values']) + [record['identity']['display_ticker']])
        entries.append(dict(ordinal=ordinal, identity_record_id=record['record_id'],
            begin=raw['raw_key']['begin'],
            source_row_ref=dict(provenance_ref=source['provenance_ref'], raw_row_ordinal=raw_ordinal,
                                raw_key=copy.deepcopy(raw['raw_key']),
                                raw_row_fingerprint=copy.deepcopy(raw['raw_row_fingerprint']))))
    if extras:
        for name, values in extras.items():
            assert name not in columns and len(values) == len(frame_rows)
            columns.append(name)
            spec['columns'].append(dict(name=name, logical_type='string', nullable=True,
                unit=None, unit_status='UNKNOWN', evidence_ref='B2/P1/fixed-extra-schema',
                null_reason='Synthetic extra column, not a production schema amendment.'))
            for row, value in zip(frame_rows, values):
                row.append(copy.deepcopy(value))
    payload = [schema, columns, frame_rows]
    identity_material = [payload, [[record['record_id'], record['identity']] for record in records],
                         [[row['values'] for row in source['rows']] for source in sources], selection]
    metadata['snapshot_id'] = 'B2_P1_CAPTURE_' + _logical(identity_material, schema)['value'][:24]
    frame_fingerprint = _logical(payload, schema)
    entry_arrays = [[entry['ordinal'], entry['identity_record_id'], entry['begin'],
        entry['source_row_ref']['provenance_ref'], entry['source_row_ref']['raw_row_ordinal'],
        _key_array(entry['source_row_ref']['raw_key']), entry['source_row_ref']['raw_row_fingerprint']['value']]
        for entry in entries]
    binding_fingerprint = _logical([metadata['snapshot_id'], metadata['dataset_id'], schema,
                                    frame_fingerprint['value'], entry_arrays], schema)
    authority = dict(snapshot_id=metadata['snapshot_id'], dataset_id=metadata['dataset_id'],
        schema_version=schema, frame_fingerprint=frame_fingerprint, entries=entries,
        binding_fingerprint=binding_fingerprint, evidence_ref='B2/P1/immutable-selection')
    bindings = copy.deepcopy(authority)
    bindings['row_count'] = len(entries)
    bindings['binding_evidence_ref'] = bindings.pop('evidence_ref')
    metadata['identity_records'] = records
    metadata['row_bindings'] = bindings
    metadata['requested_bounds'] = copy.deepcopy(records[0]['requested_bounds'])
    metadata['actual_bounds'] = copy.deepcopy(records[0]['actual_bounds'])
    for field in ('requested_bounds', 'actual_bounds'):
        metadata[field]['start']['value'] = min(record[field]['start']['value'] for record in records)
        metadata[field]['end']['value'] = max(record[field]['end']['value'] for record in records)
    metadata['counts'].update(rows_received=sum(len(source['rows']) for source in sources),
        rows_accepted=len(entries), rows_quarantined=0, pages=len(sources), instruments=len(records))
    for record_number, record in enumerate(records):
        record['counts']['rows_accepted'] = sum(number == record_number for number, _ in selection)
    membership = [dict(identity_record_id=record['record_id'],
        evidence_ref=record['coverage_evidence']['evidence_ref'],
        identity=copy.deepcopy(record['identity']), interval=24,
        requested_bounds=copy.deepcopy(record['requested_bounds']),
        expected_raw_keys=[copy.deepcopy(row['raw_key']) for row in source['rows']])
        for record, source in zip(records, sources)]
    evidence = dict(serialization_profile='ILLUSTRATIVE_JSON_ARRAYS_V1',
        frame_payload=payload, retained_sources=sources, source_membership=membership,
        producer_selection=copy.deepcopy(authority))
    frame = pd.DataFrame(copy.deepcopy(frame_rows), columns=columns, dtype=object)
    return frame, dict(spec=spec, metadata=metadata, retained_evidence=evidence)


def _fixture_case(case):
    payload = case['retained_evidence']['frame_payload']
    return pd.DataFrame(copy.deepcopy(payload[2]), columns=payload[1], dtype=object), copy.deepcopy(case)


def _calculate(candidate, frame, case, **kwargs):
    return candidate.calculate_candles(frame, spec=case['spec'], metadata=case['metadata'],
                                      retained_evidence=case['retained_evidence'], **kwargs)


def _snapshot(frame, case):
    # DataFrame.copy/deepcopy alone does not copy Python objects stored in cells.
    frozen = frame.copy(deep=True)
    for column_number in range(len(frame.columns)):
        if frame.dtypes.iloc[column_number] == object:
            for row_number in range(len(frame)):
                frozen.iat[row_number, column_number] = copy.deepcopy(frame.iat[row_number, column_number])
    frozen.attrs = copy.deepcopy(frame.attrs)
    return frozen, copy.deepcopy(case)


def _unchanged(frame, case, snapshot):
    pd.testing.assert_frame_equal(frame, snapshot[0], check_exact=True)
    assert frame.attrs == snapshot[0].attrs
    assert case == snapshot[1]


def _blockers(result):
    return {item['reason'] for item in result['blockers']}


def _assert_blocked(result):
    assert result['status'] == 'BLOCKED'
    assert result['effective_groups'] == []
    assert result['production_admission'] == 'NOT_ENABLED'
    assert all(row['disposition'] != 'EFFECTIVE' for row in result['candidate_row_map'])


def _forbid_ta(monkeypatch, cases):
    calls = []

    def unexpected(*args, **kwargs):
        calls.append(True)
        pytest.fail('A native TA function ran before the complete admission/conversion gate')

    for call in cases['native_calls']:
        monkeypatch.setattr(talib, call['function'], unexpected)
    return calls


def _legacy_result(module, frame, monkeypatch, *, suffix='csv'):
    outputs = []
    monkeypatch.setattr(module, 'current_path', 'B2_SYNTHETIC_OFFLINE_ROOT')
    reader = lambda *args, **kwargs: frame.copy(deep=True)
    monkeypatch.setattr(module.pd, 'read_csv', reader)
    monkeypatch.setattr(module.pd, 'read_excel', reader)

    def capture(output, *args, **kwargs):
        assert kwargs.get('index') is False
        outputs.append(output.copy(deep=True))

    monkeypatch.setattr(module.pd.DataFrame, 'to_csv', capture)
    monkeypatch.setattr(module.pd.DataFrame, 'to_excel', capture)
    module.calculator('synthetic.' + suffix)
    assert len(outputs) == 1
    return outputs[0]


def _decode_pinned_reference(fixture):
    """Verify frozen historical bytes without Git, candidate source or fallback."""
    failure = 'PINNED_LEGACY_PREPARATION_FAILED'
    if (type(fixture) is not dict or fixture.get('base_sha') != LEGACY_BASE
            or fixture.get('source_path') != 'tech.py' or fixture.get('git_blob') != LEGACY_BLOB):
        raise ValueError(failure)
    frozen = fixture.get('frozen_reference')
    expected = dict(format='BASE64_ZLIB_ORIGINAL_GIT_BLOB_V1',
        source_commit=LEGACY_BASE, source_path='tech.py', git_blob=LEGACY_BLOB,
        raw_byte_length=LEGACY_BYTE_LENGTH, raw_sha256=LEGACY_SHA256,
        compressed_byte_length=LEGACY_COMPRESSED_LENGTH,
        compressed_sha256=LEGACY_COMPRESSED_SHA256)
    if type(frozen) is not dict or any(type(frozen.get(key)) is not type(value)
                                      or frozen[key] != value for key, value in expected.items()):
        raise ValueError(failure)
    encoded = frozen.get('payload')
    if type(encoded) is not str or len(encoded) != LEGACY_ENCODED_LENGTH or not encoded.isascii():
        raise ValueError(failure)
    try:
        compressed = base64.b64decode(encoded, validate=True)
        if (len(compressed) != LEGACY_COMPRESSED_LENGTH
                or hashlib.sha256(compressed).hexdigest() != LEGACY_COMPRESSED_SHA256):
            raise ValueError(failure)
        decoder = zlib.decompressobj()
        raw = decoder.decompress(compressed, LEGACY_BYTE_LENGTH + 1)
    except (binascii.Error, zlib.error) as error:
        raise ValueError(failure) from error
    if (not decoder.eof or decoder.unused_data or decoder.unconsumed_tail
            or len(raw) != LEGACY_BYTE_LENGTH or hashlib.sha256(raw).hexdigest() != LEGACY_SHA256):
        raise ValueError(failure)
    actual_blob = hashlib.sha1(b'blob ' + str(len(raw)).encode('ascii') + b'\0' + raw).hexdigest()
    if actual_blob != LEGACY_BLOB:
        raise ValueError(failure)
    return raw


def _import_pinned_legacy(raw):
    module = types.ModuleType('b2_pinned_legacy')
    module.__file__ = 'B2_PINNED_BASE_tech.py'
    exec(compile(raw, module.__file__, 'exec'), module.__dict__)
    return module


def _load_pinned_reference(fixture):
    # Validation completes before any historical source is compiled/imported.
    raw = _decode_pinned_reference(fixture)
    return _import_pinned_legacy(raw)


@pytest.fixture(scope='module')
def pinned_legacy():
    fixture = json.loads(FIXTURE_PATH.read_text(encoding='utf-8'))['legacy_negative_control']
    return _load_pinned_reference(fixture)


def test_frozen_reference_has_literal_original_bytes(calculator_cases):
    raw = _decode_pinned_reference(calculator_cases['legacy_negative_control'])
    assert len(raw) == 46632
    assert hashlib.sha256(raw).hexdigest() == '0168008093fed9104045b291cc893f552294bcba497ea7a7ea90de01595bf565'
    assert hashlib.sha1(b'blob 46632\0' + raw).hexdigest() == 'ec1c1f194980e66c0e451129b52d01df51a4653d'
    assert b'def _calculator_impl(' in raw
    assert b'def calculate_candles(' not in raw


def test_reference_preparation_needs_no_git_candidate_source_or_network(calculator_cases, monkeypatch):
    import builtins
    import socket
    import subprocess

    def forbidden(*args, **kwargs):
        pytest.fail('Frozen reference preparation attempted an external dependency or fallback')

    monkeypatch.setattr(shutil, 'which', lambda *args, **kwargs: None)
    monkeypatch.setattr(subprocess, 'run', forbidden)
    monkeypatch.setattr(subprocess, 'Popen', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    monkeypatch.setattr(socket, 'getaddrinfo', forbidden)
    original_open = builtins.open
    original_read_text = Path.read_text
    original_read_bytes = Path.read_bytes

    def candidate_path(path):
        return isinstance(path, (str, Path)) and Path(path).name.casefold() == 'tech.py'

    def checked_open(path, *args, **kwargs):
        if candidate_path(path):
            forbidden()
        return original_open(path, *args, **kwargs)

    def checked_read_text(path, *args, **kwargs):
        if candidate_path(path):
            forbidden()
        return original_read_text(path, *args, **kwargs)

    def checked_read_bytes(path, *args, **kwargs):
        if candidate_path(path):
            forbidden()
        return original_read_bytes(path, *args, **kwargs)

    monkeypatch.setattr(builtins, 'open', checked_open)
    monkeypatch.setattr(Path, 'read_text', checked_read_text)
    monkeypatch.setattr(Path, 'read_bytes', checked_read_bytes)
    record = copy.deepcopy(calculator_cases['legacy_negative_control'])
    module = _load_pinned_reference(record)
    assert module.__file__ == 'B2_PINNED_BASE_tech.py'
    assert callable(module.calculator)
    assert not hasattr(module, 'calculate_candles')
    record.pop('frozen_reference')
    with pytest.raises(ValueError, match='PINNED_LEGACY_PREPARATION_FAILED'):
        _load_pinned_reference(record)


@pytest.mark.parametrize('damage', ['missing', 'invalid_base64', 'truncated', 'trailing',
                                  'replacement', 'wrong_blob', 'wrong_length', 'wrong_sha256',
                                  'wrong_format'])
def test_corrupt_frozen_reference_rejected_before_import(calculator_cases, monkeypatch, damage):
    record = copy.deepcopy(calculator_cases['legacy_negative_control'])
    frozen = record['frozen_reference']
    imports = []

    def forbidden_import(raw):
        imports.append(raw)
        pytest.fail('Unverified reference bytes reached historical import')

    monkeypatch.setitem(globals(), '_import_pinned_legacy', forbidden_import)
    if damage == 'missing':
        record.pop('frozen_reference')
    elif damage == 'invalid_base64':
        frozen['payload'] = '!' + frozen['payload'][1:]
    elif damage == 'truncated':
        compressed = base64.b64decode(frozen['payload'], validate=True)
        frozen['payload'] = base64.b64encode(compressed[:-1]).decode('ascii')
    elif damage == 'trailing':
        compressed = base64.b64decode(frozen['payload'], validate=True)
        frozen['payload'] = base64.b64encode(compressed + b'UNVERIFIED_TRAILER').decode('ascii')
    elif damage == 'replacement':
        # Synthetic untrusted replacement, never current candidate source.
        raw = b"raise RuntimeError('UNVERIFIED_REFERENCE_MUST_NOT_IMPORT')\n"
        frozen['payload'] = base64.b64encode(zlib.compress(raw, level=9)).decode('ascii')
        frozen['raw_byte_length'] = len(raw)
        frozen['raw_sha256'] = hashlib.sha256(raw).hexdigest()
        frozen['git_blob'] = hashlib.sha1(b'blob ' + str(len(raw)).encode('ascii') + b'\0' + raw).hexdigest()
    elif damage == 'wrong_blob':
        record['git_blob'] = frozen['git_blob'] = '0' * 40
    elif damage == 'wrong_length':
        frozen['raw_byte_length'] += 1
    elif damage == 'wrong_sha256':
        frozen['raw_sha256'] = '0' * 64
    else:
        frozen['format'] = 'CANDIDATE_DERIVED_OR_FALLBACK_PROFILE'
    with pytest.raises(ValueError, match='PINNED_LEGACY_PREPARATION_FAILED'):
        _load_pinned_reference(record)
    assert imports == []


@pytest.mark.parametrize('column', ['RSI14', 'STOCH_slowk', 'MFI', 'OBV'])
def test_pinned_legacy_has_order_defect(calculator_cases, pinned_legacy, monkeypatch, column):
    frame = _sequence(calculator_cases)
    permutation = [(ordinal * calculator_cases['sequence']['permutation_multiplier']) % len(frame)
                   for ordinal in range(len(frame))]
    assert sorted(permutation) == list(range(len(frame)))
    shuffled = frame.iloc[permutation].reset_index(drop=True)
    chronological = _legacy_result(pinned_legacy, frame, monkeypatch).set_index(['ticker', 'begin']).sort_index()
    permuted = _legacy_result(pinned_legacy, shuffled, monkeypatch).set_index(['ticker', 'begin']).sort_index()
    assert chronological.index.equals(permuted.index)
    assert chronological[column].notna().sum() > 200
    assert not np.allclose(chronological[column].to_numpy(), permuted[column].to_numpy(),
                           equal_nan=True, **calculator_cases['tolerance'])


@pytest.mark.parametrize('suffix', ['csv', 'xlsx'])
def test_legacy_wrapper_preserves_pinned_values_and_columns(candidate, calculator_cases, pinned_legacy,
                                                          monkeypatch, suffix):
    frame = _sequence(calculator_cases)
    before = frame.copy(deep=True)
    expected = _legacy_result(pinned_legacy, frame, monkeypatch, suffix=suffix)
    actual = _legacy_result(candidate, frame, monkeypatch, suffix=suffix)
    pd.testing.assert_frame_equal(frame, before, check_exact=True)
    pd.testing.assert_frame_equal(actual, expected, check_exact=True)


def test_complete_result_schema_and_raw_preservation(candidate, calculator_cases, quality_cases):
    frame, case = _capture(quality_cases, [_sequence(calculator_cases)])
    frame.index = pd.Index(['position-' + str(ordinal % 7) for ordinal in range(len(frame))], name='raw_locator')
    frame.attrs['synthetic_note'] = {'values': ['untouched']}
    before = _snapshot(frame, case)
    result = _calculate(candidate, frame, case)
    assert set(result) == {'status', 'assessment', 'candidate_rows', 'candidate_row_map',
                          'effective_groups', 'quarantine', 'blockers', 'production_admission', 'diagnostics'}
    assert result['status'] == 'CALCULATED_OFFLINE'
    assert result['production_admission'] == 'NOT_ENABLED'
    assert result['blockers'] == []
    pd.testing.assert_frame_equal(result['candidate_rows'], frame, check_exact=True)
    assert result['candidate_rows'] is not frame
    assert len(result['candidate_row_map']) == len(frame)
    assert [row['candidate_ordinal'] for row in result['candidate_row_map']] == list(range(len(frame)))
    assert [row['original_index'] for row in result['candidate_row_map']] == frame.index.tolist()
    assert all(row['disposition'] == 'EFFECTIVE' for row in result['candidate_row_map'])
    group = result['effective_groups'][0]
    assert group['identity'] == case['metadata']['identity_records'][0]['identity']
    assert group['candidate_ordinals'] == list(range(len(frame)))
    pd.testing.assert_frame_equal(group['effective_rows'], frame.reset_index(drop=True), check_exact=True)
    assert group['indicators'].index.equals(pd.RangeIndex(len(frame)))
    assert list(group['indicators'].columns) == calculator_cases['output_columns']
    assert result['assessment']['raw_assessment']['source_verification'] == 'NOT_VERIFIED'
    full = next(check for check in result['assessment']['quality_result']['checks']
                if check['check_id'] == 'FULL_DATASET_VALIDATION')
    assert full['status'] == 'NOT_VERIFIED'
    _unchanged(frame, case, before)


def test_independent_permutation_and_full_identity_isolation(candidate, calculator_cases, quality_cases):
    sources = [_sequence(calculator_cases, ticker='SHARED_DISPLAY'),
               _sequence(calculator_cases, ticker='SHARED_DISPLAY', price_shift=300)]
    frame, case = _capture(quality_cases, sources)
    size = len(sources[0])
    selection = [(record, (ordinal * 73) % size) for ordinal in range(size) for record in (1, 0)]
    shuffled, shuffled_case = _capture(quality_cases, sources, selection=selection)
    assert case['metadata']['snapshot_id'] != shuffled_case['metadata']['snapshot_id']
    baseline = _calculate(candidate, frame, case)
    actual = _calculate(candidate, shuffled, shuffled_case)
    assert baseline['status'] == actual['status'] == 'CALCULATED_OFFLINE'
    expected_groups = {group['identity_record_id']: group for group in baseline['effective_groups']}
    assert len(actual['effective_groups']) == 2
    for group in actual['effective_groups']:
        expected = expected_groups[group['identity_record_id']]
        assert group['identity'] == expected['identity']
        pd.testing.assert_frame_equal(group['effective_rows'], expected['effective_rows'], check_exact=True)
        pd.testing.assert_frame_equal(group['indicators'], expected['indicators'], check_exact=True)


@pytest.mark.parametrize('native_index', range(66))
def test_all_native_calls_have_independent_parameters_masks_and_dtypes(candidate, calculator_cases,
                                                                     quality_cases, native_index):
    frame, case = _capture(quality_cases, [_sequence(calculator_cases)])
    result = _calculate(candidate, frame, case)
    assert result['status'] == 'CALCULATED_OFFLINE'
    call = calculator_cases['native_calls'][native_index]
    arguments = [pd.Series(np.asarray(frame[column].tolist(), dtype=np.float64))
                 for column in call['inputs']]
    native = getattr(talib, call['function'])(*arguments, **call['parameters'])
    expected_outputs = native if isinstance(native, tuple) else (native,)
    assert len(expected_outputs) == len(call['outputs'])
    indicators = result['effective_groups'][0]['indicators']
    for column, expected in zip(call['outputs'], expected_outputs):
        expected = np.asarray(expected)
        actual = indicators[column].to_numpy()
        assert len(actual) == len(expected) == len(frame)
        assert actual.dtype == expected.dtype, column
        np.testing.assert_array_equal(pd.isna(actual), pd.isna(expected), err_msg=column)
        np.testing.assert_allclose(actual, expected, equal_nan=True,
                                   err_msg=column, **calculator_cases['tolerance'])


def test_native_call_arguments_and_order_are_frozen(candidate, calculator_cases, quality_cases, monkeypatch):
    frame, case = _capture(quality_cases, [_sequence(calculator_cases)])
    observations = []
    for call in calculator_cases['native_calls']:
        function = getattr(talib, call['function'])

        def record(*args, _function=function, _call=call, **kwargs):
            observations.append((_call['function'], len(args), copy.deepcopy(kwargs)))
            for actual, column in zip(args, _call['inputs']):
                np.testing.assert_array_equal(np.asarray(actual), frame[column].to_numpy(dtype=np.float64))
                assert np.asarray(actual).dtype == np.dtype('float64')
            return _function(*args, **kwargs)

        monkeypatch.setattr(talib, call['function'], record)
    result = _calculate(candidate, frame, case)
    assert result['status'] == 'CALCULATED_OFFLINE'
    assert observations == [(call['function'], len(call['inputs']), call['parameters'])
                            for call in calculator_cases['native_calls']]
    assert len(observations) == 66
    assert len(calculator_cases['output_columns']) == 80


def _wilder(closes, period=14):
    values = [float('nan')] * len(closes)
    if len(closes) <= period:
        return values
    changes = [closes[index] - closes[index - 1] for index in range(1, len(closes))]
    gain = sum(max(change, 0) for change in changes[:period]) / period
    loss = sum(max(-change, 0) for change in changes[:period]) / period
    for position in range(period, len(closes)):
        if position > period:
            change = changes[position - 1]
            gain = (gain * (period - 1) + max(change, 0)) / period
            loss = (loss * (period - 1) + max(-change, 0)) / period
        values[position] = 100.0 if loss == 0 and gain else 0.0 if gain == 0 else 100 - 100 / (1 + gain / loss)
    return values


def test_exact_continue_18_raw_to_16_effective(candidate, calculator_cases, quality_cases):
    frame, case = _fixture_case(quality_cases['owner_policy_snapshot'])
    before = _snapshot(frame, case)
    result = _calculate(candidate, frame, case)
    assert result['status'] == 'CALCULATED_OFFLINE'
    assert result['assessment']['raw_assessment']['rows_checked'] == 18
    assert len(result['candidate_rows']) == 18
    group = result['effective_groups'][0]
    assert len(group['effective_rows']) == 16
    assert group['candidate_ordinals'] == list(range(15)) + [17]
    assert result['quarantine']['candidate_ordinals'] == [15, 16]
    assert [row['disposition'] for row in result['candidate_row_map']][15:17] == ['QUARANTINED', 'QUARANTINED']
    expected = _wilder(list(range(100, 115)) + [110])
    actual = group['indicators']['RSI14'].to_numpy()
    np.testing.assert_array_equal(pd.isna(actual), [True] * 14 + [False, False])
    np.testing.assert_allclose(actual[14:], [100, 76.47058823529412],
                               **calculator_cases['tolerance'])
    np.testing.assert_allclose(actual, expected, equal_nan=True, **calculator_cases['tolerance'])
    assert result['quarantine']['retained_findings'] == result['assessment']['raw_assessment']['findings']
    assert result['quarantine']['raw_groups'] == result['assessment']['raw_assessment']['groups']
    _unchanged(frame, case, before)


@pytest.mark.parametrize('field', RAW_COLUMNS[:6])
def test_late_group_inexact_integer_blocks_before_every_ta_call(candidate, calculator_cases,
                                                              quality_cases, monkeypatch, field):
    first = _sequence(calculator_cases)
    second = _sequence(calculator_cases, price_shift=400).astype(object)
    # Keep known valid OHLC while making each target computational scalar lossy.
    if field in ('open', 'close', 'high', 'low'):
        for name in ('open', 'close', 'high', 'low'):
            second.at[0, name] = 2 ** 53 + 1
    else:
        second.at[0, field] = 2 ** 53 + 1
    frame, case = _capture(quality_cases, [first, second])
    calls = _forbid_ta(monkeypatch, calculator_cases)
    result = _calculate(candidate, frame, case)
    _assert_blocked(result)
    assert 'BINARY64_INPUT_UNREPRESENTABLE' in _blockers(result)
    assert calls == []


@pytest.mark.parametrize('integer', [2 ** 53, 2 ** 54, -(2 ** 54)])
def test_exact_large_integer_has_no_fixed_consecutive_bound(candidate, calculator_cases, quality_cases, integer):
    source = _sequence(calculator_cases).astype(object)
    source.at[0, 'value'] = integer
    frame, case = _capture(quality_cases, [source])
    before = _snapshot(frame, case)
    result = _calculate(candidate, frame, case)
    assert result['status'] == 'CALCULATED_OFFLINE'
    assert result['candidate_rows'].iloc[0]['value'] == integer
    _unchanged(frame, case, before)


@pytest.mark.parametrize('mutation', ['missing_authority', 'unsupported_profile', 'coverage_unknown',
                                    'coverage_count_wrong', 'mixed_interval', 'late_bad_identity'])
def test_every_late_admission_blocker_precedes_ta(candidate, calculator_cases, quality_cases, monkeypatch, mutation):
    frame, case = _capture(quality_cases, [_sequence(calculator_cases),
                                         _sequence(calculator_cases, price_shift=400)])
    if mutation == 'missing_authority':
        case['retained_evidence'].pop('producer_selection')
    elif mutation == 'unsupported_profile':
        case['retained_evidence']['serialization_profile'] = 'UNREVIEWED_PRODUCTION_FLOAT_PROFILE'
    elif mutation == 'coverage_unknown':
        case['retained_evidence']['source_membership'] = []
    elif mutation == 'coverage_count_wrong':
        case['metadata']['identity_records'][1]['counts']['rows_received'] += 1
    elif mutation == 'mixed_interval':
        case['metadata']['identity_records'][1]['interval'] = 60
    else:
        case['metadata']['identity_records'][1]['identity']['raw_instrument_id'] = 'FOREIGN_RAW_ID'
    calls = _forbid_ta(monkeypatch, calculator_cases)
    result = _calculate(candidate, frame, case)
    _assert_blocked(result)
    assert calls == []


@pytest.mark.parametrize('damage', ['duplicate', 'bool', 'out_of_range', 'empty', 'missing_record'])
def test_assessor_protocol_cannot_authorize_bad_ordinals(candidate, calculator_cases, quality_cases,
                                                       monkeypatch, damage):
    frame, case = _capture(quality_cases, [_sequence(calculator_cases),
                                         _sequence(calculator_cases, price_shift=400)])
    report = candidate.data_quality.assess_candles(frame, **case)
    assert report['eligibility']['status'] == 'PASS'
    if damage == 'duplicate':
        report['eligibility']['groups'][1]['candidate_ordinals'] = [320, 320]
    elif damage == 'bool':
        report['eligibility']['groups'][1]['candidate_ordinals'] = [True]
    elif damage == 'out_of_range':
        report['eligibility']['groups'][1]['candidate_ordinals'] = [len(frame)]
    elif damage == 'empty':
        report['eligibility']['groups'][1]['candidate_ordinals'] = []
    else:
        report['eligibility']['groups'].pop()
    monkeypatch.setattr(candidate.data_quality, 'assess_candles', lambda *args, **kwargs: copy.deepcopy(report))
    calls = _forbid_ta(monkeypatch, calculator_cases)
    result = _calculate(candidate, frame, case)
    _assert_blocked(result)
    assert calls == []


def test_e19_selection_authority_and_unknown_mapping(candidate, calculator_cases, quality_cases, monkeypatch):
    frame, case = _fixture_case(quality_cases['e19'])
    result = _calculate(candidate, frame, case)
    assert result['status'] == 'CALCULATED_OFFLINE'
    assert result['assessment']['raw_assessment']['rows_checked'] == 4
    assert len(result['candidate_rows']) == 3
    assert [len(group['effective_rows']) for group in result['effective_groups']] == [2, 1]
    assert result['quarantine']['candidate_ordinals'] == []
    bad = copy.deepcopy(case)
    bad['metadata'] = copy.deepcopy(quality_cases['e19_negative_metadata'])
    calls = _forbid_ta(monkeypatch, calculator_cases)
    blocked = _calculate(candidate, frame, bad)
    _assert_blocked(blocked)
    assert blocked['quarantine']['candidate_ordinals'] is None
    assert blocked['quarantine']['null_reason'] == 'ORIGINAL_SELECTION_NOT_VERIFIED'
    for row in blocked['candidate_row_map']:
        assert row['identity_record_id'] is None
        assert row['source_row_ref'] is None
        assert row['raw_key'] is None
    assert calls == []


@pytest.mark.parametrize('variant', ['exact_duplicate', 'end_conflict', 'ohlc_conflict', 'unselected_conflict'])
def test_raw_variants_have_no_arbitrary_winner(candidate, calculator_cases, quality_cases, monkeypatch, variant):
    source = _sequence(calculator_cases, bars=20).astype(object)
    other = source.iloc[[4]].copy(deep=True)
    if variant == 'end_conflict':
        other.iloc[0, other.columns.get_loc('end')] = '2020-01-05 17:00:00'
    elif variant in ('ohlc_conflict', 'unselected_conflict'):
        for field in ('open', 'close', 'high', 'low'):
            other.iloc[0, other.columns.get_loc(field)] += 30
    source = pd.concat([source, other], ignore_index=True)
    selected = [(0, ordinal) for ordinal in range(20 if variant == 'unselected_conflict' else 21)]
    frame, case = _capture(quality_cases, [source], selection=selected)
    before = _snapshot(frame, case)
    result = _calculate(candidate, frame, case)
    raw = result['assessment']['raw_assessment']
    groups = raw['groups']
    competing = [group for group in groups if group['variant_count'] == 2]
    assert len(competing) == 1
    conflict = competing[0]
    assert conflict['raw_key']['begin'] == source.iloc[4]['begin']
    record_id = case['metadata']['identity_records'][0]['record_id']
    provenance_ref = case['retained_evidence']['retained_sources'][0]['provenance_ref']
    assert {tuple(locator) for locator in conflict['variants']} == {
        (record_id, provenance_ref, 4), (record_id, provenance_ref, 20)}
    assert raw['rows_checked'] == 21
    assert raw['selected_raw_rows'] == len(selected)
    assert raw['unselected_raw_rows'] == (1 if variant == 'unselected_conflict' else 0)
    assert result['assessment']['quality_result']['rows_checked'] == len(selected)
    assert result['assessment']['coverage_assessment']['status'] == 'COMPLETE'
    if variant == 'exact_duplicate':
        _assert_blocked(result)
        assert conflict['classification'] == 'DUPLICATE_EXACT'
        assert conflict['reasons'] == ['DUPLICATE_EXACT']
        assert raw['quarantined_rows'] == 0
    else:
        assert result['status'] == 'CALCULATED_OFFLINE'
        assert conflict['classification'] == 'SOURCE_KEY_CONFLICT'
        expected_reasons = {'REVISION_NOT_PROVEN', 'END_VARIANT' if variant == 'end_conflict' else 'OHLC_VARIANT'}
        if variant == 'unselected_conflict':
            expected_reasons.add('UNSELECTED_RAW_VARIANT_CONFLICT')
        assert set(conflict['reasons']) == expected_reasons
        assert raw['quarantined_rows'] == 2
        assert result['assessment']['quality_result']['accepted_rows'] == 19
        assert len(result['effective_groups'][0]['effective_rows']) == 19
        assert result['assessment']['quality_result']['quarantined_rows'] == (
            1 if variant == 'unselected_conflict' else 2)
        assert 4 not in result['effective_groups'][0]['candidate_ordinals']
        assert 20 not in result['effective_groups'][0]['candidate_ordinals']
        assert result['quarantine']['candidate_ordinals'] == ([4] if variant == 'unselected_conflict' else [4, 20])
    _unchanged(frame, case, before)


def test_input_index_and_extra_column_dtypes_survive_blocked_copy(candidate, calculator_cases, quality_cases):
    frame, case = _capture(quality_cases, [_sequence(calculator_cases, bars=20)])
    frame.index = pd.MultiIndex.from_tuples([('same', ordinal % 3) for ordinal in range(len(frame))],
                                           names=['label', 'repeat'])
    frame['extra_category'] = pd.Categorical(['x', 'y'] * 10)
    frame['extra_object'] = [{'notes': [ordinal]} for ordinal in range(len(frame))]
    before = _snapshot(frame, case)
    result = _calculate(candidate, frame, case)
    _assert_blocked(result)  # Extra schema was not independently captured in this negative snapshot.
    pd.testing.assert_frame_equal(result['candidate_rows'], frame, check_exact=True)
    result['candidate_rows'].iloc[0]['extra_object']['notes'].append('private-result-change')
    _unchanged(frame, case, before)


def test_assessor_receives_private_snapshots_once_and_primary_survives(candidate, calculator_cases,
                                                                     quality_cases, monkeypatch):
    frame, case = _capture(quality_cases, [_sequence(calculator_cases, bars=20)])
    frame['extra_object'] = [{'notes': [ordinal]} for ordinal in range(len(frame))]
    before = _snapshot(frame, case)
    primary = ValueError('B2_TEST_ASSESSOR_PRIMARY')
    calls = []

    def failing_assessor(private_frame, **kwargs):
        calls.append(True)
        assert private_frame is not frame
        assert kwargs['spec'] is not case['spec']
        assert kwargs['metadata'] is not case['metadata']
        assert kwargs['retained_evidence'] is not case['retained_evidence']
        private_frame.iloc[0]['extra_object']['notes'].append('assessor-private')
        kwargs['metadata']['run_id'] = 'private-mutation'
        kwargs['spec']['dataset_id'] = 'private-mutation'
        kwargs['retained_evidence']['producer_selection']['snapshot_id'] = 'private-mutation'
        raise primary

    monkeypatch.setattr(candidate.data_quality, 'assess_candles', failing_assessor)
    native = _forbid_ta(monkeypatch, calculator_cases)
    with pytest.raises(ValueError) as caught:
        _calculate(candidate, frame, case)
    assert caught.value is primary
    assert calls == [True]
    assert native == []
    _unchanged(frame, case, before)


@pytest.mark.parametrize('failure', ['assessment', 'kernel'])
def test_logging_fault_does_not_mask_primary(candidate, calculator_cases, quality_cases, monkeypatch, failure):
    import run_logging
    frame, case = _capture(quality_cases, [_sequence(calculator_cases)])
    before = _snapshot(frame, case)
    primary = RuntimeError('B2_PRIMARY_NUMERIC_FAILURE')
    secondary = RuntimeError(CANARY)

    def fail(*args, **kwargs):
        raise primary

    def logging_failure(*args, **kwargs):
        raise secondary

    if failure == 'assessment':
        monkeypatch.setattr(candidate.data_quality, 'assess_candles', fail)
    else:
        monkeypatch.setattr(talib, 'RSI', fail)
    monkeypatch.setattr(run_logging, 'emit_event', logging_failure)
    with pytest.raises(RuntimeError) as caught:
        _calculate(candidate, frame, case, logging_context={})
    assert caught.value is primary
    assert caught.value.args == ('B2_PRIMARY_NUMERIC_FAILURE',)
    _unchanged(frame, case, before)


def test_logging_fault_cannot_admit_blocked_input(candidate, calculator_cases, quality_cases, monkeypatch):
    import run_logging
    frame, case = _capture(quality_cases, [_sequence(calculator_cases)])
    case['retained_evidence'].pop('producer_selection')
    monkeypatch.setattr(run_logging, 'emit_event', lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError(CANARY)))
    calls = _forbid_ta(monkeypatch, calculator_cases)
    result = _calculate(candidate, frame, case, logging_context={})
    _assert_blocked(result)
    assert calls == []


@pytest.mark.parametrize('profile', ['compatibility', 'EMA'])
def test_unsupported_native_profile_blocks_before_ta(candidate, calculator_cases, quality_cases,
                                                    monkeypatch, profile):
    frame, case = _capture(quality_cases, [_sequence(calculator_cases)])
    if profile == 'compatibility':
        monkeypatch.setattr(talib, 'get_compatibility', lambda: 1)
    else:
        original = talib.get_unstable_period
        monkeypatch.setattr(talib, 'get_unstable_period', lambda name: 1 if name == profile else original(name))
    calls = _forbid_ta(monkeypatch, calculator_cases)
    result = _calculate(candidate, frame, case)
    _assert_blocked(result)
    assert 'B2_NATIVE_PROFILE_UNSUPPORTED' in _blockers(result)
    assert calls == []


def test_native_profile_drift_discards_all_partial_groups(candidate, calculator_cases, quality_cases, monkeypatch):
    frame, case = _capture(quality_cases, [_sequence(calculator_cases),
                                         _sequence(calculator_cases, price_shift=400)])
    changed = {'value': False}
    original = talib.RSI

    def rsi(*args, **kwargs):
        output = original(*args, **kwargs)
        changed['value'] = True
        return output

    monkeypatch.setattr(talib, 'RSI', rsi)
    monkeypatch.setattr(talib, 'get_compatibility', lambda: 1 if changed['value'] else 0)
    result = _calculate(candidate, frame, case)
    assert changed['value']
    _assert_blocked(result)
    assert 'B2_NATIVE_PROFILE_CHANGED' in _blockers(result)


def test_admitted_extra_columns_and_duplicate_labels_preserve_dtypes(candidate, calculator_cases, quality_cases):
    labels = ['x' if ordinal % 2 else 'y' for ordinal in range(320)]
    frame, case = _capture(quality_cases, [_sequence(calculator_cases)],
                          extras={'extra_label': labels, 'RSI14': [None] * 320})
    frame['extra_label'] = pd.Categorical(frame['extra_label'])
    frame['begin'] = frame['begin'].astype('string')
    frame.index = pd.MultiIndex.from_tuples([('same', ordinal % 3) for ordinal in range(len(frame))],
                                           names=['label', 'repeat'])
    before = _snapshot(frame, case)
    result = _calculate(candidate, frame, case)
    assert result['status'] == 'CALCULATED_OFFLINE'
    pd.testing.assert_frame_equal(result['candidate_rows'], frame, check_exact=True)
    group = result['effective_groups'][0]
    pd.testing.assert_frame_equal(group['effective_rows'], frame.reset_index(drop=True), check_exact=True)
    assert group['effective_rows']['RSI14'].isna().all()
    assert group['indicators']['RSI14'].notna().sum() == 306
    _unchanged(frame, case, before)


def test_candidate_duplicate_locator_cannot_become_two_bars(candidate, calculator_cases, quality_cases, monkeypatch):
    frame, case = _capture(quality_cases, [_sequence(calculator_cases, bars=20)],
                          selection=[(0, 0), (0, 0)])
    calls = _forbid_ta(monkeypatch, calculator_cases)
    result = _calculate(candidate, frame, case)
    _assert_blocked(result)
    assert result['quarantine']['candidate_ordinals'] == []
    assert calls == []


def _ema(values, period):
    output = [float('nan')] * len(values)
    available = next((index for index, value in enumerate(values) if not pd.isna(value)), len(values))
    seed = available + period - 1
    if seed >= len(values):
        return output
    previous = sum(values[available:seed + 1]) / period
    output[seed] = previous
    alpha = 2.0 / (period + 1)
    for index in range(seed + 1, len(values)):
        previous = (values[index] - previous) * alpha + previous
        output[index] = previous
    return output


def test_independent_sequential_stoch_mfi_obv_trix_oracles(candidate, calculator_cases, quality_cases):
    frame, case = _capture(quality_cases, [_sequence(calculator_cases)])
    result = _calculate(candidate, frame, case)
    assert result['status'] == 'CALCULATED_OFFLINE'
    output = result['effective_groups'][0]['indicators']
    closes, highs, lows, volumes = [frame[name].tolist() for name in ('close', 'high', 'low', 'volume')]
    obv = [float(volumes[0])]
    for index in range(1, len(frame)):
        direction = 1 if closes[index] > closes[index - 1] else -1 if closes[index] < closes[index - 1] else 0
        obv.append(obv[-1] + direction * volumes[index])
    typical = [(high + low + close) / 3 for high, low, close in zip(highs, lows, closes)]
    positive, negative = [0.0], [0.0]
    for index in range(1, len(frame)):
        flow = typical[index] * volumes[index]
        positive.append(flow if typical[index] > typical[index - 1] else 0.0)
        negative.append(flow if typical[index] < typical[index - 1] else 0.0)
    mfi = [float('nan')] * len(frame)
    for index in range(14, len(frame)):
        up = sum(positive[index - 13:index + 1])
        down = sum(negative[index - 13:index + 1])
        mfi[index] = 100 * up / (up + down) if up + down else 0.0
    raw_k = [float('nan')] * len(frame)
    for index in range(4, len(frame)):
        high, low = max(highs[index - 4:index + 1]), min(lows[index - 4:index + 1])
        raw_k[index] = 100 * (closes[index] - low) / (high - low) if high != low else 0.0
    slow_k, slow_d = [float('nan')] * len(frame), [float('nan')] * len(frame)
    for index in range(6, len(frame)):
        slow_k[index] = sum(raw_k[index - 2:index + 1]) / 3
    for index in range(8, len(frame)):
        slow_d[index] = sum(slow_k[index - 2:index + 1]) / 3
    slow_k[:8] = [float('nan')] * 8
    third = _ema(_ema(_ema(closes, 30), 30), 30)
    trix = [float('nan')] * len(frame)
    for index in range(88, len(frame)):
        trix[index] = (third[index] / third[index - 1] - 1) * 100
    for name, expected in [('OBV', obv), ('MFI', mfi), ('STOCH_slowk', slow_k),
                           ('STOCH_slowd', slow_d), ('TRIX', trix), ('RSI14', _wilder(closes))]:
        np.testing.assert_array_equal(pd.isna(output[name]), pd.isna(expected), err_msg=name)
        np.testing.assert_allclose(output[name].to_numpy(), expected, equal_nan=True,
                                   err_msg=name, **calculator_cases['tolerance'])


def test_numeric_copy_signed_zero_with_explicit_assessor_protocol_stub(candidate, calculator_cases,
                                                                     quality_cases, monkeypatch):
    # Unit isolation only: current miniature normalization does not admit floats.
    frame, case = _capture(quality_cases, [_sequence(calculator_cases, bars=20)])
    report = candidate.data_quality.assess_candles(frame, **case)
    assert report['eligibility']['status'] == 'PASS'
    frame.at[0, 'value'] = np.float64(-0.0)
    before = _snapshot(frame, case)
    monkeypatch.setattr(candidate.data_quality, 'assess_candles', lambda *args, **kwargs: copy.deepcopy(report))
    original, observed = candidate._calculate_frame, []

    def kernel(working, logging_context, state, *, group_ordinals):
        observed.append(np.signbit(working.iloc[0]['value']))
        return original(working, logging_context, state, group_ordinals=group_ordinals)

    monkeypatch.setattr(candidate, '_calculate_frame', kernel)
    result = _calculate(candidate, frame, case)
    assert result['status'] == 'CALCULATED_OFFLINE'
    assert observed == [True]
    assert np.signbit(result['candidate_rows'].iloc[0]['value'])
    assert result['assessment']['raw_assessment']['source_verification'] == 'NOT_VERIFIED'
    assert result['production_admission'] == 'NOT_ENABLED'
    _unchanged(frame, case, before)


@pytest.mark.parametrize('value', [None, True, np.bool_(False), '2', complex(2, 0), Decimal('2'), np.inf, -np.inf, np.nan])
def test_numeric_copy_rejects_unsupported_protocol_values_before_ta(candidate, calculator_cases,
                                                                 quality_cases, monkeypatch, value):
    # Deliberately injected assessor protocol, not a fabricated production profile.
    frame, case = _capture(quality_cases, [_sequence(calculator_cases, bars=20)])
    report = candidate.data_quality.assess_candles(frame, **case)
    assert report['eligibility']['status'] == 'PASS'
    frame.at[0, 'value'] = value
    monkeypatch.setattr(candidate.data_quality, 'assess_candles', lambda *args, **kwargs: copy.deepcopy(report))
    calls = _forbid_ta(monkeypatch, calculator_cases)
    result = _calculate(candidate, frame, case)
    _assert_blocked(result)
    assert 'BINARY64_INPUT_UNREPRESENTABLE' in _blockers(result)
    assert calls == []


def test_fixed_safe_diagnostics_do_not_dump_candidate_metadata(candidate, calculator_cases, quality_cases,
                                                            monkeypatch):
    import run_logging
    frame, case = _capture(quality_cases, [_sequence(calculator_cases, bars=20)])
    case['metadata']['private_test_note'] = CANARY
    events = []

    def emit(context, level, event, message, fields):
        events.append((level, event, message, copy.deepcopy(fields)))
        return dict(accepted=True, confirmed=True)

    monkeypatch.setattr(run_logging, 'emit_event', emit)
    result = _calculate(candidate, frame, case, logging_context={})
    assert result['status'] == 'CALCULATED_OFFLINE'
    assert events
    assert CANARY not in json.dumps(events, default=str)
    assert all(fields.get('fields') == {} for _, _, _, fields in events)
