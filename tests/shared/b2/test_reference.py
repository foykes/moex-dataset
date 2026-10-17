"""P2 availability and MATH_ONLY controls; no captured market-source corpus."""

import base64
import binascii
import copy
from datetime import date, timedelta
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd
import pytest
import talib


# %%
ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / 'tests/fixtures/b2'
RAW_COLUMNS = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end', 'ticker']
DATA_SHA256 = '8d0ded371bf11a71b64e2b393a49dd0dcb8bfaa1bc2c89990d945cd889fcc367'
MANIFEST_SHA256 = 'd554523e9c306e28adeb48b1b3b9d1f9038198ad2c173156c8daed5d0cbf8e92'
FROZEN_SHA256 = {
    'data_quality.py': '09a0709f7810fc9cdab997db8ba8fe28d745986343bc139974408532a50b16bb',
    'tests/shared/b2/test_quality.py': '4000bab989a56eb67d1fdc67e0e4adf2ce3b853773ef4595c79514f5dae90b35',
    'tests/fixtures/b2/cases.json': 'd56dd4befcac31ea351870dd20b847fea36d7acca263c21472b8884750511664',
    'tests/fixtures/b2/archived_observations.json': '95e3ecda17bd7aa8b55f8128f24a99c2f3df600109b7b5e8e05392baaee8cf00',
    'tests/fixtures/b2/evidence_manifest.json': 'e3e335e74db714fb3b1c312201fd1d526c0d404e6d52417e79ac2d661589606e',
}
WINDOWS = {
    '1d': (24, '2023-01-01', '2024-12-31'),
    '1h': (60, '2024-10-01', '2024-12-31'),
    '10m': (10, '2024-12-02', '2024-12-13'),
    '1m': (1, '2024-12-02', '2024-12-02'),
}
MATH_UNSTABLE_PERIODS = (
    'ADX', 'ADXR', 'ATR', 'CMO', 'DX', 'EMA', 'HT_DCPERIOD', 'HT_DCPHASE',
    'HT_PHASOR', 'HT_SINE', 'HT_TRENDLINE', 'HT_TRENDMODE', 'KAMA', 'MFI',
    'MINUS_DI', 'MINUS_DM', 'NATR', 'PLUS_DI', 'PLUS_DM', 'RSI', 'STOCHRSI', 'T3',
)


def _strict_json(text):
    # Дубликаты ключей и NaN/Infinity не превращаются в допустимый manifest.
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('DUPLICATE_JSON_KEY')
            result[key] = value
        return result

    def nonfinite(_value):
        raise ValueError('NONFINITE_JSON_CONSTANT')

    return json.loads(text, object_pairs_hook=pairs, parse_float=Decimal,
                      parse_constant=nonfinite)


def _lf_bytes(path):
    return path.read_text(encoding='utf-8').encode('utf-8')


@pytest.fixture
def reference_bundle():
    manifest = _strict_json((FIXTURES / 'reference_manifest.json').read_text(encoding='utf-8'))
    data = _strict_json((FIXTURES / 'reference_data.json').read_text(encoding='utf-8'))
    return manifest, data


def _decode_frozen_body(page):
    # Проверка точных bytes независима от доверия к поставщику и полноты данных.
    encoded = page.get('original_body_base64')
    size = page.get('original_body_bytes')
    digest = page.get('original_body_sha256')
    if type(encoded) is not str or type(size) is not int or size < 0:
        raise ValueError('INVALID_BODY_REPRESENTATION')
    if type(digest) is not str or re.fullmatch('[0-9a-f]{64}', digest) is None:
        raise ValueError('INVALID_BODY_HASH')
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as error:
        raise ValueError('INVALID_BODY_BASE64') from error
    if len(raw) != size or hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError('BODY_BYTES_MISMATCH')
    parsed = _strict_json(raw.decode('utf-8'))
    if type(parsed) is not dict:
        raise ValueError('BODY_OBJECT_REQUIRED')
    return raw, parsed


def test_reference_payload_hashes_and_profiles_are_frozen(reference_bundle):
    manifest, data = reference_bundle
    data_raw = _lf_bytes(FIXTURES / 'reference_data.json')
    assert hashlib.sha256(data_raw).hexdigest() == DATA_SHA256
    assert hashlib.sha256(_lf_bytes(FIXTURES / 'reference_manifest.json')).hexdigest() == MANIFEST_SHA256
    assert manifest['manifest_version'] == data['schema_version'] == 1
    assert type(manifest['manifest_version']) is type(data['schema_version']) is int
    assert manifest['reference_data'] == {
        'path': 'tests/fixtures/b2/reference_data.json',
        'sha256_basis': 'UTF8_TEXT_LF', 'sha256': DATA_SHA256, 'bytes': len(data_raw),
    }
    assert manifest['profile'] == data['profile'] == 'MATH_ONLY'
    assert data['evidence_category'] == 'SYNTHETIC_MATHEMATICAL_CONTROLS'
    assert data['source_observations'] == []
    assert data['source_observation_count'] == 0
    assert data['synthetic_identity'] == 'B2_MATH_ONLY'
    assert all(value == 'NOT_VERIFIED' for value in manifest['f0_statuses'].values())
    assert set(manifest['f0_statuses']) == {
        'identity', 'response', 'quality', 'coverage', 'full_dataset_validation',
    }
    assert manifest['production_admission'] == 'NOT_ENABLED'
    assert manifest['source_reference_status'] == 'NOT_AVAILABLE'
    assert manifest['source_capture_permission'] == 'NONE'


def test_all_eight_source_cells_are_explicitly_unavailable(reference_bundle):
    manifest, _data = reference_bundle
    expected = {instrument + '_' + interval for instrument in ['SBER', 'MOEX'] for interval in WINDOWS}
    cells = manifest['source_cells']
    assert len(cells) == 8
    assert {cell['cell_id'] for cell in cells} == expected
    for cell in cells:
        instrument, interval = cell['cell_id'].split('_')
        value, start, end = WINDOWS[interval]
        assert cell['requested_identity'] == {
            'engine': 'stock', 'market': 'shares', 'source_scope': 'AGGREGATE',
            'board': None, 'raw_instrument_id': instrument, 'interval': value,
        }
        assert cell['requested_window'] == {'from': start, 'till': end}
        assert cell['availability'] == cell['source_verification'] == 'NOT_AVAILABLE'
        assert cell['source_pages'] == []
        assert set(cell['observed']) == {
            'first_begin', 'last_begin', 'received_rows', 'clean_rows', 'page_count',
            'terminal_empty_page', 'acquired_at', 'original_http_sha256s',
        }
        assert all(value is None for value in cell['observed'].values())
    limits = manifest['future_capture_limits']
    assert limits['minimum_clean_rows_per_cell'] == 320
    assert limits['maximum_gets_per_cell'] == 10
    assert limits['maximum_rows_per_cell'] == 5000
    assert limits['maximum_total_gets'] == 80
    assert limits['maximum_total_original_body_bytes'] == 64 * 1024 * 1024
    assert limits['permission_required'] is limits['terminal_empty_page_required'] is True
    assert limits['automatic_window_expansion'] is False
    assert limits['cap_or_insufficient_clean_rows_outcome'] == 'INCOMPLETE'


def test_old_archive_category_and_10_of_79_ledger_are_preserved(reference_bundle):
    manifest, _data = reference_bundle
    baseline = manifest['historical_baseline']
    assert baseline['read_only'] is True
    assert baseline['evidence_category'] == 'RECONSTRUCTED_PARSED_ARCHIVE'
    assert baseline['source_verification'] == 'NOT_VERIFIED'
    for relative, digest in FROZEN_SHA256.items():
        assert hashlib.sha256(_lf_bytes(ROOT / relative)).hexdigest() == digest
    for item in baseline['files']:
        assert item['sha256_basis'] == 'UTF8_TEXT_LF'
        assert item['sha256'] == FROZEN_SHA256[item['path']]
    old = _strict_json((FIXTURES / 'evidence_manifest.json').read_text(encoding='utf-8'))
    ledger = old['coverage_ledger']
    assert old['evidence_layer'] == baseline['evidence_category']
    assert old['source_verification'] == old['source_authentication'] == 'NOT_VERIFIED'
    assert ledger['expected_original_ohlc_observations'] == baseline['expected_original_observations'] == 79
    assert ledger['reconstructed_parsed_public_observations'] == baseline['reconstructed_parsed_public_observations'] == 10
    assert ledger['unavailable_unenumerated_original_observations'] == baseline['unavailable_unenumerated_original_observations'] == 69
    assert ledger['original_unavailable_observation_ids'] is baseline['unavailable_observation_ids'] is None
    assert ledger['original_ohlc_observations_with_raw_bytes_available'] == 0
    assert ledger['original_ohlc_observations_with_raw_bytes_unavailable'] == 79
    assert baseline['additional_assb_corroborations'] == 1
    assert baseline['additional_abio_variants'] == 2
    assert baseline['corroboration_increases_10_of_79'] is False
    assert baseline['original_http_bodies_available'] is False
    assert baseline['recovered_export_status'] == 'NOT_AVAILABLE'
    assert baseline['recovered_files_created'] is False
    assert baseline['separate_local_audit'] == {
        'ohlc_anomalies': 39, 'same_corpus_as_public_79': False, 'may_replace_unknown_69': False,
    }


@pytest.mark.parametrize('text', [
    '{"a":1,"a":2}', '{"a":{"b":1,"b":2}}',
    '{"a":NaN}', '{"a":Infinity}', '{"a":-Infinity}',
])
def test_reference_json_rejects_duplicate_keys_and_nonfinite_constants(text):
    with pytest.raises(ValueError):
        _strict_json(text)


def test_valid_synthetic_body_bytes_still_cannot_be_source(reference_bundle):
    manifest, data = reference_bundle
    page = data['frozen_byte_controls'][0]
    raw, parsed = _decode_frozen_body(page)
    assert hashlib.sha256(raw).hexdigest() == page['original_body_sha256']
    assert parsed['fixture_profile'] == page['profile'] == 'MATH_ONLY'
    assert parsed['candles']['data'] == []
    assert data['source_observations'] == []
    assert all(cell['availability'] == 'NOT_AVAILABLE' for cell in manifest['source_cells'])
    assert all(value == 'NOT_VERIFIED' for value in manifest['f0_statuses'].values())
    assert manifest['source_reference_status'] == 'NOT_AVAILABLE'
    assert manifest['production_admission'] == 'NOT_ENABLED'


def test_synthetic_verified_label_and_empty_page_hash_do_not_change_source_report(reference_bundle):
    manifest, data = reference_bundle
    before = copy.deepcopy(manifest)
    # Поле VERIFIED внутри произвольного JSON остаётся недоверенным текстом.
    raw = json.dumps({'fixture_profile': 'MATH_ONLY', 'source_verification': 'VERIFIED',
                      'candles': {'columns': RAW_COLUMNS[:8], 'data': []}},
                     separators=(',', ':')).encode('utf-8')
    page = {
        'original_body_base64': base64.b64encode(raw).decode('ascii'),
        'original_body_bytes': len(raw), 'original_body_sha256': hashlib.sha256(raw).hexdigest(),
    }
    decoded, parsed = _decode_frozen_body(page)
    assert decoded == raw
    assert parsed['source_verification'] == 'VERIFIED'
    assert parsed['fixture_profile'] == 'MATH_ONLY'
    assert parsed['candles']['data'] == []
    assert manifest == before
    assert data['source_observations'] == []
    assert manifest['source_reference_status'] == 'NOT_AVAILABLE'
    assert all(value == 'NOT_VERIFIED' for value in manifest['f0_statuses'].values())
    assert all(cell['availability'] == 'NOT_AVAILABLE' for cell in manifest['source_cells'])


@pytest.mark.parametrize('field,value', [
    ('original_body_bytes', True), ('original_body_bytes', -1),
    ('original_body_bytes', '1'), ('original_body_bytes', 1),
    ('original_body_sha256', None), ('original_body_sha256', 'A' * 64),
    ('original_body_sha256', '0' * 64), ('original_body_sha256', 'abc'),
    ('original_body_base64', '!!!!'), ('original_body_base64', None),
])
def test_frozen_original_body_representation_and_hash_are_strict(reference_bundle, field, value):
    _manifest, data = reference_bundle
    page = copy.deepcopy(data['frozen_byte_controls'][0])
    page[field] = value
    with pytest.raises(ValueError):
        _decode_frozen_body(page)


@pytest.mark.parametrize('raw', [
    b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}', b'{"x":-Infinity}',
    b'[]', b'\xff', b'{',
])
def test_matching_original_bytes_hash_does_not_bypass_strict_json(raw):
    page = {
        'original_body_base64': base64.b64encode(raw).decode('ascii'),
        'original_body_bytes': len(raw), 'original_body_sha256': hashlib.sha256(raw).hexdigest(),
    }
    with pytest.raises(ValueError):
        _decode_frozen_body(page)


# %%
def _decimal_wilder(closes, period=14):
    # Независимая формула: 14 изменений для seed, затем рекурсия Wilder.
    result = np.full(len(closes), np.nan, dtype=np.float64)
    if len(closes) <= period:
        return result
    with localcontext() as context:
        context.prec = 80
        values = [Decimal(value) for value in closes]
        gain = Decimal(0)
        loss = Decimal(0)
        for position in range(1, period + 1):
            change = values[position] - values[position - 1]
            gain += max(change, Decimal(0))
            loss += max(-change, Decimal(0))
        gain /= period
        loss /= period
        for position in range(period, len(values)):
            if position > period:
                change = values[position] - values[position - 1]
                gain = (gain * (period - 1) + max(change, Decimal(0))) / period
                loss = (loss * (period - 1) + max(-change, Decimal(0))) / period
            denominator = gain + loss
            result[position] = float(100 * gain / denominator) if denominator else 0.0
    return result


def _rational_values(values):
    output = []
    with localcontext() as context:
        context.prec = 80
        for value in values:
            if value is None:
                output.append(np.nan)
            else:
                numerator, denominator = value.split('/')
                output.append(float(Decimal(numerator) / Decimal(denominator)))
    return np.asarray(output, dtype=np.float64)


@pytest.mark.parametrize('case_index', range(9))
def test_independent_wilder_matches_fixed_rationals_and_native_edges(reference_bundle, case_index):
    _manifest, data = reference_bundle
    assert talib.get_compatibility() == 0
    assert talib.get_unstable_period('RSI') == 0
    case = data['rsi_controls'][case_index]
    assert case['case_id'].startswith('B2_MATH_')
    closes = case['closes']
    expected = _rational_values(case['expected_rsi_rationals'])
    independent = _decimal_wilder(closes, period=14)
    native = talib.RSI(np.asarray(closes, dtype=np.float64), timeperiod=14)
    np.testing.assert_allclose(independent, expected, rtol=1e-12, atol=3e-12, equal_nan=True)
    np.testing.assert_allclose(native, expected, rtol=1e-12, atol=3e-12, equal_nan=True)
    assert np.isnan(native[:min(14, len(closes))]).all()
    assert np.isfinite(native[14:]).all()
    if case['case_id'] == 'B2_MATH_OWNER_CONTINUATION':
        assert len(closes) == 16
        assert case['expected_rsi_rationals'][14:] == ['100/1', '1300/17']


def _math_frame(data):
    count = data['native_controls']['row_count']
    rows = []
    for ordinal in range(count):
        close = 200 + ((ordinal * 17) % 31) + (ordinal // 9)
        opening = close + (ordinal % 3) - 1
        high = max(opening, close) + 2 + (ordinal % 5)
        low = min(opening, close) - 2 - (ordinal % 4)
        volume = 1000 + 11 * ordinal
        day = (date(2020, 1, 1) + timedelta(days=ordinal)).isoformat()
        rows.append([opening, close, high, low, close * volume, volume,
                     day + ' 00:00:00', day + ' 23:59:59', 'B2_MATH_ONLY'])
    frame = pd.DataFrame(rows, columns=RAW_COLUMNS)
    for column in RAW_COLUMNS[:6]:
        frame[column] = frame[column].astype(np.float64)
    return frame


def _assert_math_native_profile():
    # Независимый test-only профиль; globals не меняем, validator кандидата не используем.
    compatibility = talib.get_compatibility()
    assert type(compatibility) is int and compatibility == 0, 'MATH_NATIVE_PROFILE:compatibility'
    unstable = {}
    for name in MATH_UNSTABLE_PERIODS:
        period = talib.get_unstable_period(name)
        assert type(period) is int and period == 0, 'MATH_NATIVE_PROFILE:' + name
        unstable[name] = period
    return compatibility, unstable


def _math_only_kernel_result(tech, frame, profiles):
    native_profile = _assert_math_native_profile()
    expected = frame.copy(deep=True)
    for profile in profiles:
        inputs = [frame[column].to_numpy(dtype=np.float64, copy=True) for column in profile['inputs']]
        values = getattr(talib, profile['function'])(*inputs, **profile['parameters'])
        outputs = values if len(profile['outputs']) > 1 else (values,)
        assert len(outputs) == len(profile['outputs'])
        for name, value in zip(profile['outputs'], outputs):
            expected[name] = value
    assert _assert_math_native_profile() == native_profile
    state = {'rows': len(frame), 'instruments': 1, 'phase': 'CALCULATING',
             'columns': None, 'diagnostics': False, 'native_dtypes': True}
    actual = tech._calculate_frame(frame, None, state, group_ordinals=[list(range(len(frame)))])
    assert _assert_math_native_profile() == native_profile
    return actual, expected


def test_math_only_private_kernel_matches_all_66_native_profiles(reference_bundle):
    # Общий kernel, но без публичного admission adapter и без source/F0 PASS.
    import tech

    manifest, data = reference_bundle
    paths = [FIXTURES / 'reference_data.json', FIXTURES / 'reference_manifest.json']
    paths += [ROOT / relative for relative in FROZEN_SHA256]
    before_bytes = {path: path.read_bytes() for path in paths}
    frame = _math_frame(data)
    before_frame = frame.copy(deep=True)
    profiles = data['native_controls']['profiles']
    assert len(profiles) == 66
    names = [name for profile in profiles for name in profile['outputs']]
    assert len(names) == len(set(names)) == 80
    actual, expected = _math_only_kernel_result(tech, frame, profiles)
    assert list(actual.columns) == RAW_COLUMNS + names
    assert actual.index.equals(frame.index)
    pd.testing.assert_frame_equal(actual, expected, check_dtype=True, check_exact=False,
                                  rtol=1e-12, atol=3e-12)
    assert actual['HT_TRENDMODE'].dtype == np.dtype('int32')
    for name in names:
        np.testing.assert_array_equal(pd.isna(actual[name]), pd.isna(expected[name]))
    np.testing.assert_allclose(actual['RSI14'], _decimal_wilder(before_frame['close'].astype(int).tolist()),
                               rtol=1e-12, atol=3e-12, equal_nan=True)
    pd.testing.assert_frame_equal(frame, before_frame, check_exact=True)
    assert all(path.read_bytes() == value for path, value in before_bytes.items())
    assert all(value == 'NOT_VERIFIED' for value in manifest['f0_statuses'].values())
    assert manifest['source_reference_status'] == 'NOT_AVAILABLE'
    assert manifest['production_admission'] == 'NOT_ENABLED'


@pytest.mark.parametrize('setting,phase', [
    ('compatibility', 'before_reference'), ('HT_TRENDMODE', 'before_reference'),
    ('EMA', 'before_reference'), ('EMA', 'before_kernel'), ('EMA', 'after_kernel'),
])
def test_math_only_native_profile_drift_cannot_validate_both_sides(reference_bundle, monkeypatch,
                                                               setting, phase):
    import tech

    _manifest, data = reference_bundle
    frame = _math_frame(data).iloc[:20].copy(deep=True)
    profiles = data['native_controls']['profiles']
    compatibility = talib.get_compatibility
    unstable = talib.get_unstable_period
    bbands = talib.BBANDS
    observed = {'drift': phase == 'before_reference', 'native_calls': 0, 'kernel_calls': 0}

    def reported_compatibility():
        return 1 if observed['drift'] and setting == 'compatibility' else compatibility()

    def reported_unstable(name):
        return 1 if observed['drift'] and name == setting else unstable(name)

    def native_reference(*args, **kwargs):
        observed['native_calls'] += 1
        result = bbands(*args, **kwargs)
        if phase == 'before_kernel':
            observed['drift'] = True
        return result

    def kernel(working, _context, _state, *, group_ordinals):
        observed['kernel_calls'] += 1
        assert phase == 'after_kernel', 'UNEXPECTED_KERNEL_CALL'
        observed['drift'] = True
        return working.copy(deep=True)

    # Только Python getters: реальные C-library compatibility/unstable globals неизменны.
    with monkeypatch.context() as patch:
        patch.setattr(talib, 'get_compatibility', reported_compatibility)
        patch.setattr(talib, 'get_unstable_period', reported_unstable)
        patch.setattr(talib, 'BBANDS', native_reference)
        patch.setattr(tech, '_calculate_frame', kernel)
        with pytest.raises(AssertionError, match='MATH_NATIVE_PROFILE:' + setting):
            _math_only_kernel_result(tech, frame, profiles)
    assert observed['native_calls'] == (0 if phase == 'before_reference' else 1)
    assert observed['kernel_calls'] == (1 if phase == 'after_kernel' else 0)
    assert _assert_math_native_profile()[0] == 0
