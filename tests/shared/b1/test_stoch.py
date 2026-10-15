"""B1 fixture regressions; native numerical oracle and contract-only telemetry."""

import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import traceback
import uuid
import warnings

import numpy as np
import pandas as pd
import pytest
import talib
from talib import abstract
import mds_offline_guard as guard

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / 'tests/fixtures/b1'
REVIEWED_BASE = 'ebc66fc376a2077ce027cf33689aae31728709c2'
REFERENCE_SOURCE_REF = '07945ed4ded747145b40d7a7c25e8ce97abeaa0d'
EXPECTED_REFERENCE_TECH_BLOB = '5cab43aaaffcadcbf23feb96ab8783a9acdfc986'
ACCEPTED_B1_SOURCE_REF = '3b6e78f11f7681cdcd9c813fff4236a8c63ab677'
EXPECTED_ACCEPTED_B1_TECH_BLOB = 'ca7cc63bc5a95238dd587b46f68765cbf6ff38a1'
TRIX_BASE_LINE = b'        if len(TRIX_real_ticker) > 0: _full = pd.concat([TRIX_full, TRIX_real_ticker])\n'
TRIX_FIXED_LINE = b'        if len(TRIX_real_ticker) > 0: TRIX_full = pd.concat([TRIX_full, TRIX_real_ticker])\n'
CANARY = 'B1_SYNTHETIC_SECRET_CANARY'
PRIVATE_PATH = 'C:/B1_SYNTHETIC_PRIVATE/source'


def _load_module(path):
    specification = importlib.util.spec_from_file_location('b1_reference_' + uuid.uuid4().hex, path)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


@pytest.fixture
def schema():
    return json.loads((FIXTURES / 'schema.json').read_text(encoding='utf-8'))


@pytest.fixture
def candidate():
    import tech
    return tech


@pytest.fixture
def reference(tmp_path):
    git = shutil.which('git')
    if git is None:
        pytest.fail('REFERENCE_PREPARATION_FAILURE: Git unavailable')
    command = [git, 'show', REFERENCE_SOURCE_REF + ':tech.py']
    ticket = guard.issue_ticket('native-git', command, native=True)
    result = guard.run_child(command, role='native-git', ticket=ticket, timeout=10)
    if result.returncode != 0 or result.actual_exit != 0 or not result.validated or result.report_errors:
        pytest.fail('REFERENCE_PREPARATION_FAILURE: source command not validated')
    raw = result.stdout
    actual_blob = hashlib.sha1(b'blob ' + str(len(raw)).encode('ascii') + b'\0' + raw).hexdigest()
    if actual_blob != EXPECTED_REFERENCE_TECH_BLOB:
        pytest.fail('REFERENCE_PREPARATION_FAILURE: blob identity mismatch')
    source = tmp_path / 'reference-tech.py'
    source.write_bytes(raw)
    provenance = {
        'reviewed_base': REVIEWED_BASE, 'reference_source_ref': REFERENCE_SOURCE_REF,
        'reference_path': 'tech.py', 'expected_reference_tech_blob': EXPECTED_REFERENCE_TECH_BLOB,
        'actual_reference_tech_blob': actual_blob, 'reference_preparation': 'PASS',
        'command': ['git', 'show', REFERENCE_SOURCE_REF + ':tech.py'],
        'actual_exit': result.actual_exit, 'validated': result.validated,
        'ticket_id': ticket['ticket_id'], 'guard_report_errors': result.report_errors,
        'source_sha256': hashlib.sha256(raw).hexdigest(),
        'python': sys.version.split()[0], 'talib_wrapper': talib.__version__,
        'talib_native': talib.__ta_version__.decode('ascii'),
    }
    (tmp_path / 'reference-provenance.json').write_text(json.dumps(provenance, sort_keys=True), encoding='utf-8')
    try:
        return _load_module(source)
    except Exception:
        pytest.fail('REFERENCE_PREPARATION_FAILURE: complete module import failed')


def _accepted_git(git, arguments, records, *, expected_exit=0):
    command = [git, *arguments]
    ticket = guard.issue_ticket('native-git', command, native=True, expected_exit=expected_exit)
    result = guard.run_child(command, role='native-git', ticket=ticket, timeout=10)
    if (not result.validated or result.report_errors or result.timed_out
            or result.returncode != result.actual_exit
            or expected_exit is not None and (result.actual_exit != expected_exit or result.stderr != b'')):
        pytest.fail('ACCEPTED_B1_REFERENCE_PREPARATION_FAILURE: native command not validated')
    records.append({'command': ['git', *arguments], 'ticket_id': ticket['ticket_id'],
                    'expected_exit': expected_exit, 'actual_exit': result.actual_exit,
                    'validated': result.validated, 'guard_report_errors': result.report_errors,
                    'timed_out': result.timed_out})
    return result


def _git_blob(raw):
    return hashlib.sha1(b'blob ' + str(len(raw)).encode('ascii') + b'\0' + raw).hexdigest()


def _trix_profile(schema):
    function = abstract.Function('TRIX')
    state = {'compatibility': talib.get_compatibility(),
             'ema_unstable_period': talib.get_unstable_period('EMA')}
    if (function.parameters != schema['trix_parameters']
            or function.parameters != {'timeperiod': 30}
            or function.lookback != schema['trix_lookback'] or function.lookback != 88
            or state != schema['trix_native_state']
            or state != {'compatibility': 0, 'ema_unstable_period': 0}):
        pytest.fail('TRIX_ORACLE_PREPARATION_FAILURE: native profile mismatch')
    return {'python': sys.version.split()[0], 'talib_wrapper': talib.__version__,
            'talib_native': talib.__ta_version__.decode('ascii'),
            'parameters': dict(function.parameters), 'lookback': function.lookback,
            'native_state': state}


@pytest.fixture
def accepted_b1_reference(tmp_path, schema):
    # Historical STOCH reference stays independent; this is the accepted H88 source.
    git = shutil.which('git')
    if git is None:
        pytest.fail('ACCEPTED_B1_REFERENCE_PREPARATION_FAILURE: Git unavailable')
    records = []
    head_result = _accepted_git(git, ['show', 'HEAD:tech.py'], records)
    head_raw = head_result.stdout
    head_blob = _git_blob(head_raw)
    restored = head_raw
    transformation_count = 0
    if head_blob != EXPECTED_ACCEPTED_B1_TECH_BLOB:
        if head_raw.count(TRIX_FIXED_LINE) != 1 or head_raw.count(TRIX_BASE_LINE) != 0:
            pytest.fail('ACCEPTED_B1_REFERENCE_PREPARATION_FAILURE: single TRIX line required')
        restored = head_raw.replace(TRIX_FIXED_LINE, TRIX_BASE_LINE, 1)
        transformation_count = 1
    if _git_blob(restored) != EXPECTED_ACCEPTED_B1_TECH_BLOB:
        pytest.fail('ACCEPTED_B1_REFERENCE_PREPARATION_FAILURE: candidate source has foreign changes')

    availability = _accepted_git(git, ['rev-parse', '--verify', ACCEPTED_B1_SOURCE_REF + '^{commit}'],
                                 records, expected_exit=None)
    if (availability.actual_exit == 0 and availability.stderr == b''
            and availability.stdout.rstrip(b'\r\n') == ACCEPTED_B1_SOURCE_REF.encode('ascii')):
        source_result = _accepted_git(git, ['show', ACCEPTED_B1_SOURCE_REF + ':tech.py'], records)
        raw = source_result.stdout
        method = 'DIRECT_ACCEPTED_GIT_OBJECT'
        source_ref = ACCEPTED_B1_SOURCE_REF
        reference_transformations = 0
    elif (availability.actual_exit == 128 and availability.stdout == b''
          and availability.stderr.rstrip(b'\r\n') == b'fatal: Needed a single revision'):
        raw = restored
        method = 'STRICT_SINGLE_TRIX_REVERSAL' if transformation_count else 'HEAD_ACCEPTED_SOURCE'
        source_ref = 'HEAD'
        reference_transformations = transformation_count
    else:
        pytest.fail('ACCEPTED_B1_REFERENCE_PREPARATION_FAILURE: object availability unresolved')
    actual_blob = _git_blob(raw)
    if actual_blob != EXPECTED_ACCEPTED_B1_TECH_BLOB:
        pytest.fail('ACCEPTED_B1_REFERENCE_PREPARATION_FAILURE: accepted blob mismatch')
    source = tmp_path / 'accepted-b1-tech.py'
    source.write_bytes(raw)
    provenance = {'accepted_base': ACCEPTED_B1_SOURCE_REF,
                  'tested_sha': guard.report_metadata().get('tested_sha'),
                  'retrieved_source_ref': source_ref, 'reference_path': 'tech.py',
                  'candidate_source_ref': 'HEAD', 'candidate_tech_blob': head_blob,
                  'candidate_source_sha256': hashlib.sha256(head_raw).hexdigest(),
                  'candidate_identity_transformations': transformation_count,
                  'reference_preparation_method': method,
                  'reference_transformations': reference_transformations,
                  'expected_reference_tech_blob': EXPECTED_ACCEPTED_B1_TECH_BLOB,
                  'actual_reference_tech_blob': actual_blob,
                  'source_sha256': hashlib.sha256(raw).hexdigest(),
                  'native_commands': records, 'native_profile': _trix_profile(schema),
                  'reference_preparation': 'PASS'}
    (tmp_path / 'accepted-b1-reference-provenance.json').write_text(
        json.dumps(provenance, sort_keys=True), encoding='utf-8')
    try:
        return _load_module(source)
    except Exception:
        pytest.fail('ACCEPTED_B1_REFERENCE_PREPARATION_FAILURE: complete module import failed')


def _fixture():
    return pd.read_csv(FIXTURES / 'ohlc.csv')


def _input(root, suffix, frame=None, filename=None):
    directory = root / 'datasets'
    directory.mkdir(parents=True)
    path = directory / (filename or 'b1_fixture.' + suffix)
    if suffix == 'csv' and frame is None:
        path.write_bytes((FIXTURES / 'ohlc.csv').read_bytes())
    elif suffix == 'csv':
        frame.to_csv(path, index=False)
    else:
        (frame if frame is not None else _fixture()).to_excel(path, index=False)
    return path


def _read(path):
    return pd.read_csv(path) if path.suffix == '.csv' else pd.read_excel(path)


def _calculate(module, path, monkeypatch):
    monkeypatch.setattr(module, 'current_path', str(path.parent.parent))
    assert module.calculator(path.name) is None
    return _read(path)


def _pair(reference, candidate, tmp_path, monkeypatch, suffix):
    seed = _input(tmp_path / 'seed', suffix)
    input_frame = _read(seed)
    paths = []
    outputs = []
    for name, module in [('reference', reference), ('candidate', candidate)]:
        destination = tmp_path / name / 'datasets'
        destination.mkdir(parents=True)
        path = destination / seed.name
        path.write_bytes(seed.read_bytes())
        paths.append(path)
        outputs.append(_calculate(module, path, monkeypatch))
    return input_frame, outputs[0], outputs[1], paths[1]


def _assert_approx(actual, expected, tolerance, label):
    # Shape first: core allclose must never accept broadcasting.
    assert actual.shape == expected.shape, label + ' shape mismatch'
    assert np.array_equal(np.isnan(actual), np.isnan(expected)), label + ' NaN mask mismatch'
    assert np.allclose(actual, expected, equal_nan=True, **tolerance), label + ' value mismatch'


def _historical_projection(output, schema):
    assert output.columns.tolist() == schema['candidate_columns']
    projected = output.drop(columns=['TRIX'])
    assert projected.columns.tolist() == schema['columns']
    return projected


def _assert_trix_native(input_frame, output, schema):
    profile = _trix_profile(schema)
    assert output['TRIX'].dtype == np.dtype('float64')
    for ticker in input_frame['ticker'].drop_duplicates():
        mask = input_frame['ticker'] == ticker
        expected = talib.TRIX(input_frame.loc[mask, 'close'].to_numpy(dtype=float),
                              **schema['trix_parameters'])
        actual = output.loc[mask, 'TRIX'].to_numpy()
        _assert_approx(actual, expected, schema['tolerance'], 'TRIX30 ' + ticker)
        assert np.isnan(actual[:profile['lookback']]).all()
        assert np.isfinite(actual[profile['lookback']:]).all()
    assert _trix_profile(schema) == profile


def _assert_accepted_b1(input_frame, before, after, schema, *, extra_columns=()):
    baseline_columns = list(schema['columns'])
    candidate_columns = list(schema['candidate_columns'])
    insertion = len(schema['raw_columns'])
    baseline_columns[insertion:insertion] = extra_columns
    candidate_columns[insertion:insertion] = extra_columns
    assert before.columns.tolist() == baseline_columns
    assert after.columns.tolist() == candidate_columns
    pd.testing.assert_frame_equal(before, after.drop(columns=['TRIX']),
                                  check_exact=True, check_dtype=True)
    assert len(before) == len(after) == len(input_frame)
    pd.testing.assert_frame_equal(after[schema['raw_columns']], input_frame[schema['raw_columns']],
                                  check_exact=False, check_dtype=True, **schema['tolerance'])
    pd.testing.assert_frame_equal(after[['ticker', 'begin']], input_frame[['ticker', 'begin']],
                                  check_exact=True, check_dtype=True)
    assert not after[['ticker', 'begin']].duplicated().any()
    _assert_native(input_frame, after, schema)
    _assert_trix_native(input_frame, after, schema)


@pytest.mark.parametrize('actual, expected, failure', [
    ([1.0, 2.0], [[1.0, 2.0]], 'shape mismatch'),
    ([np.nan, 1.0], [0.0, 1.0], 'NaN mask mismatch'),
    ([1.0, 2.0 + 1e-8], [1.0, 2.0], 'value mismatch'),
])
def test_assertion_controls_reject_shape_nan_and_value(actual, expected, failure, schema):
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter('always')
        with pytest.raises(AssertionError, match=failure):
            _assert_approx(np.array(actual), np.array(expected), schema['tolerance'], 'control')
    assert recorded == []


@pytest.mark.parametrize('actual, expected', [
    ([np.nan, np.inf, -np.inf, 1.0], [np.nan, np.inf, -np.inf, 1.0]),
    ([1.0 + 5e-13, 2.0 - 5e-13], [1.0, 2.0]),
])
def test_assertion_controls_accept_equal_and_within_tolerance(actual, expected, schema):
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter('always')
        _assert_approx(np.array(actual), np.array(expected), schema['tolerance'], 'control')
    assert recorded == []


def _assert_native(input_frame, output, schema, require_difference=False):
    for ticker in input_frame['ticker'].drop_duplicates():
        mask = input_frame['ticker'] == ticker
        instrument = input_frame.loc[mask]
        slowk, slowd = talib.STOCH(instrument['high'].to_numpy(dtype=float),
                                 instrument['low'].to_numpy(dtype=float),
                                 instrument['close'].to_numpy(dtype=float),
                                 **schema['stoch_parameters'])
        if require_difference:
            assert np.any(np.isfinite(slowk) & np.isfinite(slowd) & (np.abs(slowk - slowd) > 1e-8))
        for column, expected in [('STOCH_slowk', slowk), ('STOCH_slowd', slowd)]:
            actual = output.loc[mask, column].to_numpy()
            _assert_approx(actual, expected, schema['tolerance'],
                           column + ' must match the native STOCH output (slowd is second)')
        rsi = talib.RSI(instrument['close'].to_numpy(dtype=float), timeperiod=14)
        _assert_approx(output.loc[mask, 'RSI14'], rsi, schema['tolerance'], 'RSI14')


@pytest.mark.parametrize('suffix', ['csv', 'xlsx'])
def test_stoch_slowd_matches_native_second_output(reference, candidate, tmp_path, monkeypatch, schema, suffix):
    input_frame, before, after, _ = _pair(reference, candidate, tmp_path, monkeypatch, suffix)
    if before.columns.tolist() != schema['columns']:
        pytest.fail('RUNTIME_BASELINE_H88_MISMATCH')
    after = _historical_projection(after, schema)
    assert len(after) == schema['rows'] == 333
    assert after[['ticker', 'begin']].to_records(index=False).tolist() == input_frame[['ticker', 'begin']].to_records(index=False).tolist()
    _assert_native(input_frame, after, schema, require_difference=True)


@pytest.mark.parametrize('suffix', ['csv', 'xlsx'])
def test_before_after_preserves_every_other_column(reference, candidate, tmp_path, monkeypatch, schema, suffix):
    input_frame, before, after, _ = _pair(reference, candidate, tmp_path, monkeypatch, suffix)
    after = _historical_projection(after, schema)
    assert before.columns.tolist() == after.columns.tolist() == schema['columns']
    pd.testing.assert_frame_equal(before.drop(columns=['STOCH_slowd']), after.drop(columns=['STOCH_slowd']),
                                  check_exact=True, check_dtype=True)
    pd.testing.assert_frame_equal(after[schema['raw_columns']], input_frame, check_exact=False,
                                  **schema['tolerance'])
    assert not after[['ticker', 'begin']].duplicated().any()


@pytest.mark.parametrize('suffix', ['csv', 'xlsx'])
def test_repeat_calculator_matches_reference_on_identical_pass_inputs(reference, candidate, tmp_path, monkeypatch, schema, suffix):
    # Same-format, same-pass inputs prove reference equivalence; replay checks STOCH/RSI.
    # Full CSV idempotence remains mandatory B3 work (Refs #54, #63).
    initial, reference_first, first, path = _pair(reference, candidate, tmp_path / 'repeat', monkeypatch, suffix)
    reference_path = tmp_path / 'repeat' / 'reference' / 'datasets' / path.name
    # Replay one saved enriched input: both implementations receive identical bytes.
    replay_bytes = path.read_bytes()
    reference_path.write_bytes(replay_bytes)
    assert path.read_bytes() == reference_path.read_bytes() == replay_bytes
    replay_input = _read(path)[schema['raw_columns']]
    reference_second = _calculate(reference, reference_path, monkeypatch)
    second = _calculate(candidate, path, monkeypatch)
    first = _historical_projection(first, schema)
    second = _historical_projection(second, schema)
    for actual, expected, parsed_input in [(first, reference_first, initial),
                                           (second, reference_second, replay_input)]:
        assert actual.columns.tolist() == expected.columns.tolist() == schema['columns']
        assert len(actual) == len(expected) == schema['rows'] == 333
        assert actual.dtypes.equals(expected.dtypes)
        pd.testing.assert_frame_equal(actual.drop(columns=['STOCH_slowd']),
                                      expected.drop(columns=['STOCH_slowd']),
                                      check_exact=True, check_dtype=True)
        pd.testing.assert_frame_equal(actual[schema['raw_columns']], parsed_input,
                                      check_exact=False, check_dtype=True, **schema['tolerance'])
        pd.testing.assert_frame_equal(actual[['ticker', 'begin']], parsed_input[['ticker', 'begin']],
                                      check_exact=True, check_dtype=True)
        assert not actual[['ticker', 'begin']].duplicated().any()
        _assert_native(parsed_input, actual, schema)
    assert first.dtypes.equals(second.dtypes)
    pd.testing.assert_frame_equal(first.isna(), second.isna(), check_exact=True, check_dtype=True)
    for column in ['STOCH_slowk', 'STOCH_slowd', 'RSI14']:
        _assert_approx(second[column].to_numpy(), first[column].to_numpy(),
                       schema['tolerance'], column + ' repeat fixture evidence')

@pytest.mark.parametrize('kind', ['short', 'flat'])
def test_short_and_flat_match_native_masks(candidate, tmp_path, monkeypatch, schema, kind):
    frame = _fixture()
    if kind == 'short':
        frame = frame.groupby('ticker', sort=False).head(7).reset_index(drop=True)
    else:
        frame.loc[:, ['open', 'close', 'high', 'low']] = 100.0
    path = _input(tmp_path / kind, 'csv', frame)
    output = _calculate(candidate, path, monkeypatch)
    assert output.columns.tolist() == schema['candidate_columns']
    _assert_native(_read(path)[schema['raw_columns']], output, schema)
    _assert_trix_native(_read(path)[schema['raw_columns']], output, schema)
    if kind == 'short':
        assert output[['STOCH_slowk', 'STOCH_slowd']].isna().all().all()
        assert output['TRIX'].isna().all()
    else:
        for ticker in frame['ticker'].drop_duplicates():
            values = output.loc[frame['ticker'] == ticker, 'TRIX'].iloc[schema['trix_lookback']:]
            _assert_approx(values.to_numpy(), np.zeros(len(values)), schema['tolerance'],
                           'TRIX30 flat fixture evidence')


def test_csv_xlsx_stoch_rsi_parity_and_reference_equivalence(reference, candidate, tmp_path, monkeypatch, schema):
    # Fixture evidence: reference within each format, STOCH/RSI across formats.
    # Full derived CSV/XLSX parity and equality of all inferred pandas output dtypes
    # across formats remain mandatory B3 work (Refs #54, #63).
    outputs = []
    trix_outputs = []
    for suffix in ['csv', 'xlsx']:
        parsed_input, expected, actual, _ = _pair(reference, candidate, tmp_path / suffix,
                                                  monkeypatch, suffix)
        _assert_trix_native(parsed_input, actual, schema)
        trix_outputs.append(actual['TRIX'])
        actual = _historical_projection(actual, schema)
        assert actual.columns.tolist() == expected.columns.tolist() == schema['columns']
        assert len(actual) == len(expected) == schema['rows'] == 333
        assert actual.dtypes.equals(expected.dtypes)
        pd.testing.assert_frame_equal(actual.drop(columns=['STOCH_slowd']),
                                      expected.drop(columns=['STOCH_slowd']),
                                      check_exact=True, check_dtype=True)
        pd.testing.assert_frame_equal(actual[schema['raw_columns']], parsed_input,
                                      check_exact=False, check_dtype=True, **schema['tolerance'])
        pd.testing.assert_frame_equal(actual[['ticker', 'begin']], parsed_input[['ticker', 'begin']],
                                      check_exact=True, check_dtype=True)
        assert not actual[['ticker', 'begin']].duplicated().any()
        _assert_native(parsed_input, actual, schema)
        outputs.append(actual)
    csv, xlsx = outputs
    assert csv.columns.tolist() == xlsx.columns.tolist() == schema['columns']
    pd.testing.assert_frame_equal(csv[schema['raw_columns']], xlsx[schema['raw_columns']],
                                  check_exact=False, check_dtype=True, **schema['tolerance'])
    pd.testing.assert_frame_equal(csv.isna(), xlsx.isna(), check_exact=True, check_dtype=True)
    for column in ['STOCH_slowk', 'STOCH_slowd', 'RSI14']:
        _assert_approx(csv[column].to_numpy(), xlsx[column].to_numpy(),
                       schema['tolerance'], column + ' CSV/XLSX fixture evidence')
    _assert_approx(trix_outputs[0].to_numpy(), trix_outputs[1].to_numpy(),
                   schema['tolerance'], 'TRIX30 CSV/XLSX fixture evidence')


@pytest.mark.parametrize('suffix', ['csv', 'xlsx'])
def test_trix_preserves_accepted_b1_on_first_and_repeated_pass(
        accepted_b1_reference, candidate, tmp_path, monkeypatch, schema, suffix):
    initial, before, first, path = _pair(accepted_b1_reference, candidate, tmp_path,
                                       monkeypatch, suffix)
    assert len(initial) == schema['rows'] == 333
    _assert_accepted_b1(initial, before, first, schema)
    reference_path = tmp_path / 'reference' / 'datasets' / path.name
    replay_bytes = path.read_bytes()
    reference_path.write_bytes(replay_bytes)
    assert path.read_bytes() == reference_path.read_bytes() == replay_bytes
    replay_input = _read(path)
    before_second = _calculate(accepted_b1_reference, reference_path, monkeypatch)
    second = _calculate(candidate, path, monkeypatch)
    _assert_accepted_b1(replay_input, before_second, second, schema)
    assert first['TRIX'].dtype == second['TRIX'].dtype
    _assert_approx(second['TRIX'].to_numpy(), first['TRIX'].to_numpy(),
                   schema['tolerance'], 'TRIX30 repeated calculator')


@pytest.mark.parametrize('suffix', ['csv', 'xlsx'])
@pytest.mark.parametrize('instrument_rows', [7, 88, 89])
def test_trix_warmup_boundary_matches_native(candidate, tmp_path, monkeypatch, schema,
                                            suffix, instrument_rows):
    frame = _fixture().groupby('ticker', sort=False).head(instrument_rows).reset_index(drop=True)
    path = _input(tmp_path / 'warmup', suffix, frame)
    parsed_input = _read(path)
    output = _calculate(candidate, path, monkeypatch)
    assert output.columns.tolist() == schema['candidate_columns']
    assert len(output) == 2 * instrument_rows
    pd.testing.assert_frame_equal(output[['ticker', 'begin']], parsed_input[['ticker', 'begin']],
                                  check_exact=True, check_dtype=True)
    _assert_trix_native(parsed_input, output, schema)
    for ticker in parsed_input['ticker'].drop_duplicates():
        values = output.loc[parsed_input['ticker'] == ticker, 'TRIX'].to_numpy()
        assert np.isfinite(values).sum() == max(0, instrument_rows - schema['trix_lookback'])


@pytest.mark.parametrize('suffix', ['csv', 'xlsx'])
def test_trix_replaces_stale_column_preserves_extra_and_replays(
        accepted_b1_reference, candidate, tmp_path, monkeypatch, schema, suffix):
    extra = 'B1_KEEP_EXTRA'
    frame = _fixture()
    frame['TRIX'] = -999.0
    frame[extra] = np.arange(len(frame), dtype=np.int64)
    seed = _input(tmp_path / 'seed', suffix, frame)
    initial = _read(seed)
    outputs = []
    paths = []
    for name, module in [('reference', accepted_b1_reference), ('candidate', candidate)]:
        destination = tmp_path / name / 'datasets'
        destination.mkdir(parents=True)
        path = destination / seed.name
        path.write_bytes(seed.read_bytes())
        paths.append(path)
        outputs.append(_calculate(module, path, monkeypatch))
    before, first = outputs
    _assert_accepted_b1(initial, before, first, schema, extra_columns=[extra])
    pd.testing.assert_series_equal(first[extra], initial[extra], check_exact=True, check_dtype=True)
    assert first.columns.tolist().count('TRIX') == 1
    assert not (first['TRIX'] == -999.0).any()
    reference_path, path = paths
    replay_bytes = path.read_bytes()
    reference_path.write_bytes(replay_bytes)
    assert path.read_bytes() == reference_path.read_bytes() == replay_bytes
    replay_input = _read(path)
    before_second = _calculate(accepted_b1_reference, reference_path, monkeypatch)
    second = _calculate(candidate, path, monkeypatch)
    _assert_accepted_b1(replay_input, before_second, second, schema, extra_columns=[extra])
    pd.testing.assert_series_equal(second[extra], first[extra], check_exact=True, check_dtype=True)
    assert second.columns.tolist().count('TRIX') == 1
    assert first['TRIX'].dtype == second['TRIX'].dtype
    _assert_approx(second['TRIX'].to_numpy(), first['TRIX'].to_numpy(),
                   schema['tolerance'], 'TRIX30 stale-column replay')


def _primary():
    return RuntimeError(CANARY + ' ' + PRIVATE_PATH)


def _fail_stoch(monkeypatch, primary):
    def stoch_failure(*args, **kwargs):
        raise primary
    monkeypatch.setattr(talib, 'STOCH', stoch_failure)


def _fail_trix(monkeypatch, primary):
    def trix_failure(*args, **kwargs):
        raise primary
    monkeypatch.setattr(talib, 'TRIX', trix_failure)


def _assert_primary(caught, primary):
    assert caught.value is primary
    assert any(frame.name in {'stoch_failure', 'trix_failure', 'reader_failure', 'writer_failure'}
               for frame in traceback.extract_tb(caught.value.__traceback__))


@pytest.mark.parametrize('suffix', ['csv', 'xlsx'])
@pytest.mark.parametrize('phase', ['read', 'stoch', 'trix', 'pre_writer', 'partial_writer'])
def test_io_fault_boundaries_preserve_only_proven_prewrite(candidate, tmp_path, monkeypatch, suffix, phase):
    path = _input(tmp_path / 'fault', suffix)
    original_bytes = path.read_bytes()
    fixture_hash = hashlib.sha256((FIXTURES / 'ohlc.csv').read_bytes()).hexdigest()
    primary = _primary()
    writer_calls = []
    writer_name = 'to_csv' if suffix == 'csv' else 'to_excel'
    real_writer = getattr(pd.DataFrame, writer_name)
    def writer_failure(frame, destination, *args, **kwargs):
        writer_calls.append(str(destination))
        if phase == 'partial_writer':
            Path(destination).write_bytes(b'B1_PARTIAL_WRITE\n')
        if phase in {'pre_writer', 'partial_writer'}:
            raise primary
        return real_writer(frame, destination, *args, **kwargs)
    monkeypatch.setattr(pd.DataFrame, writer_name, writer_failure)
    if phase == 'read':
        def reader_failure(*args, **kwargs):
            raise primary
        monkeypatch.setattr(pd, 'read_csv' if suffix == 'csv' else 'read_excel', reader_failure)
    elif phase == 'stoch':
        _fail_stoch(monkeypatch, primary)
    elif phase == 'trix':
        _fail_trix(monkeypatch, primary)
    monkeypatch.setattr(candidate, 'current_path', str(path.parent.parent))
    with pytest.raises(RuntimeError) as caught:
        candidate.calculator(path.name)
    _assert_primary(caught, primary)
    assert len(writer_calls) == (1 if phase in {'pre_writer', 'partial_writer'} else 0)
    if phase == 'partial_writer':
        assert path.read_bytes() == b'B1_PARTIAL_WRITE\n'
        assert path.read_bytes() != original_bytes
    else:
        assert path.read_bytes() == original_bytes
    assert hashlib.sha256((FIXTURES / 'ohlc.csv').read_bytes()).hexdigest() == fixture_hash


def _recording_context(monkeypatch, root):
    import run_logging
    context = {'config': run_logging.validate_logging_config(), '_checkout': root,
               '_secrets': (CANARY, PRIVATE_PATH), 'run_id': 'b1-contract',
               'producer_sha': REVIEWED_BASE, 'worker_id': None}
    events = []
    def captured_emit(context, level, event, message, fields):
        events.append(run_logging._event(context, level, event, message, fields))
        return {'accepted': True, 'confirmed': level == 'ERROR'}
    monkeypatch.setattr(run_logging, 'emit_event', captured_emit)
    return context, events


def test_contextual_telemetry_is_pure_contract_with_canaries(candidate, tmp_path, monkeypatch, capsys, schema):
    frame = _fixture()
    frame.loc[frame['ticker'] == 'B1_SYNTH_A', 'ticker'] = CANARY
    path = _input(tmp_path / 'canary', 'csv', frame, CANARY + '.csv')
    context, events = _recording_context(monkeypatch, path.parent.parent)
    monkeypatch.setattr(candidate, 'current_path', str(path.parent.parent))
    assert candidate.calculator(path.name, logging_context=context) is None
    names = [event['event'] for event in events]
    assert names[0] == 'calculator_started'
    assert names[-1] == 'calculator_returned'
    assert names.count('calculator_instrument_returned') == 2
    assert all(event['stage'] == 'tech' and event['run_id'] == 'b1-contract' for event in events)
    assert all(event['duration_ms'] is None or event['duration_ms'] >= 0 for event in events)
    returned = events[-1]
    assert returned['outcome'] == 'RETURNED'
    assert returned['counts']['rows_received'] == 333
    assert returned['counts']['instruments'] == 2
    assert returned['counts']['rows_accepted'] is None
    assert '5/3/3' in events[0]['message'] and '0/0' in events[0]['message']
    assert 'columns=89' in returned['message']
    captured = capsys.readouterr()
    serialized = json.dumps(events) + captured.out + captured.err
    assert CANARY not in serialized and PRIVATE_PATH not in serialized
    assert 'TECH_DIAGNOSTICS_FAILED' not in captured.out + captured.err
    assert _read(path).columns.tolist() == schema['candidate_columns']


@pytest.mark.parametrize('contextual', [False, True])
def test_primary_error_diagnostics_have_no_raw_duplicate(candidate, tmp_path, monkeypatch, capsys, contextual):
    path = _input(tmp_path / 'error', 'csv', filename=CANARY + '.csv')
    context, events = _recording_context(monkeypatch, path.parent.parent)
    _fail_stoch(monkeypatch, primary := _primary())
    monkeypatch.setattr(candidate, 'current_path', str(path.parent.parent))
    with pytest.raises(RuntimeError) as caught:
        candidate.calculator(path.name, logging_context=context if contextual else None)
    _assert_primary(caught, primary)
    captured = capsys.readouterr()
    serialized = json.dumps(events) + captured.out + captured.err
    assert CANARY not in serialized and PRIVATE_PATH not in serialized
    if contextual:
        failures = [event for event in events if event['level'] == 'ERROR']
        assert len(failures) == 1
        assert failures[0]['event'] == 'calculator_failed'
        assert failures[0]['error']['exception_type'] == 'RuntimeError'
        assert '<redacted>' in failures[0]['error']['message']
    else:
        assert events == []
        assert captured.out.count('TECH_CALCULATOR_FAILED') == 1


@pytest.mark.parametrize('fault', ['make_error', 'emit', 'unconfirmed', 'fallback_console'])
def test_diagnostic_fault_never_replaces_primary(candidate, tmp_path, monkeypatch, capsys, fault):
    import run_logging
    path = _input(tmp_path / fault, 'csv', filename=CANARY + '.csv')
    before = path.read_bytes()
    context, events = _recording_context(monkeypatch, path.parent.parent)
    _fail_stoch(monkeypatch, primary := _primary())
    diagnostic = RuntimeError('DIAGNOSTIC_' + CANARY)
    if fault == 'make_error':
        def failed_make_error(*args, **kwargs):
            raise diagnostic
        monkeypatch.setattr(run_logging, 'make_error', failed_make_error)
    else:
        real_emit = run_logging.emit_event
        def failed_emit(context, level, event, message, fields):
            if level == 'ERROR':
                if fault == 'unconfirmed':
                    return {'accepted': True, 'confirmed': False}
                raise diagnostic
            return real_emit(context, level, event, message, fields)
        monkeypatch.setattr(run_logging, 'emit_event', failed_emit)
    if fault == 'fallback_console':
        def failed_print(*args, **kwargs):
            raise diagnostic
        monkeypatch.setattr(candidate, 'print', failed_print, raising=False)
    monkeypatch.setattr(candidate, 'current_path', str(path.parent.parent))
    with pytest.raises(RuntimeError) as caught:
        candidate.calculator(path.name, logging_context=context)
    _assert_primary(caught, primary)
    assert path.read_bytes() == before
    captured = capsys.readouterr()
    serialized = json.dumps(events) + captured.out + captured.err
    assert CANARY not in serialized and PRIVATE_PATH not in serialized
    if fault != 'fallback_console':
        assert captured.err.count('TECH_DIAGNOSTICS_FAILED') == 1


def test_diagnostic_failure_after_write_does_not_claim_preservation(candidate, tmp_path, monkeypatch, capsys, schema):
    import run_logging
    path = _input(tmp_path / 'written', 'csv')
    before = path.read_bytes()
    context, events = _recording_context(monkeypatch, path.parent.parent)
    real_emit = run_logging.emit_event
    def failed_returned(context, level, event, message, fields):
        if event == 'calculator_returned':
            raise RuntimeError(CANARY)
        return real_emit(context, level, event, message, fields)
    monkeypatch.setattr(run_logging, 'emit_event', failed_returned)
    monkeypatch.setattr(candidate, 'current_path', str(path.parent.parent))
    assert candidate.calculator(path.name, logging_context=context) is None
    assert path.read_bytes() != before
    assert _read(path).columns.tolist() == schema['candidate_columns']
    captured = capsys.readouterr()
    assert 'TECH_DIAGNOSTICS_FAILED' in captured.err
    assert 'WRITTEN' in captured.err
    assert CANARY not in captured.out + captured.err


def test_none_context_never_creates_logging_resources(candidate, tmp_path, monkeypatch, capsys):
    import run_logging
    def forbidden(*args, **kwargs):
        raise AssertionError('Automatic logger or emitter forbidden')
    for name in ['configure_logging', 'configure_worker', 'prepare_worker', 'emit_event', 'make_error']:
        monkeypatch.setattr(run_logging, name, forbidden)
    path = _input(tmp_path / 'no_context', 'csv', filename=CANARY + '.csv')
    monkeypatch.setattr(candidate, 'current_path', str(path.parent.parent))
    assert candidate.calculator(path.name) is None
    captured = capsys.readouterr()
    assert CANARY not in captured.out + captured.err
    assert 'TECH_DIAGNOSTICS_FAILED' not in captured.out + captured.err


@pytest.mark.parametrize('contextual', [False, True])
def test_main_catches_exception_and_continues_without_raw_print(candidate, tmp_path, monkeypatch, capsys, contextual):
    root = tmp_path / 'main'
    good = _input(root, 'csv')
    bad = good.parent / (CANARY + '.csv')
    bad.write_bytes(good.read_bytes())
    (good.parent / '~$ignored.csv').write_bytes(b'ignored')
    context, events = _recording_context(monkeypatch, root)
    primary = _primary()
    actual_reader = pd.read_csv
    def reader_failure(path, *args, **kwargs):
        if Path(path).name == bad.name:
            raise primary
        return actual_reader(path, *args, **kwargs)
    monkeypatch.setattr(pd, 'read_csv', reader_failure)
    actual_calculator = candidate.calculator
    attempted = []
    def tracked_calculator(filename, *, logging_context=None):
        attempted.append(filename)
        return actual_calculator(filename, logging_context=logging_context)
    monkeypatch.setattr(candidate, 'calculator', tracked_calculator)
    monkeypatch.setattr(candidate, 'current_path', str(root))
    assert candidate.main(str(root), logging_context=context if contextual else None) is None
    assert set(attempted) == {good.name, bad.name}
    assert 'STOCH_slowd' in actual_reader(good).columns
    captured = capsys.readouterr()
    serialized = json.dumps(events) + captured.out + captured.err
    assert CANARY not in serialized and PRIVATE_PATH not in serialized
    if contextual:
        assert sum(event['event'] == 'calculator_failed' for event in events) == 1
    else:
        assert captured.out.count('TECH_CALCULATOR_FAILED') == 1
