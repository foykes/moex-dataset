"""Six bounded synthetic telemetry oracles; no native/default-command launch.

These nodes also run inside default-command probes. No recursion exemption or
skip is needed: each uses at most 32 in-memory records, each below 4 KiB.
"""

import io

import pytest
import mds_offline_guard as guard


def _identity():
    return dict(run_id='1' * 32, lane='f3', role='command', ticket_id='2' * 32,
                tested_sha='3' * 40, source_digest='4' * 64, pid=123, creation_100ns=456)


def _records():
    return [dict(_identity(), seq=n, mono_ns=n * 10, event=event, value=0, phase=0)
            for n, event in enumerate(('bootstrap', 'start_written', 'final_end', 'writer_end'), 1)]


def test_timing_identity_binds_run_lane_role_sha():
    records = _records()
    required = {'bootstrap', 'start_written', 'final_end', 'writer_end'}
    assert guard.timing_trace_errors(records, _identity(), required, 40) == []
    for key in guard._TIMING_ID_KEYS:
        foreign = [dict(row) for row in records]
        foreign[0][key] = 999 if key in {'pid', 'creation_100ns'} else 'FOREIGN'
        assert 'FOREIGN' in guard.timing_trace_errors(foreign, _identity(), required, 40), key


def test_timing_trace_rejects_missing_corrupt_or_foreign_records():
    required = {'bootstrap', 'start_written', 'final_end', 'writer_end'}
    original = _records()
    cases = [
        (original[:-1], 'MISSING'),
        ([None, *original[1:]], 'CORRUPT'),
        ([*original[:2], dict(original[2], tested_sha='a' * 40), original[3]], 'FOREIGN'),
        ([*original[:3], dict(original[3], mono_ns=41)], 'LATE'),
        ([*original, original[-1]], 'DUPLICATE'),
        ([dict(original[0], seq=[]), *original[1:]], 'CORRUPT'),
    ]
    for records, expected in cases:
        assert expected in guard.timing_trace_errors(records, _identity(), required, 40)
    assert _records() == original  # The oracle does not repair/normalize input.


def test_timing_result_cannot_override_native_failure():
    # Original outward failure codes remain independent, including a logical
    # pytest success followed by a native timeout/guard failure.
    for native in (86, 124, 1, 'NOT_AVAILABLE'):
        for valid in (True, False):
            result = guard.timing_verdict(native, valid=valid)
            assert result['NATIVE_VERDICT'] == native
            assert result['DIAGNOSTIC_VERDICT'] == ('COMPLETE' if valid else 'INVALID')
    assert guard.timing_verdict(0, valid=False) == {
        'NATIVE_VERDICT': 0, 'DIAGNOSTIC_VERDICT': 'INVALID'}


def test_timing_budget_boundaries_and_abort_use_fake_clock():
    # Exact nanoseconds from a recorded fake clock; no real sleep or performance test.
    stamps = iter((10, 1000000010, 10, 1000000011))
    start = next(stamps)
    elapsed = next(stamps) - start
    boundary = dict(cpu_percent=1, rss_bytes=134217728, sync_ns=elapsed,
                    telemetry_bytes=4194304)
    assert guard.timing_verdict(86, **boundary)['DIAGNOSTIC_VERDICT'] == 'COMPLETE'
    start = next(stamps)
    over = next(stamps) - start
    for changed in ({'cpu_percent': 1.000001}, {'rss_bytes': 134217729},
                    {'sync_ns': over}, {'telemetry_bytes': 4194305}):
        result = guard.timing_verdict(86, **{**boundary, **changed})
        assert result == {'NATIVE_VERDICT': 86, 'DIAGNOSTIC_VERDICT': 'ABORTED'}
    sink = io.BytesIO()
    assert guard.timing_store(sink, b'1234', 0, 4) == 4
    with pytest.raises(ValueError, match='TIMING_BYTES'):
        guard.timing_store(sink, b'5', 4, 4)
    assert sink.getvalue() == b'1234'


def test_timing_storage_failure_is_not_complete():
    class Refusal:
        def write(self, payload):
            raise OSError('synthetic refusal')

    class ShortWrite:
        def write(self, payload):
            return len(payload) - 1

    for store in (Refusal(), ShortWrite()):
        with pytest.raises(OSError):
            guard.timing_store(store, b'fixture', 0, 4096)
        assert guard.timing_verdict(0, stored=False) == {
            'NATIVE_VERDICT': 0, 'DIAGNOSTIC_VERDICT': 'ABORTED'}


def test_timing_public_report_excludes_synthetic_private_values():
    record = dict(_records()[0], argv='F3_SYNTHETIC_SECRET',
                  private_path='C:/synthetic/private', environment={'TOKEN': 'SYNTHETIC'},
                  exception='SYNTHETIC_EXCEPTION_PAYLOAD', stdout='RAW_SYNTHETIC_OUTPUT')
    public = guard.timing_public(record)
    assert public == _records()[0]
    assert all(value not in str(public) for value in (
        'F3_SYNTHETIC_SECRET', 'C:/synthetic/private', 'TOKEN',
        'SYNTHETIC_EXCEPTION_PAYLOAD', 'RAW_SYNTHETIC_OUTPUT'))
    malformed = dict(record, value='SYNTHETIC_PRIVATE_VALUE', role='C:/synthetic/private')
    assert 'value' not in guard.timing_public(malformed)
    assert 'role' not in guard.timing_public(malformed)
