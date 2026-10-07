"""Reuse the real, pure F-LOG error constructor without opening log resources."""

import json
from pathlib import Path


def test_make_error_has_literal_redacted_contract():
    import run_logging
    root = Path(__file__).resolve().parents[2]
    context = {'_secrets': ('F3_CANARY_VALUE',), '_checkout': root,
               'config': run_logging.validate_logging_config(), 'run_id': 'f3-fixture'}
    record = run_logging.make_error(context, ValueError('F3_CANARY_VALUE'), 'source')
    assert record['category'] == 'UNKNOWN'
    assert record['code'] == 'APPLICATION_ERROR'
    assert record['stage'] == 'source'
    assert record['retryable'] is False
    assert record['final'] is True
    assert record['message'] == 'ValueError: "<redacted>"'
    assert record['run_id'] == 'f3-fixture'
    assert record['exception_type'] == 'ValueError'
    nullable = {'snapshot_id', 'dataset_id', 'instrument', 'interval', 'page', 'file',
                'artifact_id', 'target_id', 'endpoint', 'attempt'}
    assert all(record[name] is None for name in nullable)
    assert record['null_reasons'] == {name: 'NOT_AVAILABLE_OR_NOT_APPLICABLE' for name in nullable}
    assert set(record) == {'category', 'code', 'stage', 'retryable', 'final', 'message',
                           'run_id', 'exception_type', 'null_reasons', *nullable}
    assert 'F3_CANARY_VALUE' not in json.dumps(record)
