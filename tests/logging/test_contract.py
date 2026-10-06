import copy
import datetime
import json
from pathlib import Path

import pytest

EVENT_KEYS = set('timestamp_utc level event stage message run_id producer_sha process worker snapshot_id release_id dataset_id instrument interval page file artifact_id target_id outcome elapsed_ms duration_ms counts retries error fields null_reasons'.split())
ERROR_KEYS = set('category code stage retryable final message run_id snapshot_id dataset_id instrument interval page file artifact_id target_id endpoint attempt exception_type null_reasons'.split())


def records(session):
    return [json.loads(line) for line in (session['_run_root'] / 'events.jsonl').read_text(encoding='utf-8').splitlines()]


def test_f0_envelope_unicode_levels_idempotence(log, session):
    context = json.loads((session['_run_root'] / 'context.json').read_text(encoding='utf-8'))
    assert context['versions']['python'] == '3.14.8'
    assert context['versions']['pandas'] == '2.3.3'
    assert context['versions']['TA-Lib'] == '0.6.8'
    for level in ('DEBUG', 'INFO', 'WARNING', 'ERROR'):
        log.emit_event(session, level, 'progress', 'Привет Москва', {'instrument': 'SBER', 'interval': 24, 'page': 0})
    assert log.flush_logging(session)['confirmed']
    assert log.configure_logging(session['_checkout'], run_id=session['run_id'], environment='offline',
        producer_sha='c4f2210', config=session['config'], session=session) is session
    with pytest.raises(ValueError, match='LOG_SESSION_CHANGED'):
        log.configure_logging(session['_checkout'], run_id=session['run_id'], environment='other',
            producer_sha='c4f2210', config=session['config'], session=session)
    values = records(session)
    assert [value['level'] for value in values] == ['DEBUG', 'INFO', 'WARNING', 'ERROR']
    for value in values:
        assert set(value) == EVENT_KEYS
        assert value['message'] == 'Привет Москва'
        assert value['fields'] == {}
        assert value['snapshot_id'] is None and value['counts'] is None
        assert datetime.datetime.fromisoformat(value['timestamp_utc'].replace('Z', '+00:00')).utcoffset().total_seconds() == 0
        for key, item in value.items():
            if item is None:
                assert value['null_reasons'][key]
    report = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert report['logical_event_counts_by_level'] == {'DEBUG': 1, 'INFO': 2, 'WARNING': 1, 'ERROR': 1}
    assert report['release_result'] is None
    assert report['observed_stages'] == []
    assert report['run_duration_ms'] >= 0
    assert report['delivery_outcome'] == 'COMPLETE'
    assert report['storage_sealed']
    assert log.finalize_logging(session, execution_outcome='FAILED', exit_status=8) is report


@pytest.mark.parametrize('config', [[], {'unknown': 1}, {'queue_size': True}, {'queue_size': 0},
    {'critical_deadline_s': float('nan')}, {'shutdown_deadline_s': float('inf')},
    {'file_level': 'NOTSET'}, {'max_workers': 17}, {'log_root': '../CANARY'},
    {'log_root': 'C:/CANARY'}, {'root_budget_bytes': 100}, {'rotation_bytes': 100}, {'queue_size': 10**400}])
def test_config_pure_safe_rejection(log, config):
    with pytest.raises(ValueError) as caught:
        log.validate_logging_config(config)
    assert 'CANARY' not in str(caught.value)


@pytest.mark.parametrize('fields', [{'bogus': 0}, {'interval': True}, {'duration_ms': -1},
    {'counts': {'unknown': 1}}, {'retries': {'attempt': True, 'limit': 3}},
    {'fields': {'ordinary_extra': 'secret'}}, {'error': {'message': 'secret'}}])
def test_invalid_f0_payloads(log, session, fields):
    with pytest.raises(ValueError):
        log.emit_event(session, 'INFO', 'invalid', 'safe', fields)


def test_accepted_f0_counts(log, session):
    log.emit_event(session, 'INFO', 'request_failed', 'Нет ответа', {'counts': {
        'rows_received': 0, 'rows_accepted': None, 'rows_quarantined': None,
        'pages': 1, 'instruments': 1, 'null_reasons': {'rows_accepted': 'NOT_MEASURED', 'rows_quarantined': 'NOT_MEASURED'}}})
    assert log.flush_logging(session)['confirmed']
    assert records(session)[0]['counts']['rows_accepted'] is None


def test_source_capacity_literals_and_event(log, session):
    fixture = json.loads((Path(__file__).parent / 'fixtures/capacity.json').read_text(encoding='utf-8'))
    objects = {item['name']: item['value'] for item in fixture['objects']}
    assert objects['xlsx_boundary_pass']['required_rows'] == 1048576
    assert objects['xlsx_boundary_pass']['required_cells'] == 10485760
    assert objects['sheets_boundary_pass']['workbook_cells_after'] == 101
    pending = next(copy.deepcopy(item['value']) for item in fixture['objects'] if item['type'] == 'event' and item['source_example'] == 'E18')
    fields = {k: v for k, v in pending.items() if k not in {'timestamp_utc', 'level', 'event', 'message', 'run_id', 'producer_sha', 'process', 'worker'}}
    fields['error']['run_id'] = session['run_id']
    fields['null_reasons'].pop('producer_sha', None)
    fields['null_reasons'].pop('worker', None)
    receipt = log.emit_event(session, 'ERROR', pending['event'], pending['message'], fields)
    assert receipt['confirmed']
    fields['fields']['capacity']['exit_status'] = None
    assert log.emit_event(session, 'ERROR', pending['event'], pending['message'], fields)['confirmed']
    result = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert result['exit_status'] == 1
    assert len(result['errors']) == 1
    assert set(result['errors'][0]) == ERROR_KEYS
    capacity = result['capacity_reports'][0]
    assert capacity['previous_verification'] == 'UNVERIFIED'
    assert capacity['previous_freshness'] == 'STALE'
    assert capacity['preservation_outcome'] == 'NOT_TOUCHED'
    assert capacity['release_status'] == 'BLOCKED'


@pytest.mark.parametrize('outcome,status', [('FAILED', 0), ('UNKNOWN', 0), ('COMPLETED', 1)])
def test_execution_contradiction_rejected(log, session, outcome, status):
    with pytest.raises(ValueError, match='CONTRADICTION'):
        log.finalize_logging(session, execution_outcome=outcome, exit_status=status)


@pytest.mark.parametrize('event', ['run_completed', 'worker_completed'])
def test_lifecycle_reserved_for_idempotent_finalizers(log, session, event):
    with pytest.raises(ValueError, match='LOG_LIFECYCLE_RESERVED'):
        log.emit_event(session, 'INFO', event, 'premature final', {})


def test_event_type_rejected_before_hashing(log, session):
    class HashTrap:
        def __hash__(self):
            raise AssertionError('event object must not be hashed')

    for event in ([], {}, None, HashTrap()):
        with pytest.raises(ValueError, match='LOG_EVENT_SCHEMA'):
            log.emit_event(session, 'INFO', event, 'safe', {})
    assert log.flush_logging(session)['confirmed']
    assert records(session) == []
