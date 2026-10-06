import json
import logging
from pathlib import Path
import uuid

import pytest

from _probe import ROOT


def test_first_logrecord_and_all_sinks_redacted(log, caplog, capsys):
    secret = 'private-CANARY_671'
    session = log.configure_logging(ROOT, run_id='redact-' + uuid.uuid4().hex, environment='offline',
        producer_sha='c4f2210', sensitive_values=(secret,))
    session['_logger'].addHandler(caplog.handler)
    try:
        raise ValueError(secret, {'API_KEY': secret, 'client Secret': secret})
    except ValueError as error:
        record = log.make_error(session, error, 'fixture')
    receipt = log.emit_event(session, 'ERROR', 'failure',
        secret + ' https://u:p@host.test/a?token=secret#fragment C:/outside/file with spaces',
        {'error': record, 'file': '\\\\host\\share\\' + secret, 'target_id': 'https://docs.google.com/spreadsheets/d/' + secret})
    assert receipt['confirmed']
    assert caplog.records
    for captured in caplog.records:
        assert secret not in captured.getMessage()
        assert captured.args == () and captured.exc_info is None and captured.exc_text is None and captured.stack_info is None
        assert secret not in json.dumps(captured.flog_event)
    report = log.finalize_logging(session, execution_outcome='FAILED', exit_status=1, application_error=record)
    assert len(report['errors']) == 1
    for path in session['_run_root'].iterdir():
        assert secret.encode() not in path.read_bytes()
    output = capsys.readouterr()
    assert secret not in output.out + output.err
    assert 'https://<private-target>' in output.err
    assert 'u:p' not in output.err


def test_arbitrary_objects_cycles_oversize_never_repr(log, session):
    class Trap:
        def __repr__(self):
            raise AssertionError('repr called')
        def __str__(self):
            raise AssertionError('str called')
    for value in (Trap(), float('nan'), 'x' * 70000):
        with pytest.raises(ValueError):
            log.emit_event(session, 'INFO', 'invalid', 'safe', {'file': value})
    cycle = []
    cycle.append(cycle)
    with pytest.raises(ValueError):
        log._sanitize(session, cycle)
    assert log.make_error(session, ValueError(Trap()), 'fixture')['message'].startswith('ValueError: <unsupported>')


@pytest.mark.parametrize('path', ['C:/private/a b.txt', '\\\\private\\share\\key.txt', '/opt/private/key.txt', '/mnt/private/data'])
def test_path_privacy(log, session, path):
    assert log._text(session, path) == '<external>'


def test_caller_error_sanitized_identically_in_summary(log, session):
    record = log.make_error(session, ValueError('safe'), 'fixture')
    record['message'] = 'password=RAW_CANARY_901'
    log.emit_event(session, 'ERROR', 'failure', 'safe', {'error': record})
    report = log.finalize_logging(session, execution_outcome='FAILED', exit_status=1, application_error=record)
    assert len(report['errors']) == 1
    assert 'RAW_CANARY_901' not in json.dumps(report)


def test_sensitive_identifier_refused_before_context(log):
    with pytest.raises(ValueError, match='LOG_ID_SENSITIVE'):
        log.configure_logging(ROOT, run_id='sensitive-' + uuid.uuid4().hex, environment='ID_CANARY',
            producer_sha='c4f2210', sensitive_values=('ID_CANARY',))


@pytest.mark.parametrize('message,secret', [
    ('Authorization: Bearer AUTH_CANARY_942', 'AUTH_CANARY_942'),
    ('Authorization: Basic AUTH_CANARY_942', 'AUTH_CANARY_942'),
    ('{"password":"JSON_CANARY_943"}', 'JSON_CANARY_943'),
    ("{'token':'JSON_CANARY_943'}", 'JSON_CANARY_943'),
    ('HTTPS://user:URL_CANARY_944@private.example/path', 'URL_CANARY_944'),
    ('file:///C:/private/PATH_CANARY_945/key.json', 'PATH_CANARY_945'),
    ('https://sheets.googleapis.com/v4/spreadsheets/PRIVATE_CANARY_ID/values', 'PRIVATE_CANARY_ID'),
    ('https://[fd00::1]/PRIVATE_CANARY_ID?q=key', 'PRIVATE_CANARY_ID'),
    ('Cookie: session=COOKIE_CANARY; second=COOKIE_CANARY', 'COOKIE_CANARY')])
def test_common_secret_forms_without_registration(log, session, capsys, message, secret):
    assert log.emit_event(session, 'ERROR', 'redaction', message, {})['confirmed']
    result = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    assert secret not in json.dumps(result)
    for path in session['_run_root'].iterdir():
        assert secret.encode() not in path.read_bytes()
    assert secret not in capsys.readouterr().err


def test_sensitive_worker_registration_rejected(log):
    context = log.configure_logging(ROOT, run_id='worker-secret-' + uuid.uuid4().hex,
        environment='offline', producer_sha='c4f2210', sensitive_values=('WORKER_CANARY',))
    with pytest.raises(ValueError, match='LOG_WORKER_SENSITIVE'):
        log.prepare_worker(context, 'WORKER_CANARY')
    assert context['_expected'] == {}
    log.finalize_logging(context, execution_outcome='COMPLETED', exit_status=0)


@pytest.mark.parametrize('secret', ['INFO', 'tech', 'run_id'])
def test_structural_secret_collision_rejected(log, secret):
    with pytest.raises(ValueError, match='LOG_ID_SENSITIVE'):
        log.configure_logging(ROOT, run_id='structural-' + uuid.uuid4().hex,
            environment='offline', producer_sha='c4f2210', sensitive_values=(secret,))
