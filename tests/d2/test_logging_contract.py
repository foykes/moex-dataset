"""Control-flow and provisional receipt checks, separate from native child proof."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import types
import uuid

import pytest


def local_result(upload, run_id='r' * 32):
    plan = {'google':[{'output_id':'google-000001','group':'google'}],
            'ftp':[{'output_id':'ftp-000001','group':'root'}]}
    result = upload._d2_result(run_id, 'a' * 32, 'b' * 40, plan)
    for order, row in enumerate(result['workers'].values(), 1):
        row.update(started=True, pid=order, exitcode=0, settled=True, alive=False,
                   timed_out=False, cleanup_confirmed=True, checkpoint_state='VALID',
                   execution_state='RETURNED', normal_return_observed=True,
                   diagnostics_state='COMPLETE', error_code=None)
        for output in row['outputs']:
            output.update(transfer_state='ACKNOWLEDGED', commit_state='UNKNOWN')
    result['parent_outcome'] = 'RETURNED'
    upload._d2_aggregate(result)
    return result


def final_report(run_id='r' * 32):
    return dict(run_id=run_id, producer_sha='b' * 40, exit_status=0,
                delivery_outcome='COMPLETE', storage_sealed=True,
                evidence_incomplete=False, execution_outcome='COMPLETED')


def load_pipeline(project_root, monkeypatch, upload_call, helpers=None):
    for name in ('data_gathering','dividends','tech','dohodru_data'):
        module = types.ModuleType(name)
        module.main = lambda *args, **kwargs: None
        monkeypatch.setitem(sys.modules, name, module)
    fake_upload = types.ModuleType('upload')
    fake_upload.main = upload_call
    if helpers is not None:
        fake_upload._d2_apply_finalization = helpers._d2_apply_finalization
        fake_upload._d2_completion_allowed = helpers._d2_completion_allowed
    monkeypatch.setitem(sys.modules, 'upload', fake_upload)
    specification = importlib.util.spec_from_file_location('d2_pipeline_' + uuid.uuid4().hex,
                                                          project_root / 'main.py')
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def fake_log(monkeypatch):
    calls = []
    module = types.ModuleType('run_logging')
    module.initialize_main_stages = lambda context: calls.append(('initialize',context))
    module.check_logging_health = lambda context: {'healthy':True}
    module.emit_event = lambda *args, **kwargs: {'confirmed':True}
    module.observe_stage = lambda *args, **kwargs: None
    module.mark_unreached = lambda *args, **kwargs: None
    module.make_error = lambda context, error, stage, **kwargs: {'code':'SAFE'}
    monkeypatch.setitem(sys.modules, 'run_logging', module)
    return module, calls


def test_main_old_positional_handoff_stays_compatible(project_root, monkeypatch, capsys):
    calls = []
    def upload_call(*args, **kwargs):
        calls.append((args,kwargs))
    pipeline = load_pipeline(project_root, monkeypatch, upload_call)
    assert pipeline.main() is None
    assert len(calls) == 1 and len(calls[0][0]) == 2
    assert calls[0][0][0] == str(project_root)
    assert calls[0][1] == {}
    capsys.readouterr()


@pytest.mark.parametrize('with_context', [False,True])
def test_run_stage_forwards_context_and_sink_exactly(project_root, upload, monkeypatch, capsys, with_context):
    fake_log(monkeypatch)
    context = {'run_id':'unit-run'} if with_context else None
    sink, calls = {}, []
    def upload_call(*args, **kwargs):
        calls.append(kwargs)
        assert kwargs['logging_context'] is context
        assert kwargs['result_sink'] is sink
        if not with_context:
            sink.update(local_result(upload))
            assert upload._d2_apply_finalization(sink, final_report()) == 0
    pipeline = load_pipeline(project_root, monkeypatch, upload_call, upload)
    assert pipeline.main(logging_context=context, result_sink=sink) is None
    assert len(calls) == 1
    output = capsys.readouterr()
    if with_context:
        assert 'полностью закончил работу' not in output.out


@pytest.mark.parametrize('fault', ['raises','incomplete','unsealed','nonzero','foreign_run'])
def test_parent_receipt_remains_provisional_after_finalization_fault(upload, tmp_path, fault):
    sink = local_result(upload)
    before_document = copy.deepcopy(sink)
    path = tmp_path / 'result.json'
    before = upload._d2_bytes(before_document)
    path.write_bytes(before)
    digest = hashlib.sha256(before).hexdigest()
    assert before_document['receipt_scope'] == 'PREFINAL_SUPERVISION'
    assert before_document['finalization_state'] == 'NOT_OBSERVED'
    assert before_document['diagnostic_success_allowed'] is False
    report = None if fault == 'raises' else final_report()
    if fault == 'incomplete': report.update(delivery_outcome='INCOMPLETE', evidence_incomplete=True)
    if fault == 'unsealed': report['storage_sealed'] = False
    if fault == 'nonzero': report['exit_status'] = 7
    if fault == 'foreign_run': report['run_id'] = 'foreign'
    assert upload._d2_apply_finalization(sink, report) != 0
    assert sink['diagnostics_state'] == 'INCOMPLETE'
    assert sink['final_exit_status'] != 0
    assert upload._d2_completion_allowed(sink) is False
    if fault == 'raises': assert sink['storage_sealed'] == 'UNKNOWN'
    assert path.read_bytes() == before
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    assert json.loads(path.read_bytes()) == before_document


@pytest.mark.parametrize('worker_fault', ['missing_worker','alive','exit23','no_final','no_normal_return'])
def test_real_flog_success_cannot_upgrade_bad_supervision(upload, worker_fault):
    sink = local_result(upload)
    worker = sink['workers']['ftp']
    if worker_fault == 'missing_worker': sink['workers'].pop('ftp')
    if worker_fault == 'alive': worker.update(settled=False, alive=True)
    if worker_fault == 'exit23': worker['exitcode'] = 23
    if worker_fault == 'no_final': worker['diagnostics_state'] = 'INCOMPLETE'
    if worker_fault == 'no_normal_return': worker['normal_return_observed'] = False
    assert upload._d2_apply_finalization(sink, final_report()) != 0
    assert sink['diagnostic_success_allowed'] is False
    assert sink['committed_outputs'] == []


@pytest.mark.parametrize('fault', ['none','raises','incomplete'])
def test_cli_one_parent_session_and_finalization_gate(project_root, upload, monkeypatch, tmp_path, capsys, fault):
    log, calls = fake_log(monkeypatch)
    seen = {}
    def configure(root, *, run_id, environment, producer_sha, config):
        assert root == project_root
        context = dict(run_id=run_id, producer_sha=producer_sha)
        calls.append(('configure',context))
        return context
    log.configure_logging = configure
    persisted = tmp_path / 'result.json'
    def upload_call(*args, **kwargs):
        context, sink = kwargs['logging_context'], kwargs['result_sink']
        seen.update(context=context, sink=sink)
        sink.update(local_result(upload, context['run_id']))
        sink['producer_sha'] = context['producer_sha']
        persisted.write_bytes(upload._d2_bytes(sink))
        seen['before'] = persisted.read_bytes()
    def finalize(context, **kwargs):
        calls.append(('finalize',context))
        assert context is seen['context']
        assert kwargs['release_result'] is None
        assert persisted.read_bytes() == seen['before']
        if fault == 'raises':
            raise RuntimeError('D2_SYNTHETIC_SECRET_CANARY')
        report = final_report(context['run_id'])
        report['producer_sha'] = context['producer_sha']
        if fault == 'incomplete': report.update(exit_status=1, delivery_outcome='INCOMPLETE', evidence_incomplete=True)
        return report
    log.finalize_logging = finalize
    pipeline = load_pipeline(project_root, monkeypatch, upload_call, upload)
    monkeypatch.setattr(pipeline, '_source_sha', lambda root: 'b' * 40)
    monkeypatch.setattr(sys, 'argv', ['main.py','--log-environment','offline'])
    status = pipeline._run_cli()
    output = capsys.readouterr()
    assert [name for name, _ in calls].count('configure') == 1
    assert [name for name, _ in calls].count('finalize') == 1
    assert seen['sink']['run_id'] == seen['context']['run_id']
    assert persisted.read_bytes() == seen['before']
    assert 'D2_SYNTHETIC_SECRET_CANARY' not in output.out + output.err
    if fault == 'none':
        assert status == 0
        assert seen['sink']['final_exit_status'] == 0
        assert 'полностью закончил работу' in output.out
    else:
        assert status != 0
        assert seen['sink']['diagnostics_state'] == 'INCOMPLETE'
        assert seen['sink']['final_exit_status'] != 0
        assert 'полностью закончил работу' not in output.out
