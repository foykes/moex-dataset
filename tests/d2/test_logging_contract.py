"""Finalization truth and entrypoint gates; unit doubles are never OS evidence."""

import copy
from contextlib import contextmanager
import hashlib
import json
import sys
import types

import pytest


def local_result(upload):
    plan = {'google': [{'output_id': 'google', 'group': 'google'}],
            'ftp': [{'output_id': 'ftp-0001', 'group': 'root'}]}
    result = upload._d2_result('a' * 32, 'b' * 32, 'c' * 40, plan)
    result['parent_outcome'] = 'RETURNED'
    for index, row in enumerate(result['workers'].values()):
        row.update(started=True, pid=9000 + index, exitcode=0, settled=True, alive=False,
                   timed_out=False, cleanup_confirmed=True, checkpoint_state='VALID',
                   execution_state='RETURNED', normal_return_observed=True,
                   diagnostics_state='COMPLETE', error_code=None)
        for item in row['outputs']:
            item.update(transfer_state='ACKNOWLEDGED', commit_state='UNKNOWN')
    return result


def report():
    return dict(run_id='a' * 32, producer_sha='c' * 40, execution_outcome='COMPLETED',
                exit_status=0, delivery_outcome='COMPLETE', storage_sealed=True, evidence_incomplete=False)


def runtime(sink, observed_report):
    value = types.SimpleNamespace(context={'_final': observed_report}, closed=True,
                                  cleanup_complete=True, case=None, owns_tracker=False)
    sink['_runtime'] = value
    return value


@pytest.mark.parametrize('fault', ['raises', 'incomplete', 'unsealed', 'nonzero', 'foreign_run', 'unobserved_report'])
def test_parent_receipt_remains_provisional_after_finalization_fault(upload, tmp_path, monkeypatch, fault):
    sink = local_result(upload)
    persisted = upload._d2_bytes(upload._d2_public_result(sink))
    path = tmp_path / 'parent-result.json'
    path.write_bytes(persisted)
    digest = hashlib.sha256(persisted).hexdigest()
    actual = None if fault == 'raises' else report()
    if fault == 'incomplete':
        actual.update(delivery_outcome='INCOMPLETE', evidence_incomplete=True)
    if fault == 'unsealed':
        actual['storage_sealed'] = False
    if fault == 'nonzero':
        actual['exit_status'] = 7
    if fault == 'foreign_run':
        actual['run_id'] = 'd' * 32
    runtime(sink, copy.deepcopy(actual) if fault == 'unobserved_report' else actual)
    monkeypatch.setattr(upload, '_d2_finish_transport', lambda current: True)
    assert upload._d2_apply_finalization(sink, actual) != 0
    assert sink['diagnostics_state'] == 'INCOMPLETE'
    assert sink['final_exit_status'] != 0 and sink['diagnostic_success_allowed'] is False
    assert upload._d2_completion_allowed(sink) is False
    assert path.read_bytes() == persisted and hashlib.sha256(path.read_bytes()).hexdigest() == digest
    stored = json.loads(path.read_bytes())
    assert stored['receipt_scope'] == 'PREFINAL_SUPERVISION'
    assert stored['finalization_state'] == 'NOT_OBSERVED'
    assert stored['diagnostic_success_allowed'] is False
    if fault == 'raises':
        assert sink['storage_sealed'] == 'UNKNOWN'


@pytest.mark.parametrize('fault', ['missing_worker', 'alive', 'exit23', 'no_final', 'no_normal_return', 'same_pid'])
def test_real_flog_success_cannot_upgrade_bad_supervision(upload, monkeypatch, fault):
    sink, actual = local_result(upload), report()
    row = sink['workers']['ftp']
    if fault == 'missing_worker':
        sink['workers'].pop('ftp')
    elif fault == 'alive':
        row.update(settled=False, alive=True)
    elif fault == 'exit23':
        row['exitcode'] = 23
    elif fault == 'no_final':
        row['diagnostics_state'] = 'INCOMPLETE'
    elif fault == 'no_normal_return':
        row['normal_return_observed'] = False
    else:
        row['pid'] = sink['workers']['google']['pid']
    runtime(sink, actual)
    monkeypatch.setattr(upload, '_d2_finish_transport', lambda current: True)
    assert upload._d2_apply_finalization(sink, actual) != 0
    assert sink['diagnostic_success_allowed'] is False and sink['committed_outputs'] == []


def test_cleanup_failure_does_not_rewrite_actual_flog_seal(upload, monkeypatch):
    sink, actual = local_result(upload), report()
    before = copy.deepcopy(actual)
    runtime(sink, actual)
    monkeypatch.setattr(upload, '_d2_finish_transport', lambda current: False)
    assert upload._d2_apply_finalization(sink, actual) != 0
    assert actual == before and actual['storage_sealed'] is True
    assert sink['diagnostics_state'] == 'INCOMPLETE'
    assert upload._d2_completion_allowed(sink) is False


def test_success_requires_observed_report_and_settled_transport(upload, monkeypatch):
    sink, actual = local_result(upload), report()
    runtime(sink, actual)
    monkeypatch.setattr(upload, '_d2_finish_transport', lambda current: True)
    assert upload._d2_apply_finalization(sink, actual) == 0
    assert upload._d2_completion_allowed(sink) is True
    assert sink['receipt_scope'] == 'FINAL_IN_MEMORY'
    assert actual['storage_sealed'] is True and sink['committed_outputs'] == []


def test_unsupported_upload_refuses_before_source_or_credentials(upload, monkeypatch, tmp_path, capsys):
    replacement = types.SimpleNamespace(**vars(sys))
    replacement.platform = 'win32'
    monkeypatch.setattr(upload, 'sys', replacement)
    reached = []
    def forbidden(*args, **kwargs):
        reached.append(True)
        raise AssertionError('D2_SOURCE_REACHED_BEFORE_PLATFORM_REFUSAL')
    monkeypatch.setattr(upload, '_d2_build_source_plan', forbidden)
    with pytest.raises(Exception):
        upload.main(str(tmp_path), 'D2_LITERAL_TARGET')
    assert reached == []
    assert 'Скрипт загрузки данных закончил работу.' not in capsys.readouterr().out


def test_pipeline_unsupported_gate_precedes_every_mutating_stage(pipeline, upload, monkeypatch, capsys):
    replacement = types.SimpleNamespace(**vars(sys))
    replacement.platform = 'win32'
    monkeypatch.setattr(upload, 'sys', replacement)
    reached = []
    def forbidden(*args, **kwargs):
        reached.append(True)
        raise AssertionError('D2_MUTATING_STAGE_REACHED')
    for name in ('data_gathering', 'dividends', 'tech', 'dohodru_data'):
        monkeypatch.setitem(sys.modules, name, types.SimpleNamespace(main=forbidden))
    with pytest.raises(Exception):
        pipeline.main()
    assert reached == []
    output = capsys.readouterr()
    assert 'полностью закончил работу' not in output.out


def test_forged_cli_runtime_context_cannot_bypass_platform_admission(pipeline, upload, monkeypatch):
    replacement = types.SimpleNamespace(**vars(sys))
    replacement.platform = 'win32'
    monkeypatch.setattr(upload, 'sys', replacement)
    reached = []
    fake = types.SimpleNamespace(bind_context=lambda context: context, active=True)
    context = {'run_id': 'a' * 32, 'producer_sha': 'c' * 40, '_d2_cli_runtime': fake}
    def forbidden(*args, **options):
        reached.append(True)
        raise AssertionError('forged runtime reached mutating stage')
    monkeypatch.setitem(sys.modules, 'run_logging', types.SimpleNamespace(initialize_main_stages=lambda context: None,
        check_logging_health=lambda context: {'healthy': True}, emit_event=lambda *args, **kwargs: {'confirmed': True},
        observe_stage=lambda *args, **kwargs: None))
    for name in ('data_gathering', 'dividends', 'tech', 'dohodru_data'):
        monkeypatch.setitem(sys.modules, name, types.SimpleNamespace(main=forbidden))
    with pytest.raises(Exception):
        pipeline.main(logging_context=context, result_sink={})
    assert reached == []


def test_main_modern_profile_and_keyword_only_interface(pipeline):
    import inspect
    assert pipeline.D2_SOURCE_PROFILE == 'linux-stage-b-v1'
    signature = inspect.signature(pipeline.main)
    assert list(signature.parameters) == ['logging_context', 'result_sink']
    assert all(row.kind is inspect.Parameter.KEYWORD_ONLY and row.default is None
               for row in signature.parameters.values())


def test_success_renderer_preserves_legacy_user_messages(upload, capsys):
    upload._d2_render_completion(3661.25)
    output = capsys.readouterr().out
    assert 'Скрипт загрузки данных закончил работу.' in output
    assert '1 часов, 1 минут, 1 секунд' in output
    upload._d2_render_completion(3661.25, pipeline=True)
    output = capsys.readouterr().out
    assert 'Скрипт закончил отрабатывать:' in output
    assert 'Скрипт полностью закончил работу.' in output


def orchestration_doubles(pipeline, monkeypatch, *, context=None, final_fault='none', stage_error=None):
    """Strict unit-only ABI; actual F-LOG/native evidence comes from controllers."""
    state = dict(active=False, bound=False, closed=False, runtime=None, context=context,
                 sink=None, stages=[], configurations=[], reports=[], final_calls=0, upload_calls=[])
    class Runtime:
        owns_context = False
        def bind_context(self, current):
            assert state['active'] and (state['context'] is None or state['context'] is current)
            state.update(context=current, bound=True)
            return current
    @contextmanager
    def scope(*, logging_context=None):
        assert not state['active'] and state['runtime'] is None
        current = Runtime()
        state.update(runtime=current, context=logging_context, active=True)
        try:
            yield current
        finally:
            state.update(active=False, closed=True)
    def stage(name, *args, **options):
        assert state['active'] and state['bound'] and not state['closed']
        state['stages'].append((name, args, options))
        if name == 'data_gathering' and stage_error is not None:
            raise stage_error
    def uploading(root, target, *, logging_context=None, result_sink=None, _runtime=None):
        assert _runtime is state['runtime'] and state['active'] and state['bound']
        assert logging_context is state['context'] and type(result_sink) is dict
        state['sink'] = result_sink
        result_sink['fixture_upload_returned'] = True
        state['upload_calls'].append((root, target, logging_context, result_sink, _runtime))
        stage('upload', root, target)
    def configure(root, **options):
        value = dict(run_id=options['run_id'], producer_sha=options['producer_sha'])
        state['configurations'].append(value)
        return value
    def finalize(current, **options):
        assert current is state['context'] and state['closed']
        state['final_calls'] += 1
        if final_fault == 'raises':
            raise RuntimeError('D2_PRIVATE_CANARY_4c1e11')
        value = dict(run_id=current['run_id'], execution_outcome=options['execution_outcome'],
                     exit_status=options['exit_status'], delivery_outcome='COMPLETE',
                     storage_sealed=True, evidence_incomplete=False)
        if final_fault == 'incomplete':
            value.update(exit_status=1, delivery_outcome='INCOMPLETE', storage_sealed=False, evidence_incomplete=True)
        state['reports'].append(value)
        return value
    def apply(sink, actual, *, status=0):
        assert state['closed']
        if state['sink'] is None:
            state['sink'] = sink
        assert sink is state['sink']
        if actual is not None:
            assert any(actual is value for value in state['reports']), 'fabricated F-LOG report'
        complete = (sink.get('fixture_upload_returned') is True and actual is not None
            and actual['delivery_outcome'] == 'COMPLETE' and actual['storage_sealed'] is True
            and actual['execution_outcome'] == 'COMPLETED' and actual['exit_status'] == 0 and status == 0)
        value = status or (actual['exit_status'] if actual is not None else 1) or (0 if complete else 1)
        sink.update(final_exit_status=value, diagnostics_state='COMPLETE' if complete else 'INCOMPLETE',
                    diagnostic_success_allowed=complete)
        return value
    fake_upload = types.SimpleNamespace(runtime_scope=scope, main=uploading,
        _d2_apply_finalization=apply,
        _d2_completion_allowed=lambda sink: sink.get('diagnostic_success_allowed') is True)
    fake_log = types.SimpleNamespace(
        initialize_main_stages=lambda current: None,
        check_logging_health=lambda current: {'healthy': True},
        emit_event=lambda *args, **kwargs: {'confirmed': True},
        observe_stage=lambda *args, **kwargs: None,
        make_error=lambda current, error, stage: {'error_type': type(error).__name__},
        mark_unreached=lambda *args: None,
        configure_logging=configure, finalize_logging=finalize)
    monkeypatch.setitem(sys.modules, 'upload', fake_upload)
    monkeypatch.setitem(sys.modules, 'run_logging', fake_log)
    for name in ('data_gathering', 'dividends', 'tech', 'dohodru_data'):
        def call(*args, name=name, **options):
            return stage(name, *args, **options)
        monkeypatch.setitem(sys.modules, name, types.SimpleNamespace(main=call))
    return state


@pytest.mark.parametrize('with_context', [False, True])
def test_run_stage_forwards_context_and_sink_exactly(pipeline, monkeypatch, capsys, with_context):
    context = {'run_id': 'a' * 32, 'producer_sha': 'c' * 40} if with_context else None
    state = orchestration_doubles(pipeline, monkeypatch, context=context)
    sink = {}
    assert pipeline.main(logging_context=context, result_sink=sink) is None
    assert [row[0] for row in state['stages']] == ['data_gathering', 'dividends', 'tech', 'dohodru_data', 'upload']
    assert len(state['upload_calls']) == 1
    root, target, received, received_sink, runtime_value = state['upload_calls'][0]
    assert received is context and received_sink is sink and runtime_value is state['runtime']
    assert state['closed'] is True
    assert state['stages'][1][2] == ({} if context is None else {'logging_context': context})
    assert all(options == {} for name, arguments, options in state['stages'] if name != 'dividends')
    assert 'Скрипт полностью закончил работу.' not in capsys.readouterr().out


@pytest.mark.parametrize('fault', ['none', 'raises', 'incomplete'])
def test_cli_one_parent_session_and_finalization_gate(pipeline, monkeypatch, capsys, fault):
    state = orchestration_doubles(pipeline, monkeypatch, final_fault=fault)
    monkeypatch.setattr(pipeline, '_source_sha', lambda root: 'c' * 40)
    monkeypatch.setattr(sys, 'argv', ['main.py', '--log-environment', 'offline'])
    code = pipeline._run_cli()
    assert len(state['configurations']) == state['final_calls'] == 1
    assert state['context'] is state['configurations'][0]
    assert state['sink'] is state['upload_calls'][0][3]
    assert state['upload_calls'][0][2] is state['context']
    output = capsys.readouterr()
    assert 'D2_PRIVATE_CANARY_4c1e11' not in output.out + output.err
    if fault == 'none':
        assert code == 0 and state['sink']['final_exit_status'] == 0
        assert 'Скрипт полностью закончил работу.' in output.out
    else:
        assert code != 0 and state['sink']['diagnostics_state'] == 'INCOMPLETE'
        assert 'Скрипт полностью закончил работу.' not in output.out


def test_original_stage_exception_survives_secondary_diagnostics(pipeline, monkeypatch, capsys):
    original = RuntimeError('D2_PRIVATE_CANARY_4c1e11')
    state = orchestration_doubles(pipeline, monkeypatch,
        context={'run_id': 'a' * 32, 'producer_sha': 'c' * 40}, stage_error=original)
    logger = sys.modules['run_logging']
    def diagnostics_failure(*args, **options):
        raise OSError('D2_PRIVATE_CANARY_4c1e11')
    logger.make_error = diagnostics_failure
    with pytest.raises(RuntimeError) as raised:
        pipeline.main(logging_context=state['context'], result_sink={})
    assert raised.value is original
    assert [row[0] for row in state['stages']] == ['data_gathering']
    output = capsys.readouterr()
    assert 'D2_PRIVATE_CANARY_4c1e11' not in output.out + output.err


@pytest.mark.parametrize('code,expected', [(None, 1), (0, 1), (7, 7), ('D2_PRIVATE_CANARY_4c1e11', 1)],
                         ids=['none', 'zero', 'seven', 'string'])
def test_cli_stage_systemexit_preserves_nonzero_semantics(pipeline, monkeypatch, capsys, code, expected):
    state = orchestration_doubles(pipeline, monkeypatch, stage_error=SystemExit(code))
    monkeypatch.setattr(pipeline, '_source_sha', lambda root: 'c' * 40)
    monkeypatch.setattr(sys, 'argv', ['main.py', '--log-environment', 'offline'])
    assert pipeline._run_cli() == expected
    assert [row[0] for row in state['stages']] == ['data_gathering']
    output = capsys.readouterr()
    assert 'D2_PRIVATE_CANARY_4c1e11' not in output.out + output.err
    assert 'Скрипт полностью закончил работу.' not in output.out
