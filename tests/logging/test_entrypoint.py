"""Source-bound regression; baseline leaks the canary in its traceback."""

from pathlib import Path
import ast
from contextlib import contextmanager
import hashlib
import subprocess
import sys
import importlib.util
import json
import types
import uuid

import pytest

from _probe import ROOT


LEGACY_MAIN_SHA256 = 'c035f4439c060391e38c270f6f7adf257b356e6aae2104b135cf53e770509c3a'
MODERN_PROFILE = 'linux-stage-b-v1'


def source_profile(source):
    """Closed test-only admission. A signature alone never selects modern."""
    canonical = source.replace(b'\r\n', b'\n')
    tree = ast.parse(canonical.decode('utf-8'))
    if hashlib.sha256(canonical).hexdigest() == LEGACY_MAIN_SHA256:
        return 'legacy'
    markers = [node for node in tree.body if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign))
               and any(isinstance(item, ast.Name) and item.id == 'D2_SOURCE_PROFILE'
                       for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
                       for item in ast.walk(target))]
    assert len(markers) == 1 and isinstance(markers[0], ast.Assign), 'Unknown main source profile'
    assert len(markers[0].targets) == 1 and isinstance(markers[0].targets[0], ast.Name), 'Unknown main source profile'
    assert isinstance(markers[0].value, ast.Constant) and markers[0].value.value == MODERN_PROFILE, 'Unknown main source profile'
    for name, keywords in (('main', ['logging_context', 'result_sink']), ('_run_cli', [])):
        definitions = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name]
        assert len(definitions) == 1 and type(definitions[0]) is ast.FunctionDef, 'Unknown main source profile'
        function = definitions[0]
        args = function.args
        assert not (args.posonlyargs or args.args or args.vararg or args.kwarg or args.defaults or function.decorator_list), 'Unknown main source profile'
        assert [arg.arg for arg in args.kwonlyargs] == keywords, 'Unknown main source profile'
        assert all(isinstance(value, ast.Constant) and value.value is None for value in args.kw_defaults), 'Unknown main source profile'
    return 'modern'


def fixture_upload(stage_call, *, profile, log, observed_reports):
    """Unit orchestration seam; never supplies native or remote success evidence."""
    module = types.ModuleType('upload')
    state = dict(runtime=None, context=None, sink=None, active=False,
                 entered=False, bound=False, closed=False, returned=False,
                 report=None, final_status=None)
    module._fixture_state = state
    def stage_call_after_admission(order, args):
        if profile == 'modern':
            assert (state['active'] and state['bound'] and not state['closed']), 'Stage before fixture runtime admission'
        return stage_call(order, args)
    module._fixture_stage_call = stage_call_after_admission
    if profile == 'legacy':
        module.main = lambda *args: stage_call_after_admission(4, args)
        return module
    assert profile == 'modern'

    class Runtime:
        def bind_context(self, context):
            assert state['active'] and not state['closed']
            if not state['bound'] and state['context'] is None:
                state['context'] = context
            assert context is state['context'], 'Fixture context replaced'
            state['bound'] = True

    @contextmanager
    def runtime_scope(*, logging_context=None):
        assert not state['entered'], 'Fixture runtime reused'
        runtime = Runtime()
        state.update(runtime=runtime, context=logging_context, active=True, entered=True)
        try:
            yield runtime
        finally:
            state.update(active=False, closed=True)

    def upload(root, target, *, logging_context=None, result_sink=None, _runtime=None):
        assert state['active'] and state['bound'] and not state['returned']
        assert _runtime is state['runtime'] and logging_context is state['context']
        assert type(result_sink) is dict, 'Fixture requires parent-owned sink'
        state['sink'] = result_sink
        result = stage_call_after_admission(4, (root, target))
        state['returned'] = True
        return result

    def apply_finalization(sink, report, *, status=0):
        # Identity comes from an observed real finalize_logging call, not a
        # shape-compatible dictionary invented by this fixture or application.
        assert type(sink) is dict
        if state['sink'] is None and not state['returned']:
            state['sink'] = sink
        assert sink is state['sink']
        assert type(status) is int and status >= 0
        assert any(context is state['context'] and actual is report
                   for context, actual in observed_reports), 'Unobserved F-LOG report'
        assert report['run_id'] == state['context']['run_id']
        incomplete = (not state['returned'] or not state['closed']
                      or report['execution_outcome'] != 'COMPLETED'
                      or report['delivery_outcome'] != 'COMPLETE'
                      or not report['storage_sealed'] or report['evidence_incomplete'])
        state.update(report=report, final_status=status or report['exit_status'] or (1 if incomplete else 0))
        return state['final_status']

    def completion_allowed(sink):
        assert sink is state['sink'] and sink is not None
        report = state['report']
        return bool(state['returned'] and state['closed'] and state['final_status'] == 0
                    and report is not None and report['execution_outcome'] == 'COMPLETED'
                    and report['exit_status'] == 0 and report['delivery_outcome'] == 'COMPLETE'
                    and report['storage_sealed'] and not report['evidence_incomplete'])

    module.runtime_scope = runtime_scope
    module.main = upload
    module._d2_apply_finalization = apply_finalization
    module._d2_completion_allowed = completion_allowed
    return module


def test_cli_redacts_application_exception():
    probe = Path(__file__).with_name('_probe.py').resolve()
    result = subprocess.run([sys.executable, '-I', '-B', '-X', 'utf8', str(probe), 'error'],
                            capture_output=True, text=True, encoding='utf-8', timeout=30)
    assert result.returncode == 1
    assert 'CLI_CANARY_823' not in result.stdout + result.stderr
    # The application error must actually be reached, even on unsupported Windows.
    assert 'FIXTURE_DATA_GATHERING_RAISED' in result.stdout


def load_main(monkeypatch, stage_call):
    source = (ROOT / 'main.py').read_bytes()
    profile = source_profile(source)
    import run_logging as log
    observed = []
    finalizer = log.finalize_logging
    def finalized(context, **kwargs):
        report = finalizer(context, **kwargs)
        observed.append((context, report))
        return report
    monkeypatch.setattr(log, 'finalize_logging', finalized)
    upload_fixture = fixture_upload(stage_call, profile=profile, log=log, observed_reports=observed)
    names = ('data_gathering', 'dividends', 'tech', 'dohodru_data', 'upload')
    for order, name in enumerate(names):
        module = types.ModuleType(name)
        module.main = lambda *args, order=order, **kwargs: upload_fixture._fixture_stage_call(order, args)
        if name == 'upload':
            module = upload_fixture
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location('flog_main_' + uuid.uuid4().hex, ROOT / 'main.py')
    module = importlib.util.module_from_spec(spec)
    exec(compile(source, str(ROOT / 'main.py'), 'exec'), module.__dict__)
    module._fixture_profile = profile
    module._fixture_upload = sys.modules['upload']
    module._fixture_reports = observed
    return module


@pytest.mark.parametrize('mode', ['success', 'failure', 'interrupt', 'diagnostics', 'warning'])
def test_observed_stage_timing_independent_oracle(log, monkeypatch, mode):
    now = [0.0]
    monkeypatch.setattr(log, '_clock', lambda: now[0])
    prepare = log._prepare_root
    def prepared(*args):
        result = prepare(*args)
        now[0] += .010
        return result
    monkeypatch.setattr(log, '_prepare_root', prepared)
    close = log._close_transport
    def closed(*args):
        close(*args)
        now[0] += .015
    monkeypatch.setattr(log, '_close_transport', closed)
    context = log.configure_logging(ROOT, run_id='timing-' + uuid.uuid4().hex, environment='offline', producer_sha='c4f2210')
    calls = []
    primary = KeyboardInterrupt() if mode == 'interrupt' else ValueError('safe failure')
    def stage(order, args):
        calls.append(args)
        now[0] += [.020, .030, .040, .050, .060][order]
        if order == 1 and mode == 'warning':
            log.emit_event(context, 'WARNING', 'fixture_warning', 'Один warning', {})
        if order == 2 and mode in {'failure', 'interrupt'}:
            raise primary
    module = load_main(monkeypatch, stage)
    if mode == 'diagnostics':
        emitter = log.emit_event
        def emit(*args):
            if args[2] == 'stage_returned' and args[4]['stage'] == 'dividends':
                context['_failed'] = True
                return {'confirmed': False}
            return emitter(*args)
        monkeypatch.setattr(log, 'emit_event', emit)
    application_error = None
    outcome, status = 'COMPLETED', 0
    try:
        module.main(logging_context=context)
    except BaseException as error:
        if mode in {'failure', 'interrupt'}:
            assert error is primary
        else:
            assert str(error) == 'LOGGING_INCOMPLETE'
        outcome, status = ('INTERRUPTED', 130) if mode == 'interrupt' else ('FAILED', 1)
        application_error = context.get('_application_error')
    result = log.finalize_logging(context, execution_outcome=outcome, exit_status=status, application_error=application_error)
    rows = result['observed_stages']
    assert [r['stage'] for r in rows] == ['data_gathering', 'dividends', 'tech', 'dohodru_data', 'upload']
    assert [r['order'] for r in rows] == [1, 2, 3, 4, 5]
    assert all(set(row) == {'order', 'stage', 'execution_state', 'elapsed_ms', 'duration_ms', 'error_ref', 'state_reason'} for row in rows)
    if mode in {'success', 'warning'}:
        assert [r['execution_state'] for r in rows] == ['RETURNED'] * 5
        assert [r['elapsed_ms'] for r in rows] == pytest.approx([10, 30, 60, 100, 150])
        assert [r['duration_ms'] for r in rows] == pytest.approx([20, 30, 40, 50, 60])
        assert result['run_duration_ms'] == pytest.approx(225)
        assert result['errors'] == []
        assert result['logical_event_counts_by_level']['WARNING'] == (1 if mode == 'warning' else 0)
        assert calls[0] == (str(ROOT),) and calls[1] == () and calls[2] == (str(ROOT),)
        assert calls[3] == ('https://www.dohod.ru/ik/analytics/dividend',)
        assert len(calls[4]) == 2 and calls[4][0] == str(ROOT)
    elif mode == 'diagnostics':
        assert [r['execution_state'] for r in rows] == ['RETURNED', 'RETURNED', 'NOT_STARTED', 'NOT_STARTED', 'NOT_STARTED']
        assert result['run_duration_ms'] == pytest.approx(75)
        assert len(calls) == 2
        assert all(r['state_reason'] == 'DIAGNOSTICS_FAILURE' and r['elapsed_ms'] is None and r['duration_ms'] is None for r in rows[2:])
    else:
        assert [r['execution_state'] for r in rows] == ['RETURNED', 'RETURNED', 'INTERRUPTED' if mode == 'interrupt' else 'FAILED', 'NOT_STARTED', 'NOT_STARTED']
        assert [r['elapsed_ms'] for r in rows[:3]] == pytest.approx([10, 30, 60])
        assert [r['duration_ms'] for r in rows[:3]] == pytest.approx([20, 30, 40])
        assert result['run_duration_ms'] == pytest.approx(115)
        assert rows[2]['error_ref'] == '#/errors/0'
        assert result['errors'][0]['stage'] == 'tech'
        assert all(r['duration_ms'] is None and r['elapsed_ms'] is None for r in rows[3:])
    stored = json.loads((context['_run_root'] / 'run_summary.json').read_text(encoding='utf-8'))
    assert stored == result


@pytest.mark.parametrize('fault', ['make_error', 'observe_stage'])
@pytest.mark.parametrize('primary', [ValueError('safe'), KeyboardInterrupt(), SystemExit(0)])
def test_original_exception_survives_secondary_diagnostics(log, session, monkeypatch, fault, primary):
    def stage(order, args):
        raise primary
    module = load_main(monkeypatch, stage)
    original = getattr(log, fault)
    def fail(*args, **kwargs):
        if fault == 'observe_stage' and args[2] == 'STARTED':
            return original(*args, **kwargs)
        raise ValueError('secondary diagnostics failure')
    monkeypatch.setattr(log, fault, fail)
    with pytest.raises(BaseException) as caught:
        module.main(logging_context=session)
    assert caught.value is primary
    report = log.finalize_logging(session, execution_outcome='INTERRUPTED' if isinstance(primary, (KeyboardInterrupt, SystemExit)) else 'FAILED', exit_status=1)
    assert report['exit_status'] != 0


@pytest.mark.parametrize('code,wanted', [(None, 0), (0, 0), (7, 7), ('password=EXIT_CANARY', 1)])
def test_cli_systemexit_semantics(log, monkeypatch, code, wanted):
    reached = []
    def stage(order, args):
        reached.append(order)
        raise SystemExit(code)
    module = load_main(monkeypatch, stage)
    monkeypatch.setattr(sys, 'argv', ['main.py', '--log-environment', 'offline'])
    # SystemExit(0/None) from a mandatory stage is an incomplete modern run.
    # Keep the legacy expectation and original parameter IDs verbatim.
    expected = 1 if module._fixture_profile == 'modern' and wanted == 0 else wanted
    assert module._run_cli() == expected
    assert reached == [0]
    reports = module._fixture_reports
    assert len(reports) == 1
    assert reports[0][1]['execution_outcome'] == 'INTERRUPTED'
    if module._fixture_profile == 'modern':
        assert reports[0][1]['exit_status'] != 0


def test_cli_keyboardinterrupt_130(log, monkeypatch):
    reached = []
    def interrupted(order, args):
        reached.append(order)
        raise KeyboardInterrupt()
    module = load_main(monkeypatch, interrupted)
    monkeypatch.setattr(sys, 'argv', ['main.py'])
    assert module._run_cli() == 130
    assert reached == [0]


def test_cli_bad_config_before_stages_safe(log, monkeypatch, capsys):
    calls = []
    module = load_main(monkeypatch, lambda *a: calls.append(a))
    monkeypatch.setattr(sys, 'argv', ['main.py', '--log-root', '../CLI_PATH_CANARY'])
    assert module._run_cli() == 1
    assert calls == []
    output = capsys.readouterr()
    assert 'CLI_PATH_CANARY' not in output.out + output.err
    assert 'LOG_CONFIG_ROOT' in output.err


def test_closed_source_profile_selector():
    source = (ROOT / 'main.py').read_bytes()
    profile = source_profile(source)
    assert profile in {'legacy', 'modern'}
    if profile == 'legacy':
        assert source_profile(source.replace(b'\r\n', b'\n')) == 'legacy'
        with pytest.raises(AssertionError, match='Unknown main source profile'):
            source_profile(source + b'\n# unreviewed source drift\n')
    # Synthetic syntax exercises selection only, never claims application PASS.
    modern = b"D2_SOURCE_PROFILE = 'linux-stage-b-v1'\ndef main(*, logging_context=None, result_sink=None): pass\ndef _run_cli(): pass\n"
    assert source_profile(modern) == 'modern'
    for unknown in (
            modern.replace(b'linux-stage-b-v1', b'unknown'),
            modern.replace(b'D2_SOURCE_PROFILE', b'UNDECLARED'),
            modern.replace(b'*, logging_context', b'logging_context'),
            modern.replace(b'result_sink=None', b'result_sink=False'),
            modern.replace(b'result_sink=None', b'result_sink=None, **kwargs'),
            modern.replace(b'_run_cli()', b'_run_cli(extra=None)'),
            modern + b'D2_SOURCE_PROFILE = "linux-stage-b-v1"\n',
            modern + b'D2_SOURCE_PROFILE: str = "linux-stage-b-v1"\n',
            modern + b'D2_SOURCE_PROFILE += "x"\n'):
        with pytest.raises(AssertionError, match='Unknown main source profile'):
            source_profile(unknown)


def test_load_main_rejects_unknown_before_execution(monkeypatch, tmp_path):
    (tmp_path / 'main.py').write_text(
        "raise RuntimeError('UNREVIEWED_SOURCE_EXECUTED')\n", encoding='utf-8')
    monkeypatch.setitem(load_main.__globals__, 'ROOT', tmp_path)
    with pytest.raises(AssertionError, match='Unknown main source profile'):
        load_main(monkeypatch, lambda *args: pytest.fail('Unknown source reached a stage'))


def test_all_owned_source_selectors_agree():
    # Compile only owned fixture predicates, never import another suite or main.
    selectors = [source_profile]
    for relative in ('tests/f2/test_entrypoint_contract.py', 'tests/shared/c1/test_main_logging.py',
                     'tests/shared/c2/test_dohod_compatibility.py', 'tests/shared/c2/test_source_result.py'):
        tree = ast.parse((ROOT / relative).read_text(encoding='utf-8'))
        helper = next((node for node in tree.body if isinstance(node, ast.FunctionDef)
                       and node.name == '_d2_main_profile'), None)
        if helper is None:
            candidates = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                          and node.name in {'test_parent_stage_propagates_dohod_failure_upload_and_success_unreachable',
                                           'test_pipeline_stage_failure_never_reaches_later_stages'}]
            assert len(candidates) == 1
            body = candidates[0].body
            starts = [i for i, node in enumerate(body) if isinstance(node, ast.Assign)
                      and any(isinstance(target, ast.Name) and target.id == 'source_text' for target in node.targets)]
            stops = [i for i, node in enumerate(body) if isinstance(node, ast.Assert)
                     and isinstance(node.msg, ast.Constant) and node.msg.value == 'D2_FIXTURE_SOURCE_PROFILE_UNSUPPORTED']
            assert len(starts) == len(stops) == 1 and starts[0] < stops[0]
            helper = ast.parse("def _d2_main_profile(candidate):\n    source_text = candidate.replace(b'\\r\\n', b'\\n')\n").body[0]
            helper.body.extend(body[starts[0] + 1:stops[0] + 1])
            helper.body.append(ast.parse("return 'legacy' if legacy else 'modern'").body[0])
        namespace = {'ast': ast, 'hashlib': hashlib}
        selected = ast.fix_missing_locations(ast.Module(body=[helper], type_ignores=[]))
        exec(compile(selected, relative, 'exec'), namespace)
        selectors.append(namespace['_d2_main_profile'])
    legacy = (ROOT / 'main.py').read_bytes()
    modern = b"D2_SOURCE_PROFILE = 'linux-stage-b-v1'\ndef main(*, logging_context=None, result_sink=None): pass\ndef _run_cli(): pass\n"
    rejected = [
        modern.replace(b'linux-stage-b-v1', b'unknown'),
        modern.replace(b'D2_SOURCE_PROFILE', b'MISSING'),
        modern.replace(b"'linux-stage-b-v1'", b"str('linux-stage-b-v1')"),
        modern + b"D2_SOURCE_PROFILE = 'other'\n",
        modern + b"D2_SOURCE_PROFILE: str = 'other'\n",
        modern + b"D2_SOURCE_PROFILE += 'other'\n",
        modern + b"D2_SOURCE_PROFILE, other = 'other', 1\n",
        modern + b"[D2_SOURCE_PROFILE, other] = ['other', 1]\n",
        modern + b"async def main(): pass\n",
        modern + b"async def _run_cli(): pass\n",
        modern.replace(b'def main', b'async def main'),
        modern.replace(b'def main', b'@decorator\ndef main'),
        modern.replace(b'*, logging_context', b'logging_context'),
        modern.replace(b'result_sink=None', b'result_sink=None, **kwargs'),
        modern.replace(b'result_sink=None', b'result_sink=None, extra=None'),
        modern.replace(b'logging_context=None, result_sink=None', b'result_sink=None, logging_context=None'),
        modern.replace(b'result_sink=None', b'result_sink=False'),
        modern.replace(b'def _run_cli(): pass', b''),
    ]
    if source_profile(legacy) == 'legacy':
        rejected.append(legacy + b'\n# unknown drift\n')
    for select in selectors:
        assert select(legacy) == source_profile(legacy)
        assert select(modern) == select(modern.replace(b'\n', b'\r\n')) == 'modern'
        for unknown in rejected:
            with pytest.raises(AssertionError):
                select(unknown)


@pytest.mark.parametrize('fault', ['context', 'runtime', 'sink', 'inactive', 'replaced_context', 'extra_keyword'])
def test_modern_unit_upload_rejects_wrong_handoff(fault):
    context, sink, reports, calls = {}, {}, [], []
    upload = fixture_upload(lambda *args: calls.append(args), profile='modern',
                            log=None, observed_reports=reports)
    with upload.runtime_scope(logging_context=context) as runtime:
        runtime.bind_context(context)
        kwargs = dict(logging_context=context, result_sink=sink, _runtime=runtime)
        if fault == 'context':
            kwargs['logging_context'] = {}
        elif fault == 'runtime':
            kwargs['_runtime'] = object()
        elif fault == 'sink':
            kwargs['result_sink'] = None
        elif fault == 'inactive':
            upload._fixture_state['active'] = False
        elif fault == 'replaced_context':
            with pytest.raises(AssertionError):
                runtime.bind_context({})
            assert calls == []
            return
        else:
            kwargs['bypass'] = True
        with pytest.raises((AssertionError, TypeError)):
            upload.main('unit-root', 'unit-target', **kwargs)
    assert calls == []


@pytest.mark.parametrize('fault', ['none', 'copied', 'foreign', 'unclosed', 'not_returned', 'nonzero', 'interrupted_zero'])
def test_modern_unit_completion_requires_real_finalization(log, session, fault):
    # Parent-only real transport: unit evidence, zero registered native workers.
    sink, reports, calls = {}, [], []
    def returned(*args):
        calls.append(args)
        if fault == 'not_returned':
            raise ValueError('unit primary')
    upload = fixture_upload(returned, profile='modern',
                            log=log, observed_reports=reports)
    with upload.runtime_scope(logging_context=session) as runtime:
        runtime.bind_context(session)
        if fault == 'not_returned':
            with pytest.raises(ValueError, match='^unit primary$'):
                upload.main('unit-root', 'unit-target', logging_context=session,
                            result_sink=sink, _runtime=runtime)
        else:
            upload.main('unit-root', 'unit-target', logging_context=session,
                        result_sink=sink, _runtime=runtime)
        assert not upload._d2_completion_allowed(sink)
        if fault == 'unclosed':
            report = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
            reports.append((session, report))
            assert upload._d2_apply_finalization(sink, report) == 1
    if fault != 'unclosed':
        report = log.finalize_logging(session,
            execution_outcome=('FAILED' if fault == 'not_returned' else
                               'INTERRUPTED' if fault == 'interrupted_zero' else 'COMPLETED'),
            exit_status=1 if fault == 'not_returned' else 0)
        reports.append((session, report))
        if fault in {'copied', 'foreign'}:
            if fault == 'copied':
                proposed = dict(report)
            else:
                foreign = log.configure_logging(ROOT, run_id='foreign-' + uuid.uuid4().hex,
                    environment='offline', producer_sha='c4f2210')
                proposed = log.finalize_logging(foreign, execution_outcome='COMPLETED', exit_status=0)
                reports.append((foreign, proposed))
            with pytest.raises(AssertionError, match='Unobserved F-LOG report'):
                upload._d2_apply_finalization(sink, proposed)
            assert not upload._d2_completion_allowed(sink)
            return
        assert upload._d2_apply_finalization(sink, report, status=1 if fault == 'nonzero' else 0) == (0 if fault == 'none' else 1)
    assert upload._d2_completion_allowed(sink) is (fault == 'none')
    assert [row['worker_id'] for row in report['expected_producers']] == ['parent']
    assert report['worker_outcomes'] == {} and report['release_result'] is None
    assert len(calls) == 1


def test_modern_unit_rejects_real_incomplete_report(log, session, monkeypatch):
    sink, reports = {}, []
    upload = fixture_upload(lambda *args: None, profile='modern', log=log, observed_reports=reports)
    with upload.runtime_scope(logging_context=session) as runtime:
        runtime.bind_context(session)
        upload.main('unit-root', 'unit-target', logging_context=session, result_sink=sink, _runtime=runtime)
    # Inject before finalization; do not rewrite a successfully sealed report.
    reached = []
    def inventory_failed(*args):
        reached.append('before_seal')
        raise OSError('fixture')
    monkeypatch.setattr(log, '_inventory', inventory_failed)
    report = log.finalize_logging(session, execution_outcome='COMPLETED', exit_status=0)
    reports.append((session, report))
    assert reached == ['before_seal'] and not report['storage_sealed']
    assert report['delivery_outcome'] == 'INCOMPLETE' and report['exit_status'] != 0
    upload._d2_apply_finalization(sink, report)
    assert not upload._d2_completion_allowed(sink)


@pytest.mark.parametrize('phase', ['before_runtime', 'unbound', 'closed', 'admitted'])
def test_modern_first_stage_requires_prior_admission(log, monkeypatch, tmp_path, phase):
    # Disposable modern syntax tests the fixture, not an implemented D2 pipeline.
    bodies = {
        'before_runtime': '    data_gathering.main("unit-root")\n',
        'unbound': ('    with upload.runtime_scope(logging_context=logging_context) as runtime:\n'
                    '        data_gathering.main("unit-root")\n'),
        'closed': ('    with upload.runtime_scope(logging_context=logging_context) as runtime:\n'
                   '        runtime.bind_context(logging_context)\n'
                   '    data_gathering.main("unit-root")\n'),
        'admitted': ('    with upload.runtime_scope(logging_context=logging_context) as runtime:\n'
                     '        runtime.bind_context(logging_context)\n'
                     '        data_gathering.main("unit-root")\n'),
    }
    source = ("D2_SOURCE_PROFILE = 'linux-stage-b-v1'\n"
              'def main(*, logging_context=None, result_sink=None):\n'
              '    import data_gathering, upload\n' + bodies[phase] +
              'def _run_cli(): pass\n')
    (tmp_path / 'main.py').write_text(source, encoding='utf-8')
    monkeypatch.setitem(load_main.__globals__, 'ROOT', tmp_path)
    effects = []
    module = load_main(monkeypatch, lambda order, args: effects.append((order, args)))
    if phase == 'admitted':
        module.main(logging_context={}, result_sink={})
        assert effects == [(0, ('unit-root',))]
    else:
        with pytest.raises(AssertionError, match='Stage before fixture runtime admission'):
            module.main(logging_context={}, result_sink={})
        assert effects == []
