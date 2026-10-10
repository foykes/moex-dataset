"""Source-bound regression; baseline leaks the canary in its traceback."""

from pathlib import Path
import subprocess
import sys
import importlib.util
import json
import types
import uuid

import pytest

from _probe import ROOT


def test_cli_redacts_application_exception():
    probe = Path(__file__).with_name('_probe.py').resolve()
    result = subprocess.run([sys.executable, '-I', '-B', '-X', 'utf8', str(probe), 'error'],
                            capture_output=True, text=True, encoding='utf-8', timeout=30)
    assert result.returncode == 1
    assert 'CLI_CANARY_823' not in result.stdout + result.stderr


def load_main(monkeypatch, stage_call):
    names = ('data_gathering', 'dividends', 'tech', 'dohodru_data', 'upload')
    for order, name in enumerate(names):
        module = types.ModuleType(name)
        module.main = lambda *args, order=order, **kwargs: stage_call(order, args)
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location('flog_main_' + uuid.uuid4().hex, ROOT / 'main.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
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
    def stage(order, args):
        raise SystemExit(code)
    module = load_main(monkeypatch, stage)
    monkeypatch.setattr(sys, 'argv', ['main.py', '--log-environment', 'offline'])
    assert module._run_cli() == wanted


def test_cli_keyboardinterrupt_130(log, monkeypatch):
    module = load_main(monkeypatch, lambda *a: (_ for _ in ()).throw(KeyboardInterrupt()))
    monkeypatch.setattr(sys, 'argv', ['main.py'])
    assert module._run_cli() == 130


def test_cli_bad_config_before_stages_safe(log, monkeypatch, capsys):
    calls = []
    module = load_main(monkeypatch, lambda *a: calls.append(a))
    monkeypatch.setattr(sys, 'argv', ['main.py', '--log-root', '../CLI_PATH_CANARY'])
    assert module._run_cli() == 1
    assert calls == []
    output = capsys.readouterr()
    assert 'CLI_PATH_CANARY' not in output.out + output.err
    assert 'LOG_CONFIG_ROOT' in output.err
