import time, datetime
from pathlib import Path


D2_SOURCE_PROFILE = 'linux-stage-b-v1'
_D2_CLI_HANDOFFS = {}


def main(*, logging_context=None, result_sink=None):
    import upload
    from contextlib import nullcontext
    sink = {} if result_sink is None else result_sink
    if type(sink) is not dict:
        raise ValueError('D2_RESULT_SINK_INVALID')
    original_context = logging_context
    runtime = None
    primary = None
    cli_runtime = logging_context.pop('_d2_cli_runtime', None) if logging_context is not None else None
    if cli_runtime is not None:
        issued = globals().get('_D2_CLI_HANDOFFS', {}).pop(id(logging_context), None)
        if issued is None or issued[0] is not logging_context or issued[1] is not cli_runtime:
            raise RuntimeError('D2_RUNTIME_HANDOFF_INVALID')
    def _pipeline(logging_context, result_sink, runtime):
        if logging_context is not None:
            import run_logging
            run_logging.initialize_main_stages(logging_context)
        # Notebook-пары проверяются в dev/CI до запуска, production их не конвертирует.
        print("Запуск скрипта: {}".format(datetime.datetime.now()))
        start_time = time.time()
        current_path = str(Path(__file__).resolve().parent)

        def run_stage(stage, call, *args):
            if logging_context is None:
                return call(*args)
            if not run_logging.check_logging_health(logging_context)['healthy']:
                run_logging.mark_unreached(logging_context, 'DIAGNOSTICS_FAILURE')
                raise RuntimeError('LOGGING_INCOMPLETE')
            receipt = run_logging.emit_event(logging_context, 'INFO', 'stage_started',
                'Начало этапа ' + stage, {'stage': stage})
            if not receipt['confirmed'] or not run_logging.check_logging_health(logging_context)['healthy']:
                run_logging.mark_unreached(logging_context, 'DIAGNOSTICS_FAILURE')
                raise RuntimeError('LOGGING_INCOMPLETE')
            run_logging.observe_stage(logging_context, stage, 'STARTED')
            try:
                result = call(*args)
            except BaseException as error:
                interrupted = isinstance(error, (KeyboardInterrupt, SystemExit))
                state = 'INTERRUPTED' if interrupted else 'FAILED'
                try:
                    record = run_logging.make_error(logging_context, error, stage)
                    logging_context['_application_error'] = record
                    run_logging.observe_stage(logging_context, stage, state, error=record,
                        reason='INTERRUPTION' if interrupted else 'PRIOR_STAGE_FAILED')
                    run_logging.emit_event(logging_context, 'ERROR',
                        'stage_interrupted' if interrupted else 'stage_failed',
                        'Этап ' + stage + ': ' + state, {'stage': stage, 'error': record, 'outcome': state})
                except BaseException:
                    logging_context['_failed'] = True
                raise
            run_logging.observe_stage(logging_context, stage, 'RETURNED')
            receipt = run_logging.emit_event(logging_context, 'INFO', 'stage_returned',
                'Этап вернулся: ' + stage, {'stage': stage, 'outcome': 'RETURNED'})
            if not receipt['confirmed'] or not run_logging.check_logging_health(logging_context)['healthy']:
                run_logging.mark_unreached(logging_context, 'DIAGNOSTICS_FAILURE')
                raise RuntimeError('LOGGING_INCOMPLETE')
            return result

        import data_gathering, dividends, tech, dohodru_data, upload

        run_stage('data_gathering', data_gathering.main, current_path)

        def run_dividends():
            if logging_context is None:
                return dividends.main()
            return dividends.main(logging_context=logging_context)

        run_stage('dividends', run_dividends)

        run_stage('tech', tech.main, current_path)

        url = 'https://www.dohod.ru/ik/analytics/dividend'
        run_stage('dohodru_data', dohodru_data.main, url)

        gdoc_to_write = "https://docs.google.com/spreadsheets/d/1HXXoxcDVqIrWN6QEg5ij88AxcNAKT-G-xm2UTUfQe1Q/edit?usp=sharing" ## Гугл док для сохранения данных
        def run_upload():
            return upload.main(current_path, gdoc_to_write,
                logging_context=logging_context, result_sink=result_sink, _runtime=runtime)
        run_stage('upload', run_upload)
        result_sink['_pipeline_elapsed'] = time.time() - start_time

    try:
        with (nullcontext(cli_runtime) if cli_runtime is not None else
              upload.runtime_scope(logging_context=logging_context)) as runtime:
            sink['_runtime'] = runtime
            runtime.result = sink
            bound = runtime.bind_context(logging_context)
            if bound is not None:
                logging_context = bound
            _pipeline(logging_context, sink, runtime)
    except BaseException as error:
        primary = error
        if sink.get('parent_outcome') == 'UNKNOWN':
            sink.update(parent_outcome='INTERRUPTED' if isinstance(error, (KeyboardInterrupt, SystemExit)) else 'FAILED',
                        error_code='D2_PIPELINE_FAILED')
    if original_context is None and logging_context is not None:
        # Real bind_context(None) creates the parent-owned session. Test doubles
        # return None and never fabricate this ownership or a successful seal.
        if not sink:
            sink.update(upload._d2_result(logging_context['run_id'], __import__('uuid').uuid4().hex,
                logging_context['producer_sha'], {'google': [], 'ftp': []}))
            sink['_runtime'] = runtime
        status = upload._d2_finalize_owned(runtime, sink,
            outcome='INTERRUPTED' if isinstance(primary, (KeyboardInterrupt, SystemExit)) else 'FAILED' if primary else 'COMPLETED',
            status=130 if isinstance(primary, KeyboardInterrupt) else 1 if primary else 0,
            application_error=logging_context.get('_application_error'))
        if status and primary is None:
            primary = RuntimeError('D2_DIAGNOSTICS_INCOMPLETE')
        if upload._d2_completion_allowed(sink):
            _render_completion(sink)
    if primary is not None:
        raise primary
    return None




def _render_completion(result_sink):
    elapsed = result_sink.get('_pipeline_elapsed', 0.0)
    mins, secs = divmod(elapsed, 60)
    hours, mins = divmod(mins, 60)
    print("Скрипт закончил отрабатывать: {}".format(datetime.datetime.now()))
    print('Скрипт полностью закончил работу.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'.format(
        round(hours), round(mins), round(secs), round(elapsed, 3), 'сек'))


def _source_sha(root):
    # Только чтение Git metadata: запуск ETL не вызывает subprocess или сеть.
    git = root / '.git'
    if git.is_file():
        git = (root / git.read_text(encoding='utf-8').strip().split(': ', 1)[1]).resolve()
    head = (git / 'HEAD').read_text(encoding='utf-8').strip()
    if not head.startswith('ref: '):
        return head
    ref = head[5:]
    common = git
    if (git / 'commondir').exists():
        common = (git / (git / 'commondir').read_text(encoding='utf-8').strip()).resolve()
    if (common / ref).exists():
        return (common / ref).read_text(encoding='utf-8').strip()
    for line in (common / 'packed-refs').read_text(encoding='utf-8').splitlines():
        if line.endswith(' ' + ref):
            return line.split(' ', 1)[0]
    raise ValueError('LOG_SOURCE_SHA_UNAVAILABLE')


def _run_cli():
    import argparse
    import json
    import sys
    import uuid

    class SafeParser(argparse.ArgumentParser):
        def error(self, message):
            raise ValueError('LOG_CLI_INVALID')

    parser = SafeParser(description='MOEX pipeline with local diagnostic evidence')
    parser.add_argument('--log-config')
    parser.add_argument('--log-root')
    parser.add_argument('--log-environment', default='local')
    parser.add_argument('--log-console-level')
    parser.add_argument('--log-file-level')
    context = None
    runtime = None
    sink = {}
    outcome, status, application_error = 'COMPLETED', 0, None
    try:
        import run_logging
        import upload
        args = parser.parse_args()
        config = {}
        if args.log_config:
            path = Path(args.log_config)
            if path.stat().st_size > 65536:
                raise ValueError('LOG_CONFIG_TOO_LARGE')
            config = json.loads(path.read_text(encoding='utf-8'))
            if type(config) is not dict:
                raise ValueError('LOG_CONFIG_INVALID')
        for name in ('root', 'console_level', 'file_level'):
            value = getattr(args, 'log_' + name)
            if value is not None:
                config['log_root' if name == 'root' else name] = value
        config = run_logging.validate_logging_config(config)
        if config['shutdown_deadline_s'] > 10:
            raise ValueError('LOG_CONFIG_D2_BUDGET')
        root = Path(__file__).resolve().parent
        # The native owner exists before multiprocessing creates F-LOG queues.
        with upload.runtime_scope() as runtime:
            sink['_runtime'] = runtime
            runtime.result = sink
            context = run_logging.configure_logging(root, run_id=uuid.uuid4().hex,
                environment=args.log_environment, producer_sha=_source_sha(root), config=config)
            runtime.bind_context(context)
            runtime.owns_context = True
            context['_d2_cli_runtime'] = runtime
            handoffs = globals().setdefault('_D2_CLI_HANDOFFS', {})
            handoffs[id(context)] = (context, runtime)
            try:
                main(logging_context=context, result_sink=sink)
            except BaseException as error:
                outcome = 'INTERRUPTED' if isinstance(error, (KeyboardInterrupt, SystemExit)) else 'FAILED'
                if isinstance(error, KeyboardInterrupt):
                    status = 130
                elif isinstance(error, SystemExit):
                    status = error.code if type(error.code) is int and error.code != 0 else 1
                else:
                    status = 1
                try:
                    application_error = context.get('_application_error') or run_logging.make_error(context, error, 'startup')
                except BaseException:
                    context['_failed'] = True
                run_logging.mark_unreached(context, 'INTERRUPTION' if outcome == 'INTERRUPTED' else 'PRIOR_STAGE_FAILED')
            finally:
                handoffs.pop(id(context), None)
                context.pop('_d2_cli_runtime', None)
    except SystemExit as error:
        return error.code if type(error.code) is int else 1
    except BaseException as error:
        if context is None:
            cause = 'LOG_OUTPUT_ROOT_UNAVAILABLE' if isinstance(error, OSError) else type(error).__name__
            if isinstance(error, ValueError) and error.args and type(error.args[0]) is str:
                if __import__('re').fullmatch('LOG_[A-Z_]+', error.args[0]):
                    cause = error.args[0]
            sys.stderr.write('LOG_STARTUP_FAILED: ' + cause + '\n')
            return 130 if isinstance(error, KeyboardInterrupt) else 1
        if status == 0:
            status = 130 if isinstance(error, KeyboardInterrupt) else 1
            outcome = 'INTERRUPTED' if isinstance(error, (KeyboardInterrupt, SystemExit)) else 'FAILED'
        context['_failed'] = True
    report = None
    try:
        report = run_logging.finalize_logging(context, execution_outcome=outcome,
            exit_status=status, application_error=application_error)
        status = upload._d2_apply_finalization(sink, report, status=status)
        if upload._d2_completion_allowed(sink):
            _render_completion(sink)
        return status
    except BaseException:
        sys.stderr.write('LOG_FINALIZATION_FAILED\n')
        status = status if status else 1
        try:
            # Preserve a real seal if an operation after it failed. Never
            # replace its report with None or call finalization a second time.
            upload._d2_apply_finalization(sink, report, status=status)
        except BaseException:
            sink.update(receipt_scope='FINAL_IN_MEMORY', finalization_state='OBSERVED_INCOMPLETE',
                diagnostic_success_allowed=False, diagnostics_state='INCOMPLETE', final_exit_status=status)
        return status


if __name__ == "__main__":
    raise SystemExit(_run_cli())
