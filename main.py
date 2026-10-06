import time, datetime
from pathlib import Path


def main(*, logging_context=None):
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

    run_stage('dividends', dividends.main)

    run_stage('tech', tech.main, current_path)

    url = 'https://www.dohod.ru/ik/analytics/dividend'
    run_stage('dohodru_data', dohodru_data.main, url)

    gdoc_to_write = "https://docs.google.com/spreadsheets/d/1HXXoxcDVqIrWN6QEg5ij88AxcNAKT-G-xm2UTUfQe1Q/edit?usp=sharing" ## Гугл док для сохранения данных
    run_stage('upload', upload.main, current_path, gdoc_to_write)

    print("Скрипт закончил отрабатывать: {}".format(datetime.datetime.now()))
    end_time = time.time()
    elapsed_time = end_time - start_time
    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)
    print('Скрипт полностью закончил работу.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))


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
    import run_logging

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
    try:
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
        root = Path(__file__).resolve().parent
        context = run_logging.configure_logging(root, run_id=uuid.uuid4().hex,
            environment=args.log_environment, producer_sha=_source_sha(root), config=config)
    except SystemExit as error:
        return error.code if type(error.code) is int else 1
    except BaseException as error:
        cause = 'LOG_OUTPUT_ROOT_UNAVAILABLE' if isinstance(error, OSError) else type(error).__name__
        if isinstance(error, ValueError) and error.args and type(error.args[0]) is str:
            if __import__('re').fullmatch('LOG_[A-Z_]+', error.args[0]):
                cause = error.args[0]
        sys.stderr.write('LOG_STARTUP_FAILED: ' + cause + '\n')
        return 130 if isinstance(error, KeyboardInterrupt) else 1
    outcome, status, application_error = 'COMPLETED', 0, None
    try:
        main(logging_context=context)
    except BaseException as error:
        outcome = 'INTERRUPTED' if isinstance(error, (KeyboardInterrupt, SystemExit)) else 'FAILED'
        if isinstance(error, KeyboardInterrupt):
            status = 130
        elif isinstance(error, SystemExit):
            status = 0 if error.code is None else error.code if type(error.code) is int else 1
        else:
            status = 1
        try:
            application_error = context.get('_application_error') or run_logging.make_error(context, error, 'startup')
        except BaseException:
            context['_failed'] = True
        run_logging.mark_unreached(context, 'INTERRUPTION' if outcome == 'INTERRUPTED' else 'PRIOR_STAGE_FAILED')
    try:
        report = run_logging.finalize_logging(context, execution_outcome=outcome,
            exit_status=status, application_error=application_error)
        return report['exit_status']
    except BaseException:
        sys.stderr.write('LOG_FINALIZATION_FAILED\n')
        return status if status else 1


if __name__ == "__main__":
    raise SystemExit(_run_cli())
