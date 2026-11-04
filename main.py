import os, sys, time, datetime, argparse, traceback

## Запуск всего пайплайна: сбор данных -> дивиденды -> тех индикаторы -> доход.ру -> отчёт -> загрузка
##   python main.py                  — обновление датасетов + загрузка на FTP и в гугл док
##   python main.py --full-reload    — выгрузить все датасеты с нуля
##   python main.py --no-upload      — без загрузки (для проверки)
## Код выхода: 0 — всё хорошо, 3 — ошибки в шагах, другой — скрипт упал целиком
## Какой конфиг датасетов брать — переменная окружения MOEX_CONFIG (см. run.sh)
## В конце: отчёт datasets/run_report.csv и сообщение в Telegram (notify.py, если настроен)
##
## Раньше здесь был jupyter nbconvert для экспорта ноутбуков в .py.
## Теперь .py синхронизируются с ноутбуками jupytext-хуком при коммите,
## поэтому на сервере запускаются уже готовые .py из git.

print("Запуск скрипта: {}".format(datetime.datetime.now()))
start_time = time.time()

current_path = sys.path[0]


### Запуск одного шага: ошибка в шаге не роняет весь скрипт, но запоминается.
### В steps сохраняется, успешен ли шаг и сколько он шёл
def run_step(name, failed, steps, func, *args):
    print('======================= {} ======================='.format(name))
    step_start = time.time()
    ok = True
    result = None
    try:
        result = func(*args)
    except Exception:
        traceback.print_exc()
        failed.append(name)
        ok = False

    # tech.main и upload.main возвращают список того, что не получилось
    if isinstance(result, list) and len(result) > 0:
        failed.append('{}: {}'.format(name, result))
        ok = False

    steps[name] = {'ok': ok, 'seconds': time.time() - step_start}
    print('Шаг {} занял {} мин.'.format(name, round(steps[name]['seconds'] / 60, 1)))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--full-reload', action='store_true', help='выгрузить все датасеты с нуля')
    parser.add_argument('--no-upload', action='store_true', help='не загружать на FTP и в гугл док')
    args = parser.parse_args()

    import data_gathering, dividends, tech, dohodru_data, upload, run_report, notify

    failed = []
    steps = {}

    run_step('data_gathering', failed, steps, data_gathering.main, current_path, args.full_reload)
    data_failed = len(failed) > 0

    run_step('dividends', failed, steps, dividends.main)

    run_step('tech', failed, steps, tech.main, current_path)
    tech_failed = not steps['tech']['ok']

    url = 'https://www.dohod.ru/ik/analytics/dividend'
    run_step('dohodru_data', failed, steps, dohodru_data.main, url)

    ## Отчёт до загрузки, чтобы run_report.csv тоже попал на FTP
    report_result = run_step('report', failed, steps, run_report.build_report, current_path, data_gathering.config)
    report, warnings = report_result if report_result is not None else (None, ['отчёт не построен'])

    ## Не выкладываем, если сбор данных или расчёт индикаторов упал: на FTP и в гугл доке
    ## остаются прошлые (целые) данные
    if args.no_upload:
        print('Загрузка выключена (--no-upload)')
    elif data_failed or tech_failed:
        print('Загрузку пропускаю: ошибка в сборе данных или в расчёте индикаторов')
        failed.append('upload: пропущена')
    else:
        run_step('upload', failed, steps, upload.main, current_path, upload.gdoc_to_write)

    print("Скрипт закончил отрабатывать: {}".format(datetime.datetime.now()))
    end_time = time.time()
    elapsed_time = end_time - start_time
    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)
    print('Скрипт полностью закончил работу.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))

    ## Сообщение в Telegram: об ошибке — всегда, об успехе — если notify_success
    status = 'ОШИБКИ: {}'.format(failed) if len(failed) > 0 else 'успешно'
    text = run_report.report_text(status, steps, report, warnings, elapsed_time, data_gathering.config_name)
    notify.send_telegram(text, current_path, success=len(failed) == 0 and len(warnings) == 0)

    if len(failed) > 0:
        print('Шаги с ошибками: {}'.format(failed))
        # код 3 = ошибки в шагах, о них уже написали в Telegram (run.sh отличает его от падения скрипта)
        sys.exit(3)
    print('Все шаги без ошибок')
