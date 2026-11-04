import os, sys, time, datetime, argparse, traceback

## Запуск всего пайплайна: сбор данных -> дивиденды -> тех индикаторы -> доход.ру -> загрузка
##   python main.py                  — обновление датасетов + загрузка на FTP и в гугл док
##   python main.py --full-reload    — выгрузить все датасеты с нуля
##   python main.py --no-upload      — без загрузки (для проверки)
## Какой конфиг датасетов брать — переменная окружения MOEX_CONFIG (см. run.sh)
##
## Раньше здесь был jupyter nbconvert для экспорта ноутбуков в .py.
## Теперь .py синхронизируются с ноутбуками jupytext-хуком при коммите,
## поэтому на сервере запускаются уже готовые .py из git.

print("Запуск скрипта: {}".format(datetime.datetime.now()))
start_time = time.time()

current_path = sys.path[0]


### Запуск одного шага: ошибка в шаге не роняет весь скрипт, но запоминается
def run_step(name, failed, func, *args):
    print('======================= {} ======================='.format(name))
    try:
        result = func(*args)
    except Exception:
        traceback.print_exc()
        failed.append(name)
        return None

    # tech.main и upload.main возвращают список того, что не получилось
    if isinstance(result, list) and len(result) > 0:
        failed.append('{}: {}'.format(name, result))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--full-reload', action='store_true', help='выгрузить все датасеты с нуля')
    parser.add_argument('--no-upload', action='store_true', help='не загружать на FTP и в гугл док')
    args = parser.parse_args()

    import data_gathering, dividends, tech, dohodru_data, upload

    failed = []

    run_step('data_gathering', failed, data_gathering.main, current_path, args.full_reload)
    data_failed = len(failed) > 0

    run_step('dividends', failed, dividends.main)

    run_step('tech', failed, tech.main, current_path)
    tech_failed = any(f.startswith('tech') for f in failed)

    url = 'https://www.dohod.ru/ik/analytics/dividend'
    run_step('dohodru_data', failed, dohodru_data.main, url)

    ## Не выкладываем, если сбор данных или расчёт индикаторов упал: на FTP и в гугл доке
    ## остаются прошлые (целые) данные
    if args.no_upload:
        print('Загрузка выключена (--no-upload)')
    elif data_failed or tech_failed:
        print('Загрузку пропускаю: ошибка в сборе данных или в расчёте индикаторов')
        failed.append('upload: пропущена')
    else:
        run_step('upload', failed, upload.main, current_path, upload.gdoc_to_write)

    print("Скрипт закончил отрабатывать: {}".format(datetime.datetime.now()))
    end_time = time.time()
    elapsed_time = end_time - start_time
    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)
    print('Скрипт полностью закончил работу.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))

    if len(failed) > 0:
        print('Шаги с ошибками: {}'.format(failed))
        sys.exit(1)
    print('Все шаги без ошибок')
