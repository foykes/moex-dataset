# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
import pygsheets, pandas as pd, time, datetime, ftplib, sys, json
from os import listdir
from os.path import isfile, join, isdir

from multiprocessing import Process


current_path = sys.path[0]

## Префикс секретов (папка secrets лежит рядом с папкой проекта)
secrets_prefix = current_path.rsplit("/", 1)[0] + "/secrets/"

## Гугл док для сохранения данных
gdoc_to_write = "https://docs.google.com/spreadsheets/d/1HXXoxcDVqIrWN6QEg5ij88AxcNAKT-G-xm2UTUfQe1Q/edit?usp=sharing"

## Какой датасет выкладываем в гугл док
gdoc_dataset = "10years_data_1d_interval.csv"

## Лимит ячеек в одной Google таблице и размер порции строк при записи
## (один огромный запрос Google API не принимает)
gdoc_max_cells = 10000000
gdoc_chunk_rows = 20000


## Начальное время
start_time = time.time()

# %%
def gdoc_upload(current_path, gdoc_to_write):
    print('Начал загрузку данных на Google Диск.\nВремя старта: {}'.format(datetime.datetime.now()))

    ## Префикс секретов
    secrets_prefix = current_path.rsplit("/", 1)[0] + "/secrets/"
    service_account_json = secrets_prefix + '/python-304621-1e403209f05f.json'
    datasets_path = current_path + "/datasets/"

    ### Подготовка данных для гугл дока
    dataset_file = datasets_path + gdoc_dataset
    if not isfile(dataset_file):
        raise RuntimeError('Нет файла для гугл дока: {}'.format(dataset_file))

    ### Читаем только нужные колонки, чтобы не упираться в лимит ячеек гугл доков
    ### (и не грузить в память ~80 колонок индикаторов). Порядок колонок — как в файле
    white_list_columns = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end',
        'ticker', 'RSI14']
    df = pd.read_csv(dataset_file, usecols=lambda column: column in white_list_columns)
    # df = df.round(decimals = 2) #округление до 2 цифр

    ### Проверка размера ДО очистки листа: если не влезает — старые данные в доке не трогаем
    rows_needed = len(df) + 1  # + заголовок
    cells_needed = rows_needed * len(df.columns)
    if cells_needed > gdoc_max_cells:
        raise RuntimeError('Данные не влезают в гугл док: {} ячеек при лимите {}. Старые данные оставлены'.format(cells_needed, gdoc_max_cells))


    ### Загрузка данных на гугл док
    gc = pygsheets.authorize(service_file=service_account_json)
    google_doc = gc.open_by_url(gdoc_to_write)
    #opening needed sheet
    needed_sheet = google_doc[0]
    #clearing all data
    needed_sheet.clear()
    # размер листа ровно под данные
    needed_sheet.resize(rows=rows_needed, cols=len(df.columns))

    #writing data порциями, каждую порцию до 3 попыток.
    #значения те же, что раньше писал set_dataframe(..., nan='NaN'); первая строка листа — заголовок
    needed_sheet.update_values('A1', [list(df.columns)])
    for start in range(0, len(df), gdoc_chunk_rows):
        df_chunk = df.iloc[start:start + gdoc_chunk_rows]
        chunk = df_chunk.astype(object).where(df_chunk.notna(), 'NaN').values.tolist()
        for attempt in range(1, 4):
            try:
                needed_sheet.update_values('A{}'.format(start + 2), chunk)
                break
            except Exception as e:
                print('Ошибка записи строк {}-{} в гугл док (попытка {}/3): {}'.format(start + 1, start + len(chunk), attempt, e))
                if attempt == 3:
                    raise
                time.sleep(10 * attempt)

    # конечное время
    end_time = time.time()
    # разница между конечным и начальным временем
    elapsed_time = end_time - start_time
    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)
    print('Обновление данных на Google Диске закончено ({} строк).\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(len(df), round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))
    print('-----------------------')

# %%
### Загрузка одного файла на FTP: сперва под временным именем, потом переименование,
### чтобы на сайте не было видно недогруженного файла
def ftp_store(ftp_server, file_path, remote_name):
    remote_tmp = remote_name + '.uploading'
    with open(file_path, "rb") as file:
        ftp_server.storbinary("STOR {}".format(remote_tmp), file)
    try:
        ftp_server.rename(remote_tmp, remote_name)
    except ftplib.error_perm:
        # некоторые FTP-серверы не переименовывают поверх существующего файла
        ftp_server.delete(remote_name)
        ftp_server.rename(remote_tmp, remote_name)


### Удалить файл на FTP, если он там есть (ошибку "нет файла" игнорируем)
def ftp_delete_if_exists(ftp_server, remote_name):
    try:
        ftp_server.delete(remote_name)
        print('Удалил с FTP устаревший файл {}'.format(remote_name))
    except Exception:
        pass


def ftp_upload(current_path):

    secrets_prefix = current_path.rsplit("/", 1)[0] + "/secrets/"
    datasets_path = current_path + "/datasets/"

    ## Получение секрета подключения к FTP
    ftp_secrets_path = open(secrets_prefix + "moex.foykes.com.json")
    ftp_data = json.load(ftp_secrets_path)
    ftp_secrets_path.close()

    ## Подключение к FTP
    ftp_server = ftplib.FTP(ftp_data['domain'], ftp_data['username'], ftp_data['password'], timeout=300)
    ftp_server.encoding = "utf-8"

    ## Что выкладываем: (локальная папка, папка на FTP)
    ## основные датасеты (+ index.html и run_report.csv), дивиденды, доход.ру, списки тикеров
    ## Корень FTP — это папка /datasets/ сайта moex.foykes.com
    folders = [
        (datasets_path, ""),
        (datasets_path + "dividends", "dividends/"),
        (datasets_path + "dividends/dohodru", "dividends/dohodru/"),
        (datasets_path + "ticker_lists", "ticker_lists/"),
    ]

    failed = []
    uploaded = 0
    for local_dir, remote_dir in folders:
        if not isdir(local_dir):
            print('Нет папки {}, пропускаю'.format(local_dir))
            continue

        # папки на FTP может не быть (например dividends/dohodru) — создаём
        if remote_dir != "":
            try:
                ftp_server.mkd(remote_dir.rstrip('/'))
            except Exception:
                pass

        datasets_list = [f for f in listdir(local_dir) if isfile(join(local_dir, f))]
        datasets_list = sorted(set(datasets_list)) #дедуп
        # временные файлы от незаконченной записи и служебные файлы не выкладываем
        datasets_list = [f for f in datasets_list if '.tmp.' not in f and '.partial.' not in f
                         and not f.startswith('.') and not f.startswith('~$')]

        for file_name in datasets_list:
            ## Большие датасеты выкладываем только сжатыми (.csv.gz): сам .csv не грузим,
            ## а старый .csv на FTP удаляем после успешной загрузки .csv.gz (см. ниже).
            ## Если датасет стал маленьким и .csv.gz больше нет — удаляем старый .csv.gz с FTP
            if file_name.endswith('.csv') and file_name + '.gz' in datasets_list:
                continue
            if file_name.endswith('.csv') and remote_dir == "":
                ftp_delete_if_exists(ftp_server, remote_dir + file_name + '.gz')

            # ошибка с одним файлом не останавливает загрузку остальных
            try:
                ftp_store(ftp_server, join(local_dir, file_name), remote_dir + file_name)
                uploaded = uploaded + 1
                if file_name.endswith('.csv.gz'):
                    ftp_delete_if_exists(ftp_server, remote_dir + file_name[:-len('.gz')])
            except Exception as e:
                print(f"Произошла ошибка при загрузке {remote_dir + file_name}: {e}")
                failed.append(remote_dir + file_name)

    ## Закрытие подключения к FTP
    ftp_server.quit()

    # конечное время
    end_time = time.time()
    # разница между конечным и начальным временем
    elapsed_time = end_time - start_time
    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)
    print('Загрузка данных на FTP закончена. Загружено файлов: {}, с ошибкой: {}.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(uploaded, len(failed), round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))
    print('-----------------------')

    if len(failed) > 0:
        raise RuntimeError('Не загрузились на FTP: {}'.format(failed))

# %%
### Загрузка в гугл док и на FTP параллельно.
### Возвращает список шагов, которые завершились ошибкой (пустой — всё хорошо)
def main(current_path,gdoc_to_write):
    current_path = str(current_path)

    p1 = Process(target=gdoc_upload, args=(current_path, gdoc_to_write))
    p1.start()

    p2 = Process(target=ftp_upload, args=(current_path,))
    p2.start()

    p1.join()
    p2.join()

    # если внутри процесса была ошибка, у процесса ненулевой exitcode
    failed = []
    if p1.exitcode != 0: failed.append('google')
    if p2.exitcode != 0: failed.append('ftp')
    if len(failed) > 0:
        print('Загрузка с ошибками: {}'.format(failed))

    # конечное время
    end_time_ftp = time.time()
    # разница между конечным и начальным временем
    elapsed_time = end_time_ftp - start_time
    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)
    print('Скрипт загрузки данных закончил работу.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))
    print('-----------------------')

    return failed

# %%
if __name__ == "__main__": 
    main(current_path,gdoc_to_write)


