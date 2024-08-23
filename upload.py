#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pygsheets, pandas as pd, time, datetime, ftplib, sys, json
from os import listdir
from os.path import isfile, join

from multiprocessing import Process


current_path = sys.path[0]

## Префикс секретов
secrets_prefix = current_path.rsplit("/", 1)[0] + "/secrets/"

## Гугл док для сохранения данных
gdoc_to_write = "https://docs.google.com/spreadsheets/d/1HXXoxcDVqIrWN6QEg5ij88AxcNAKT-G-xm2UTUfQe1Q/edit?usp=sharing"


datasets_path = current_path + "/datasets/"


## Начальное время
start_time = time.time()


# In[ ]:


def gdoc_upload(current_path, gdoc_to_write):
    print('Начал загрузку данных на Google Диск и FTP.\nВремя старта: {}'.format(datetime.datetime.now()))

    ## Префикс секретов
    secrets_prefix = current_path.rsplit("/", 1)[0] + "/secrets/"

    service_account_json = secrets_prefix + '/python-304621-1e403209f05f.json'
    gc = pygsheets.authorize(service_file=service_account_json)

    ### Подготовка данных для гугл дока
    df = pd.read_excel(datasets_path + "10years_data_1d_interval.xlsx")
    # df = df.round(decimals = 2) #округление до 2 цифр
    # df.sort_values(by=0, ascending=True, inplace=True) #сортировка

    ### Убирание ненужных колонок, чтобы не упираться в лимит в 10кк ячеек гугл доков
    columns = df.columns
    white_list_columns = ['open', 'close', 'high', 'low', 'value', 'volume', 'begin', 'end',
        'ticker', 'RSI14']
    columns_to_remove = [i for i in columns if i not in white_list_columns]
    columns_to_remove
    df.drop(columns_to_remove, axis=1,inplace=True)


    ### Загрузка данных на гугл док

    try:
        google_doc = gc.open_by_url(gdoc_to_write)

        #opening needed sheet
        needed_sheet = google_doc[0]

        #clearing all data
        needed_sheet.clear()

        #writing data
        needed_sheet.set_dataframe(df, 'A1', copy_index=False, copy_head=True, fit=False, escape_formulae=False, nan='NaN')
    except Exception as e:
        print(f"Произошла ошибка: {e}")


    # конечное время
    end_time = time.time()

    # разница между конечным и начальным временем
    elapsed_time = end_time - start_time

    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)

    print('Обновление данных на Google Диске закончено.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))
    print('-----------------------')


# In[ ]:


def ftp_upload(current_path):

    secrets_prefix = current_path.rsplit("/", 1)[0] + "/secrets/"

    ## Получение секрета подключения к FTP
    ftp_secrets_path = open(secrets_prefix + "moex.foykes.com.json")
    ftp_data = json.load(ftp_secrets_path)
    ftp_secrets_path.close()

    ## Подключение к FTP
    ftp_server = ftplib.FTP(ftp_data['domain'], ftp_data['username'], ftp_data['password'])
    ftp_server.encoding = "utf-8"

    ## Загрузка основных датасетов
    datasets_list = [f for f in listdir(datasets_path) if isfile(join(datasets_path, f))]
    datasets_list = list(set(datasets_list)) #дедуп
    
    try:
        for i in range(0,len(datasets_list)):
            file_path = datasets_path + "/" + datasets_list[i]
            where_upload = "STOR {}".format(datasets_list[i])
            
            with open(file_path, "rb") as file:
                ftp_server.storbinary(where_upload, file)
    except Exception as e:
        print(f"Произошла ошибка: {e}")


    ## Загрузка дивидендов
    datasets_path_divs = datasets_path + "/dividends"
    datasets_list = [f for f in listdir(datasets_path_divs) if isfile(join(datasets_path_divs, f))]
    datasets_list = list(set(datasets_list)) #дедуп

    try:
        for i in range(0,len(datasets_list)):
            file_path = datasets_path_divs + "/" + datasets_list[i]
            where_upload = "STOR dividends/{}".format(datasets_list[i])
            
            with open(file_path, "rb") as file:
                ftp_server.storbinary(where_upload, file)
    except Exception as e:
        print(f"Произошла ошибка: {e}")

        
    ## Загрузка списка тикеров
    datasets_path_divs = datasets_path + "/ticker_lists"
    datasets_list = [f for f in listdir(datasets_path_divs) if isfile(join(datasets_path_divs, f))]
    datasets_list = list(set(datasets_list)) #дедуп

    try:
        for i in range(0,len(datasets_list)):
            file_path = datasets_path_divs + "/" + datasets_list[i]
            where_upload = "STOR ticker_lists/{}".format(datasets_list[i])
            
            with open(file_path, "rb") as file:
                ftp_server.storbinary(where_upload, file)
    except Exception as e:
        print(f"Произошла ошибка: {e}")

    ## Закрытие подключения к FTP   
    ftp_server.quit()


    # конечное время
    end_time = time.time()

    # разница между конечным и начальным временем
    elapsed_time = end_time - start_time

    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)

    print('Загрузка данных на FTP закончена.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))
    print('-----------------------')


# In[ ]:


def main(current_path,gdoc_to_write):
    # end_time = gdoc_upload(current_path, gdoc_to_write)
    # end_time_ftp = ftp_upload(current_path)

    p1 = Process(target=gdoc_upload, args=(current_path, gdoc_to_write))
    p1.start()
    p2 = Process(target=ftp_upload, args=(current_path,))
    p2.start()
    p1.join()
    p2.join()

    # конечное время
    end_time_ftp = time.time()

    # разница между конечным и начальным временем
    elapsed_time = end_time_ftp - start_time

    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)

    print('Скрипт загрузки данных закончил работу.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))
    print('-----------------------')


# In[ ]:


if __name__ == "__main__": 
    main(current_path,gdoc_to_write)

