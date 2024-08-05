import os,sys, time, datetime

print("Запуск скрипта: {}".format(datetime.datetime.now()))
start_time = time.time()

current_path = sys.path[0]

## Экспортирование актуальных версий скриптов
os.system(('jupyter nbconvert --to script {}/data_gathering.ipynb').format(current_path))
os.system(('jupyter nbconvert --to script {}/tech.ipynb').format(current_path))
os.system(('jupyter nbconvert --to script {}/dividends.ipynb').format(current_path))
os.system(('jupyter nbconvert --to script {}/dohodru_data.ipynb').format(current_path))

import data_gathering, dividends, tech, dohodru_data

data_gathering.main(current_path)

dividends.main()

tech.main()

dohodru_data.main()

print("Скрипт закончил отрабатывать: {}".format(datetime.datetime.now()))
end_time = time.time()
elapsed_time = end_time - start_time
print("Заняло времени: {} секунд".format(round(elapsed_time,0)))