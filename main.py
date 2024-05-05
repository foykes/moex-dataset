import os,sys, time, datetime
# os.system('python3 filename.py')

print("Запуск скрипта: {}".format(datetime.datetime.now()))
start_time = time.time()

current_path = sys.path[0]


os.system(('jupyter nbconvert --to script {}/data_gathering.ipynb').format(current_path))
os.system(('jupyter nbconvert --to script {}/tech.ipynb').format(current_path))
os.system(('jupyter nbconvert --to script {}/dividends.ipynb').format(current_path))

os.system(('python {}/data_gathering.py').format(current_path))
os.system(('python {}/dividends.py').format(current_path))
os.system(('python {}/tech.py').format(current_path))

print("Скрипт закончил отрабатывать: {}".format(datetime.datetime.now()))
end_time = time.time()
elapsed_time = end_time - start_time
print("Заняло времени: {} секунд".format(round(elapsed_time,0)))