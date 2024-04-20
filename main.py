import os 
# os.system('python3 filename.py')

current_path = os.getcwd()

os.system(('jupyter nbconvert --to script {}/data_gathering.ipynb').format(current_path))
os.system(('jupyter nbconvert --to script {}/tech.ipynb').format(current_path))

os.system(('python {}/data_gathering.py')).format(current_path)
os.system(('python {}/tech.py')).format(current_path)