import os 
# os.system('python3 filename.py')

os.system('jupyter nbconvert --to script data_gathering.ipynb')
os.system('jupyter nbconvert --to script tech.ipynb')

os.system('python data_gathering.py')
os.system('python tech.py')