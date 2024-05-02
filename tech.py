#!/usr/bin/env python
# coding: utf-8

# In[8]:


import talib, pandas as pd
import os
from os import listdir
from os.path import isfile, join

current_path = os.getcwd()


# In[9]:


def RSI14(filename):
    path = current_path + "/datasets/" + filename
    RSI_full = pd.DataFrame()

    if path.endswith('csv'):
        df = pd.read_csv(path)
    elif path.endswith('xlsx'):
        df = pd.read_excel(path)

    ticker_list = df['ticker'].to_list()
    ticker_list = list(set(ticker_list))

    for i in range(0,len(ticker_list)):
        ticker = ticker_list[i]
        df_ticker = df[(df['ticker'] == ticker)]
        RSI_ticker = talib.RSI(df_ticker["close"], timeperiod=14)
        if len(RSI_ticker) > 0: RSI_full = pd.concat([RSI_full,RSI_ticker])

    RSI_full = RSI_full.rename(columns= {0: 'RSI14'})
    df = df.join(RSI_full)

    if path.endswith('csv'):
        if len(df) > 0: df.to_csv(path,index = False)
    elif path.endswith('xlsx'):
        if len(df) > 0: df.to_excel(path,index = False)


# In[10]:


#getting datasets
datasets_list = [f for f in listdir('datasets') if isfile(join('datasets', f))]

# datasets_list = [x for x in datasets_list if not x.endswith('.csv')]

datasets_list = list(set(datasets_list))


# In[5]:


for file in datasets_list:
    RSI14(file)

