#!/usr/bin/env python
# coding: utf-8

# In[40]:


import talib, pandas as pd
from os import listdir
from os.path import isfile, join


# In[41]:


def RSI14(filename):
    path = "datasets/" + filename
    RSI_full = pd.DataFrame()

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

    if len(df) > 0: df.to_excel(path,index = False)


# In[43]:


#getting datasets
datasets_list = [f for f in listdir('datasets') if isfile(join('datasets', f))]


# In[45]:


for file in datasets_list:
    RSI14(file)

