#!/usr/bin/env python
# coding: utf-8

# In[2]:


import talib, pandas as pd
import sys
from os import listdir
from os.path import isfile, join

current_path = sys.path[0]


# In[3]:


# print(talib.get_functions())
# print(talib.get_function_groups())


# In[4]:


def calculator(filename):
    path = "./datasets/" + filename
    RSI_full = pd.DataFrame()
    ADX_full = pd.DataFrame()
    STOCH_full_slowk = pd.DataFrame()
    STOCH_full_slowd = pd.DataFrame()
    STOCHRSI_full_fastk = pd.DataFrame()
    STOCHRSI_full_fastd = pd.DataFrame()


    MACD_real_full = pd.DataFrame()
    MACD_signal_full = pd.DataFrame()
    MACD_hist_full = pd.DataFrame()

    Williams_full = pd.DataFrame()
    CCI_full = pd.DataFrame()
    ATR_full = pd.DataFrame()
    HighsLows_full = pd.DataFrame()
    UltimateOscillator_full = pd.DataFrame()
    ROC_full = pd.DataFrame()
    BullBearPower_full = pd.DataFrame()
    

    if path.endswith('csv') and "~$" not in path:
        df = pd.read_csv(path)
    elif path.endswith('xlsx') and "~$" not in path:
        df = pd.read_excel(path)

    # Проверка наличия колонки RSI в df. Убрирает в случае наличия. Необходимо для пересчёта RSI без перевыгрузки данных
    if 'RSI14' in df.columns: df.drop('RSI14', axis=1, inplace=True)
    if 'ADX' in df.columns: df.drop('ADX', axis=1, inplace=True)
    if 'STOCH_slowk' in df.columns: df.drop('STOCH_slowk', axis=1, inplace=True)
    if 'STOCH_slowd' in df.columns: df.drop('STOCH_slowd', axis=1, inplace=True)
    if 'STOCHRSI' in df.columns: df.drop('STOCHRSI_fastk', axis=1, inplace=True)
    if 'STOCHRSI' in df.columns: df.drop('STOCHRSI_fastd', axis=1, inplace=True)

    if 'MACD_real' in df.columns: df.drop('MACD_real', axis=1, inplace=True)
    if 'MACD_signal' in df.columns: df.drop('MACD_signal', axis=1, inplace=True)
    if 'MACD_hist' in df.columns: df.drop('MACD_hist', axis=1, inplace=True)
    
    if 'Williams' in df.columns: df.drop('Williams', axis=1, inplace=True)
    if 'CCI' in df.columns: df.drop('CCI', axis=1, inplace=True)
    if 'ATR' in df.columns: df.drop('ATR', axis=1, inplace=True)
    if 'HighsLows' in df.columns: df.drop('HighsLows', axis=1, inplace=True)
    if 'UltimateOscillator' in df.columns: df.drop('UltimateOscillator', axis=1, inplace=True)
    if 'ROC' in df.columns: df.drop('ROC', axis=1, inplace=True)
    if 'BullBearPower' in df.columns: df.drop('BullBearPower', axis=1, inplace=True)

    ticker_list = df['ticker'].to_list()
    ticker_list = list(set(ticker_list))

    for i in range(0,len(ticker_list)):
        ticker = ticker_list[i]
        df_ticker = df[(df['ticker'] == ticker)]

        #RSI(14)
        RSI_ticker = talib.RSI(df_ticker["close"], timeperiod=14)
        if len(RSI_ticker) > 0: RSI_full = pd.concat([RSI_full,RSI_ticker])

        #ADX - Average Directional Movement Index
        ADX_real_ticker = talib.ADX(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(ADX_real_ticker) > 0: ADX_full = pd.concat([ADX_full,ADX_real_ticker])

        STOCH_real_ticker_slowk, STOCH_real_ticker_slowd = talib.STOCH(df_ticker['high'], df_ticker['low'], df_ticker['close'], fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)

        if len(STOCH_real_ticker_slowk) > 0 and len(STOCH_real_ticker_slowd) > 0:
            STOCH_full_slowk = pd.concat([STOCH_full_slowk,STOCH_real_ticker_slowk])
            STOCH_full_slowd = pd.concat([STOCH_full_slowd,STOCH_real_ticker_slowk])
        
        STOCHRSI_ticker_fastk, STOCHRSI_ticker_fastd = talib.STOCHRSI(df_ticker['close'], timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
        if len(STOCHRSI_ticker_fastk) > 0 and len(STOCHRSI_ticker_fastd) > 0:
            STOCHRSI_full_fastk = pd.concat([STOCHRSI_full_fastk,STOCHRSI_ticker_fastk])
            STOCHRSI_full_fastd = pd.concat([STOCHRSI_full_fastd,STOCHRSI_ticker_fastd])

        MACD_real_ticker, MACD_signal_ticker, MACD_hist_ticker = talib.MACD(df_ticker['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        if len(MACD_real_ticker) > 0 and len(MACD_signal_ticker) > 0 and len(MACD_hist_ticker) > 0:
            MACD_real_full = pd.concat([MACD_real_full,MACD_real_ticker])
            MACD_signal_full = pd.concat([MACD_signal_full,MACD_signal_ticker])
            MACD_hist_full = pd.concat([MACD_hist_full,MACD_hist_ticker])

        Williams_real_ticker = talib.WILLR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(Williams_real_ticker) > 0: Williams_full = pd.concat([Williams_full,Williams_real_ticker])
        
        CCI_real_ticker = talib.CCI(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(CCI_real_ticker) > 0: CCI_full = pd.concat([CCI_full,CCI_real_ticker])
        
        ATR_real_ticker = talib.ATR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
        if len(ATR_real_ticker) > 0: ATR_full = pd.concat([ATR_full,ATR_real_ticker])
        
        # HighsLows_real_ticker = talib.
        # if len(HighsLows_real_ticker) > 0: HighsLows_full = pd.concat([HighsLows_full,HighsLows_real_ticker])
        
        UltimateOscillator_real_ticker = talib.ULTOSC(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod1=7, timeperiod2=14, timeperiod3=28)
        if len(UltimateOscillator_real_ticker) > 0: UltimateOscillator_full = pd.concat([UltimateOscillator_full,UltimateOscillator_real_ticker])
                
        ROC_real_ticker = talib.ROC(df_ticker['close'], timeperiod=10)
        if len(ROC_real_ticker) > 0: ROC_full = pd.concat([ROC_full,ROC_real_ticker])
                
        # BullBearPower_real_ticker = talib.
        # if len(BullBearPower_real_ticker) > 0: BullBearPower_full = pd.concat([BullBearPower_full,BullBearPower_real_ticker])

    RSI_full = RSI_full.rename(columns= {0: 'RSI14'})
    df = df.join(RSI_full)

    ADX_full = ADX_full.rename(columns= {0: 'ADX'})
    df = df.join(ADX_full)
    
    STOCH_full_slowk = STOCH_full_slowk.rename(columns= {0: 'STOCH_slowk'})
    df = df.join(STOCH_full_slowk)

    STOCH_full_slowd = STOCH_full_slowd.rename(columns= {0: 'STOCH_slowd'})
    df = df.join(STOCH_full_slowd)
        
    STOCHRSI_full_fastk = STOCHRSI_full_fastk.rename(columns= {0: 'STOCHRSI_fastk'})
    df = df.join(STOCHRSI_full_fastk)

    STOCHRSI_full_fastd = STOCHRSI_full_fastd.rename(columns= {0: 'STOCHRSI_fastd'})
    df = df.join(STOCHRSI_full_fastd)
        
    MACD_real_full = MACD_real_full.rename(columns= {0: 'MACD_real'})
    df = df.join(MACD_real_full)

    MACD_signal_full = MACD_signal_full.rename(columns= {0: 'MACD_signal'})
    df = df.join(MACD_signal_full)

    MACD_hist_full = MACD_hist_full.rename(columns= {0: 'MACD_hist'})
    df = df.join(MACD_hist_full)

        
    Williams_full = Williams_full.rename(columns= {0: 'Williams'})
    df = df.join(Williams_full)
        
    CCI_full = CCI_full.rename(columns= {0: 'CCI'})
    df = df.join(CCI_full)
        
    ATR_full = ATR_full.rename(columns= {0: 'ATR'})
    df = df.join(ATR_full)
        
    HighsLows_full = HighsLows_full.rename(columns= {0: 'HighsLows'})
    df = df.join(HighsLows_full)
        
    UltimateOscillator_full = UltimateOscillator_full.rename(columns= {0: 'UltimateOscillator'})
    df = df.join(UltimateOscillator_full)
        
    ROC_full = ROC_full.rename(columns= {0: 'ROC'})
    df = df.join(ROC_full)
        
    BullBearPower_full = BullBearPower_full.rename(columns= {0: 'BullBearPower'})
    df = df.join(BullBearPower_full)

    if path.endswith('csv'):
        if len(df) > 0: df.to_csv(path,index = False)
    elif path.endswith('xlsx'):
        if len(df) > 0: df.to_excel(path,index = False)


# In[5]:


# filename = "30years_data_1h_interval.csv"
# path =  "./datasets/" + filename
# df = pd.read_csv(path)
# ticker_list = df['ticker'].to_list()
# ticker_list = list(set(ticker_list))

# for i in range(0,len(ticker_list)):
#     ticker = ticker_list[i]
#     df_ticker = df[(df['ticker'] == ticker)]


# In[6]:


# df_ticker.columns


# In[7]:


# real = talib.WILLR(df_ticker['high'], df_ticker['low'], df_ticker['close'], timeperiod=14)
# real


# In[8]:


#getting datasets
datasets_list = [f for f in listdir('./datasets') if isfile(join('./datasets', f))]

# datasets_list = [x for x in datasets_list if not x.endswith('.csv')]

datasets_list = list(set(datasets_list))


# In[9]:


for file in datasets_list:
    if (file.endswith('csv') and "~$" not in file) or (file.endswith('xlsx') and "~$" not in file):
        try:
            calculator(file)
        except Exception as e:
            print (e)
            print(file)
            print('------------------------')

