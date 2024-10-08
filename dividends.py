#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd, sys
import urllib.request, json

current_path = sys.path[0]

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}

divs_all = []


# In[ ]:


### Функция выгрузки дивидендов по ISIN

def div_loader(isin, ticker):
    global divs_all

    ## Выгрузка по ISIN всех возможных secid, к которым привязаны дивиденды
    # isin = "RU0009029540"
    # ticker = "SBER"
    query  = "https://iss.moex.com/iss/securities.json?q={}&iss.meta=off".format(isin)
    with urllib.request.urlopen(query) as url:
        data = json.load(url)


    secid_list = []
    for i in range(0, len(data['securities']['data'])):
        secid = data['securities']['data'][i][1]
        secid_list.append(secid)


    ## Выгрузка дивидендов по каждому secid
    isin_div = []

    for i in range(0, len(secid_list)):
        query = "https://iss.moex.com/iss/securities/{}/dividends.json".format(secid_list[i])
        with urllib.request.urlopen(query) as url:
            data = json.load(url)

        for j in range (0,len(data['dividends']['data'])):
            tmp = []
            date = data['dividends']['data'][j][2]
            cash = data['dividends']['data'][j][3]
            currency = data['dividends']['data'][j][4]

            tmp.append(isin)
            tmp.append(ticker)
            tmp.append(date)
            tmp.append(cash)
            tmp.append(currency)

            divs_all.append(tmp)


# In[ ]:


def main():
    ## Подготовка списка для чего будут выгружаться дивиденды
    path = current_path + "/datasets/ticker_lists/moex_full.xlsx"
    df = pd.read_excel(path)
    df_isin = df[['TRADE_CODE','ISIN']]
    df_isin = df_isin.dropna(how='all')
    df_isin.drop_duplicates(keep='first', inplace=True)
    df_isin.reset_index(drop=True, inplace=True)

    

    for i in range(0, len(df_isin)):
        isin = df_isin['ISIN'][i]
        ticker = df_isin['TRADE_CODE'][i]
        div_loader(isin, ticker)
        
    print('Выгружено записей о дивидендах: {}'.format(len(divs_all)))

    df_divs_all = pd.DataFrame(divs_all, columns=['ISIN','TRADE_CODE','dt','value','currency'])

    path = current_path + "/datasets/dividends/" + "all"
    if len(df_divs_all) > 0: df_divs_all.to_excel(path + ".xlsx",index = False)
    if len(df_divs_all) > 0: df_divs_all.to_csv(path + ".csv",index = False)


# In[ ]:


if __name__ == "__main__":
    main()

