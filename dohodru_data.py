#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests, pandas as pd, sys
from bs4 import BeautifulSoup

df_overview_full = pd.DataFrame()
df_years_full = pd.DataFrame()
df_each_full = pd.DataFrame()

current_path = sys.path[0]

url = 'https://www.dohod.ru/ik/analytics/dividend'

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}


# In[ ]:


### Составление общего списка
def get_main (url):
    
    html = requests.get(url, headers=headers).content
    df_list = pd.read_html(html)
    df_mainpage = df_list[-1]

    # очистка от ненужных колонок
    bad_columns = []
    for i in list(df_mainpage.columns):
        if "Unnamed:" in i or "Капитализация, sorting" in i:
            bad_columns.append(i)

    # print(bad_columns)
    df_mainpage.drop(columns=bad_columns, inplace=True)

    # len(df_mainpage)
    if len(df_mainpage) > 0:
        df_mainpage.to_excel('{}/datasets/dividends/dohodru/obzor_vsex_divs_tickerov.xlsx'.format(current_path))
    else:
        print('Не удалось скачать актуальные данные о дивах с доход.ру')
    return df_mainpage


# In[ ]:


def get_page_info (url):

    df_overview_full = pd.DataFrame()
    df_years_full = pd.DataFrame()
    df_each_full = pd.DataFrame()

    ### Сбор всех ссылок на страницы
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.text, 'lxml')

    data = []

    for link in soup.find_all('a', href=True):
        data.append(str(link.get('href')))


    data = list(set(data))

    url_list = []

    for i in data:
        if "/ik/analytics/dividend/" in i and ".pdf" not in i:
            url = "https://www.dohod.ru" + i
            url_list.append(url)
    

    print("Количество страниц для обхода: {}".format(len(url_list)))


    ### Обход каждой страницы

    b = []
    for url in url_list:
        html = requests.get(url, headers=headers).content
        try:
            df_list = pd.read_html(html)
        except:
            df_list = pd.DataFrame()
            print('Не смог найти таблицы на этой странице: {}',format(url))

        a = len(df_list)
        b.append(a)

        if len(df_list) == 2:
            df = df_list[0]
            df.columns = df.iloc[1]
            df.reset_index(inplace=True)
            df = df.drop (index= 1)
            df = df[['текущая доходность','доля от прибыли','индекс DSI']]
            df['page_url'] = url
            df['ticker'] = url.split(r'/')[-1].upper()
            df_overview_full = pd.concat([df_overview_full,df])

            df = df_list[1]
            df['page_url'] = url
            df['ticker'] = url.split(r'/')[-1].upper()
            df_years_full = pd.concat([df_years_full,df])

        elif len(df_list) == 3:
            df = df_list[0]
            df.columns = df.iloc[1]
            df.reset_index(inplace=True)
            df = df.drop (index= 1)
            df = df[['текущая доходность','доля от прибыли','индекс DSI']]
            df['page_url'] = url
            df['ticker'] = url.split(r'/')[-1].upper()
            df_overview_full = pd.concat([df_overview_full,df])

            df = df_list[1]
            df['page_url'] = url
            df['ticker'] = url.split(r'/')[-1].upper()
            df_years_full = pd.concat([df_years_full,df])

            df = df_list[2]
            df['page_url'] = url
            df['ticker'] = url.split(r'/')[-1].upper()
            df_each_full = pd.concat([df_each_full,df])


    ## Статистика для отладки. Сколько разных табличек спарсено
    o = list(set(b))

    for i in o:
        print("Тип {}. Количество: {}".format(i, b.count(i)))
        print("___")
    
    return df_overview_full, df_years_full, df_each_full


# In[ ]:


def main():
    df_mainpage = get_main(url)
    df_mainpage.to_excel('{}/datasets/dividends/dohodru/main_page.xlsx'.format(current_path))
    df_mainpage.to_csv('{}/datasets/dividends/dohodru/main_page.csv'.format(current_path))
    

    df_overview_full, df_years_full, df_each_full = get_page_info (url)
    df_overview_full.to_excel('{}/datasets/dividends/dohodru/rate_for_each_ticker.xlsx'.format(current_path))
    df_overview_full.to_csv('{}/datasets/dividends/dohodru/rate_for_each_ticker.csv'.format(current_path))

    df_years_full.to_excel('{}/datasets/dividends/dohodru/sum_each_year_divs.xlsx'.format(current_path))
    df_years_full.to_csv('{}/datasets/dividends/dohodru/sum_each_year_divs.csv'.format(current_path))

    df_each_full.to_excel('{}/datasets/dividends/dohodru/all_payments.xlsx'.format(current_path))
    df_each_full.to_csv('{}/datasets/dividends/dohodru/all_payments.csv'.format(current_path))


# In[ ]:


if __name__ == "__main__":
    main()

