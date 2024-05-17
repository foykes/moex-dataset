# %%
import requests
import pandas as pd
from bs4 import BeautifulSoup

df_overview_full = pd.DataFrame()
df_years_full = pd.DataFrame()
df_each_full = pd.DataFrame()

# %%
### Составление общего списка
url = 'https://www.dohod.ru/ik/analytics/dividend'
html = requests.get(url).content
df_list = pd.read_html(html)
df_mainpage = df_list[-1]

# %%
bad_columns = []
for i in list(df_mainpage.columns):
    if "Unnamed:" in i:
        bad_columns.append(i)

# print(bad_columns)
df_mainpage.drop(columns=bad_columns, inplace=True)

# %%
# len(df_mainpage)
df_mainpage.to_excel('obzor_vsex_divs_tickerov.xlsx')

# %%
### Сбор всех ссылок на страницы
page = requests.get(url)
soup = BeautifulSoup(page.text, 'lxml')

data = []

for link in soup.find_all('a', href=True):
    data.append(str(link.get('href')))


data = list(set(data))

url_list = []

for i in data:
    if "/ik/analytics/dividend/" in i and ".pdf" not in i:
        url = "https://www.dohod.ru/" + i
        url_list.append(url)

# %%
print("Количество страниц для обхода: {}".format(len(url_list)))

# %%
b = []
for url in url_list:
    html = requests.get(url).content
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


# %%
## Статистика для отладки. Сколько разных табличек спарсено
o = list(set(b))

for i in o:
    print(i)
    print(b.count(i))
    print("___")

# %%
df_overview_full.to_excel('rate_for_each_ticker.xlsx')
df_years_full.to_excel('sum_each_year_divs.xlsx')
df_each_full.to_excel('all_payments.xlsx')


