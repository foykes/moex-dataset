# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.2
#   kernelspec:
#     display_name: base
#     language: python
#     name: python3
# ---

# %%
import pandas as pd, sys
import urllib.request, urllib.parse, json, os

current_path = sys.path[0]

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}

divs_all = []


# %%
### Функция выгрузки дивидендов по ISIN
### Возвращает False, если MOEX не отдал блок dividends (с 2026 года дивиденды в ISS доступны только подписчикам)

def div_loader(isin, ticker):
    global divs_all

    ## Выгрузка по ISIN всех возможных secid, к которым привязаны дивиденды
    # isin = "RU0009029540"
    # ticker = "SBER"
    query  = "https://iss.moex.com/iss/securities.json?q={}&iss.meta=off".format(urllib.parse.quote(isin))
    with urllib.request.urlopen(query, timeout=30) as url:
        data = json.load(url)

    # номер колонки secid берём по названию: MOEX менял порядок колонок
    secid_idx = data['securities']['columns'].index('secid')
    secid_list = []
    for i in range(0, len(data['securities']['data'])):
        secid = data['securities']['data'][i][secid_idx]
        secid_list.append(secid)


    ## Выгрузка дивидендов по каждому secid
    for i in range(0, len(secid_list)):
        query = "https://iss.moex.com/iss/securities/{}/dividends.json?iss.meta=off".format(urllib.parse.quote(secid_list[i]))
        with urllib.request.urlopen(query, timeout=30) as url:
            data = json.load(url)

        # вместо дивидендов пришло описание бумаги — доступ к дивидендам закрыт
        if 'dividends' not in data:
            return False

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

    return True


# %%
def main():
    ## Подготовка списка для чего будут выгружаться дивиденды
    path = current_path + "/datasets/ticker_lists/moex_full.csv"
    df = pd.read_csv(path, index_col=0)
    df_isin = df[['TRADE_CODE','ISIN']]
    df_isin = df_isin.dropna(how='all')
    df_isin.drop_duplicates(keep='first', inplace=True)
    df_isin.reset_index(drop=True, inplace=True)

    

    errors = 0
    for i in range(0, len(df_isin)):
        isin = df_isin['ISIN'][i]
        ticker = df_isin['TRADE_CODE'][i]
        if pd.isna(isin) or str(isin).strip() == '':
            continue

        try:
            available = div_loader(isin, ticker)
        except Exception as e:
            # ошибка по одной бумаге не останавливает всю выгрузку
            errors = errors + 1
            print('Не удалось выгрузить дивиденды {} {}: {}'.format(ticker, isin, e))
            continue

        if available == False:
            print('MOEX не отдаёт дивиденды (доступно только подписчикам ISS). Выгрузку дивидендов MOEX пропускаю, старый файл не трогаю')
            break

    print('Ошибок при выгрузке дивидендов: {}'.format(errors))
        
    print('Выгружено записей о дивидендах: {}'.format(len(divs_all)))

    df_divs_all = pd.DataFrame(divs_all, columns=['ISIN','TRADE_CODE','dt','value','currency'])

    os.makedirs(current_path + "/datasets/dividends", exist_ok=True)
    path = current_path + "/datasets/dividends/" + "all"
    if len(df_divs_all) > 0: df_divs_all.to_excel(path + ".xlsx",index = False)
    if len(df_divs_all) > 0: df_divs_all.to_csv(path + ".csv",index = False)

# %%
if __name__ == "__main__":
    main()
