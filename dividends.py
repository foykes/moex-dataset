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
import urllib.request, urllib.parse, json
import re, time

current_path = sys.path[0]

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}

divs_all = []


# %%
### Функция выгрузки дивидендов по ISIN

def _select_secid(data, isin):
    # Search может вернуть похожие бумаги и переставить колонки. Shortname
    # не является SECID; принимать можно только точное совпадение ISIN.
    if type(data) is not dict or type(data.get('securities')) is not dict:
        raise ValueError('DIVIDEND_SEARCH_SCHEMA_INVALID: отсутствует таблица securities')
    columns = data['securities'].get('columns')
    rows = data['securities'].get('data')
    if (type(columns) is not list or type(rows) is not list
            or any(type(column) is not str for column in columns)
            or columns.count('secid') != 1 or columns.count('isin') != 1):
        raise ValueError('DIVIDEND_SEARCH_SCHEMA_INVALID: нужны единственные колонки secid и isin')
    secid_column = columns.index('secid')
    isin_column = columns.index('isin')
    secid_list = []
    matching_rows = 0
    for row in rows:
        if type(row) is not list or len(row) != len(columns):
            raise ValueError('DIVIDEND_SEARCH_SCHEMA_INVALID: длина строки не соответствует columns')
        if type(row[isin_column]) is not str or row[isin_column] != isin:
            continue
        matching_rows += 1
        secid = row[secid_column]
        # Не навязываем stocks-only шаблон кода: допустимый SECID остаётся
        # непрозрачным сегментом URL, но разделители/управляющие символы опасны.
        if (type(secid) is not str or not secid or secid in ('.', '..')
                or any(char.isspace() or ord(char) < 32 or 127 <= ord(char) <= 159
                       or char in '/\\?#' for char in secid)):
            raise ValueError('DIVIDEND_SECID_INVALID: недопустимый SECID точного совпадения')
        try:
            secid.encode('utf-8')
        except UnicodeEncodeError:
            raise ValueError('DIVIDEND_SECID_INVALID: SECID не кодируется в UTF-8') from None
        if secid not in secid_list:
            secid_list.append(secid)
    if not secid_list:
        raise ValueError('DIVIDEND_IDENTITY_NOT_FOUND: точный ISIN не найден')
    # Несколько разных кодов нельзя считать алиасами без evidence/контракта.
    # Одинаковые строки search — один кандидат, а не несколько выплат.
    if len(secid_list) != 1:
        raise ValueError('DIVIDEND_IDENTITY_AMBIGUOUS: точному ISIN соответствуют разные SECID')
    return secid_list[0], matching_rows


def _dividend_event(logging_context, level, event, message, ticker=None,
                    *, counts=None, outcome=None, duration_ms=None, error=None, file=None):
    if logging_context is None:
        return
    import run_logging
    fields = {'stage': 'dividends', 'instrument': ticker if type(ticker) is str else None,
              'outcome': outcome}
    if counts is not None:
        fields['counts'] = counts
    if duration_ms is not None:
        fields['duration_ms'] = duration_ms
    if error is not None:
        fields['error'] = error
    if file is not None:
        fields['file'] = file
    receipt = run_logging.emit_event(logging_context, level, event, message, fields)
    # INFO progress подтверждается barrier позже; confirmed=False здесь штатно.
    if (not receipt['accepted'] or level == 'ERROR' and not receipt['confirmed']
            or not run_logging.check_logging_health(logging_context)['healthy']):
        raise RuntimeError('LOGGING_INCOMPLETE')


def _dividend_failure(logging_context, error, ticker, endpoint, code):
    if logging_context is None:
        return
    try:
        import run_logging
        record = run_logging.make_error(logging_context, error, 'dividends', code=code)
        record['category'] = 'QUALITY' if code == 'DIVIDEND_ISIN_INVALID' else 'SOURCE'
        if code == 'LOGGING_INCOMPLETE':
            record['category'] = 'IO'
        # Endpoint входит только в существующий error envelope, не event.fields.
        for name, value in (('instrument', ticker), ('endpoint', endpoint)):
            if type(value) is str:
                record[name] = value
                record['null_reasons'].pop(name, None)
        _dividend_event(logging_context, 'ERROR', 'dividend_loader_failed',
            'Выгрузка дивидендов прервана: ' + code, ticker,
            outcome='FAILED', error=record)
    except BaseException:
        # Уже есть первичная ошибка. Вторичный отказ диагностики не заменяет
        # её; вызывающий stage также получает исходное исключение/nonzero.
        try:
            print('Не удалось записать диагностику дивидендов.', file=sys.stderr)
        except BaseException:
            pass


def div_loader(isin, ticker, *, logging_context=None):
    global divs_all

    query = None
    instrument = ticker
    failure_code = 'DIVIDEND_ISIN_INVALID'
    started = time.perf_counter()
    try:
        # Не превращаем NaN в строку "nan" и не исправляем identity догадками.
        if type(isin) is not str or re.fullmatch(r'[A-Z]{2}[A-Z0-9]{9}[0-9]', isin) is None:
            raise ValueError('DIVIDEND_ISIN_INVALID: требуется ISIN из 12 символов')
        query = 'https://iss.moex.com/iss/securities.json?' + urllib.parse.urlencode(
            {'q': isin, 'iss.meta': 'off'})
        failure_code = 'DIVIDEND_SEARCH_FAILED'
        _dividend_event(logging_context, 'INFO', 'dividend_identity_search_started',
            'Поиск точного ISIN', ticker, outcome='STARTED')
        with urllib.request.urlopen(query) as url:
            data = json.load(url)
        secid, matching_rows = _select_secid(data, isin)
        instrument = secid
        counts = {'rows_received': len(data['securities']['data']),
                  'rows_accepted': matching_rows, 'rows_quarantined': None,
                  'pages': 1, 'instruments': 1,
                  'null_reasons': {'rows_quarantined': 'NOT_EVALUATED'}}
        _dividend_event(logging_context, 'INFO', 'dividend_identity_selected',
            'Совпадений ISIN: {}; уникальных SECID: 1'.format(matching_rows),
            secid, counts=counts, outcome='SELECTED')

        query = 'https://iss.moex.com/iss/securities/{}/dividends.json'.format(
            urllib.parse.quote(secid, safe=''))
        failure_code = 'DIVIDEND_SOURCE_REQUEST_FAILED'
        _dividend_event(logging_context, 'INFO', 'dividend_request_started',
            'Запрос дивидендов выбранного SECID', secid, outcome='STARTED')
        with urllib.request.urlopen(query) as url:
            data = json.load(url)
        failure_code = 'DIVIDEND_SOURCE_SCHEMA_INVALID'
        rows = data['dividends']['data']
        for j in range(0, len(rows)):
            tmp = []
            date = rows[j][2]
            cash = rows[j][3]
            currency = rows[j][4]
            tmp.append(isin)
            tmp.append(ticker)
            tmp.append(date)
            tmp.append(cash)
            tmp.append(currency)
            divs_all.append(tmp)
        counts = {'rows_received': len(rows), 'rows_accepted': len(rows),
                  'rows_quarantined': None, 'pages': 1, 'instruments': 1,
                  'null_reasons': {'rows_quarantined': 'NOT_EVALUATED'}}
        _dividend_event(logging_context, 'INFO', 'dividend_loader_returned',
            'Получено событий из ответа: {}'.format(len(rows)), secid,
            counts=counts, outcome='RETURNED',
            duration_ms=(time.perf_counter() - started) * 1000)
        if logging_context is not None:
            import run_logging
            receipt = run_logging.flush_logging(logging_context)
            if not receipt['confirmed'] or not run_logging.check_logging_health(logging_context)['healthy']:
                raise RuntimeError('LOGGING_INCOMPLETE')
    except Exception as error:
        if error.args and type(error.args[0]) is str:
            code = error.args[0].split(':', 1)[0]
            if code in ('DIVIDEND_ISIN_INVALID', 'DIVIDEND_SEARCH_SCHEMA_INVALID',
                        'DIVIDEND_IDENTITY_NOT_FOUND', 'DIVIDEND_IDENTITY_AMBIGUOUS',
                        'DIVIDEND_SECID_INVALID', 'LOGGING_INCOMPLETE'):
                failure_code = code
        _dividend_failure(logging_context, error, instrument, query, failure_code)
        raise


# %%
def main(*, logging_context=None):
    _dividend_event(logging_context, 'INFO', 'dividend_collection_started',
        'Подготовка списка инструментов для дивидендов', outcome='STARTED')
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
        if logging_context is None:
            div_loader(isin, ticker)
        else:
            div_loader(isin, ticker, logging_context=logging_context)
        
    print('Выгружено записей о дивидендах: {}'.format(len(divs_all)))

    df_divs_all = pd.DataFrame(divs_all, columns=['ISIN','TRADE_CODE','dt','value','currency'])
    # Это размер прежнего экспортного входа, а не доказательство независимости
    # main/уникальности выплат: глобальный накопитель исправляется в C1-02.
    counts = {'rows_received': None, 'rows_accepted': len(df_divs_all),
              'rows_quarantined': None, 'pages': None, 'instruments': len(df_isin),
              'null_reasons': {'rows_received': 'NOT_MEASURED_FOR_EXPORT_INPUT',
                              'rows_quarantined': 'NOT_EVALUATED', 'pages': 'NOT_APPLICABLE'}}
    _dividend_event(logging_context, 'INFO', 'dividend_export_input',
        'Строк в экспортном входе: {}'.format(len(df_divs_all)),
        counts=counts, outcome='INPUT_PREPARED', file='datasets/dividends/all')

    path = current_path + "/datasets/dividends/" + "all"
    if len(df_divs_all) > 0: df_divs_all.to_excel(path + ".xlsx",index = False)
    if len(df_divs_all) > 0: df_divs_all.to_csv(path + ".csv",index = False)
    _dividend_event(logging_context, 'INFO', 'dividend_collection_returned',
        'Этап дивидендов вернулся; проверка публикации не выполнялась', outcome='RETURNED')

# %%
if __name__ == "__main__":
    main()
