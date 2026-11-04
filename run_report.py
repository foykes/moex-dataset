### Отчёт о запуске: что лежит в каждом датасете после запуска.
### Сохраняется в datasets/run_report.csv (выкладывается на FTP вместе с датасетами)
### и коротко уходит в Telegram (notify.py).
### Файлы читаются кусками и только 2 колонки, поэтому большие датасеты не грузятся в память целиком.
import datetime
import html
from pathlib import Path

import pandas as pd

# Если последняя свеча датасета старше стольких дней — предупреждение (данные перестали обновляться)
stale_days = 7

CHUNK_ROWS = 500000


### Строки, тикеры, первая и последняя дата одного CSV
def dataset_stats(csv_path):
    rows = 0
    tickers = set()
    first_date = None
    last_date = None

    for chunk in pd.read_csv(csv_path, usecols=['ticker', 'begin'], chunksize=CHUNK_ROWS):
        rows = rows + len(chunk)
        tickers.update(chunk['ticker'].dropna().unique())
        # даты в файлах в ISO-формате, поэтому min/max строк = min/max дат
        begin = chunk['begin'].dropna().astype(str)
        if len(begin) > 0:
            if first_date is None or begin.min() < first_date:
                first_date = begin.min()
            if last_date is None or begin.max() > last_date:
                last_date = begin.max()

    return rows, len(tickers), first_date, last_date


### Отчёт по всем датасетам конфига. Возвращает (DataFrame отчёта, список предупреждений)
def build_report(current_path, config, now=None):
    if now is None:
        now = datetime.datetime.now()
    datasets_dir = Path(current_path) / 'datasets'

    report_rows = []
    warnings = []
    for dataset in config:
        filename = dataset['filename']
        csv_path = datasets_dir / (filename + '.csv')
        xlsx_path = datasets_dir / (filename + '.xlsx')

        if not csv_path.is_file():
            warnings.append('{}: файла нет'.format(filename))
            report_rows.append({'dataset': filename, 'rows': 0, 'tickers': 0, 'first_date': None, 'last_date': None,
                                'size_mb': 0, 'xlsx': False, 'checked_at': now.strftime('%Y-%m-%d %H:%M:%S')})
            continue

        rows, tickers, first_date, last_date = dataset_stats(csv_path)
        report_rows.append({
            'dataset': filename,
            'rows': rows,
            'tickers': tickers,
            'first_date': first_date,
            'last_date': last_date,
            'size_mb': round(csv_path.stat().st_size / 1024 / 1024, 1),
            'xlsx': xlsx_path.is_file(),
            'checked_at': now.strftime('%Y-%m-%d %H:%M:%S'),
        })

        if rows == 0:
            warnings.append('{}: пустой файл'.format(filename))
        elif last_date is not None and pd.Timestamp(last_date) < pd.Timestamp(now) - pd.Timedelta(days=stale_days):
            warnings.append('{}: последняя свеча {} (старше {} дней)'.format(filename, last_date[:10], stale_days))

    report = pd.DataFrame(report_rows)

    # сколько запросов к MOEX не удалось (пишет data_gathering.main)
    exceptions_path = datasets_dir / 'exception_list.csv'
    if exceptions_path.is_file():
        failed_requests = len(pd.read_csv(exceptions_path))
        if failed_requests > 0:
            warnings.append('неудачных запросов к MOEX: {} (datasets/exception_list.csv)'.format(failed_requests))

    datasets_dir.mkdir(parents=True, exist_ok=True)
    report.to_csv(datasets_dir / 'run_report.csv', index=False)
    build_index_html(current_path, report, now)

    print(report.to_string(index=False))
    for warning in warnings:
        print('ПРЕДУПРЕЖДЕНИЕ: {}'.format(warning))

    return report, warnings


### Страница сайта datasets/index.html со всеми файлами для скачивания.
### Корень FTP — папка /datasets/ сайта, поэтому ссылки относительные: moex.foykes.com/datasets/index.html
def _file_size(path):
    size_mb = path.stat().st_size / 1024 / 1024
    if size_mb >= 1024:
        return '{:.1f} ГБ'.format(size_mb / 1024)
    return '{:.1f} МБ'.format(size_mb)


def _link(datasets_dir, relative_path):
    path = datasets_dir / relative_path
    name = html.escape(relative_path)
    return '<a href="{0}" download>{1}</a> ({2})'.format(name, html.escape(path.name), _file_size(path))


def build_index_html(current_path, report, now=None):
    if now is None:
        now = datetime.datetime.now()
    datasets_dir = Path(current_path) / 'datasets'

    lines = ['<!DOCTYPE html>', '<html>', '<head>', '<meta charset="utf-8">',
             '<title>MOEX Dataset by foykes</title>',
             '<style>body{font-family:sans-serif;margin:24px;max-width:1100px}'
             'table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:4px 8px;text-align:left}</style>',
             '</head>', '<body>',
             '<h2>MOEX Dataset</h2>',
             '<p>Свечи всех акций и депозитарных расписок Мосбиржи (MOEX ISS) с техническими индикаторами.<br>',
             'Обновлено: {}</p>'.format(now.strftime('%Y-%m-%d %H:%M')),
             '<p>Большие файлы (больше 1 048 576 строк, не влезают в Excel) выложены сжатыми: <b>.csv.gz</b>.</p>',
             '<h3>Свечи</h3>',
             '<table><tr><th>Датасет</th><th>Файлы</th><th>Строк</th><th>Тикеров</th><th>Период</th></tr>']

    for row in report.itertuples(index=False):
        csv_name = row.dataset + '.csv'
        files = []
        if (datasets_dir / (csv_name + '.gz')).is_file():
            files.append(_link(datasets_dir, csv_name + '.gz'))
        elif (datasets_dir / csv_name).is_file():
            files.append(_link(datasets_dir, csv_name))
        if (datasets_dir / (row.dataset + '.xlsx')).is_file():
            files.append(_link(datasets_dir, row.dataset + '.xlsx'))
        if len(files) == 0:
            continue

        period = ''
        if isinstance(row.first_date, str) and isinstance(row.last_date, str):
            period = '{} — {}'.format(row.first_date[:10], row.last_date[:10])
        lines.append('<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>'.format(
            html.escape(row.dataset), '<br>'.join(files), row.rows, row.tickers, period))
    lines.append('</table>')

    ## Остальные файлы: списки тикеров, дивиденды, отчёт о запуске
    sections = [('Списки тикеров', 'ticker_lists'), ('Дивиденды', 'dividends'), ('Дивиденды с доход.ру', 'dividends/dohodru')]
    for title, folder in sections:
        folder_path = datasets_dir / folder
        if not folder_path.is_dir():
            continue
        files = sorted(f.name for f in folder_path.iterdir()
                       if f.is_file() and f.suffix in ('.csv', '.xlsx', '.gz') and '.tmp.' not in f.name)
        if len(files) == 0:
            continue
        lines.append('<h3>{}</h3>'.format(title))
        lines.append('<ul>')
        for file_name in files:
            lines.append('<li>{}</li>'.format(_link(datasets_dir, folder + '/' + file_name)))
        lines.append('</ul>')

    lines.append('<h3>Служебное</h3>')
    lines.append('<ul>')
    for file_name in ('run_report.csv', 'exception_list.csv'):
        if (datasets_dir / file_name).is_file():
            lines.append('<li>{}</li>'.format(_link(datasets_dir, file_name)))
    lines.append('</ul>')

    lines.append('<p><a href="https://github.com/foykes/moex-dataset" target="_blank" rel="noopener noreferrer">Github project</a></p>')
    lines.append('</body>')
    lines.append('</html>')

    with open(datasets_dir / 'index.html', 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines) + '\n')


### Короткий текст для Telegram
def report_text(status, steps, report, warnings, elapsed_seconds, config_name):
    hours, rest = divmod(int(elapsed_seconds), 3600)
    minutes = rest // 60

    lines = ['moex-dataset: {}'.format(status),
             'Конфиг: {}, время: {} ч {} мин'.format(config_name, hours, minutes),
             '']

    for name, step in steps.items():
        lines.append('{} {}: {} мин'.format('✅' if step['ok'] else '❌', name, round(step['seconds'] / 60, 1)))

    if report is not None and len(report) > 0:
        lines.append('')
        for row in report.itertuples(index=False):
            last_date = row.last_date[:10] if isinstance(row.last_date, str) else '-'
            lines.append('{}: {} строк, {} тикеров, до {}'.format(row.dataset, row.rows, row.tickers, last_date))

    if len(warnings) > 0:
        lines.append('')
        for warning in warnings:
            lines.append('⚠️ {}'.format(warning))

    return '\n'.join(lines)
