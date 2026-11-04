### Отчёт о запуске: что лежит в каждом датасете после запуска.
### Сохраняется в datasets/run_report.csv (выкладывается на FTP вместе с датасетами)
### и коротко уходит в Telegram (notify.py).
### Файлы читаются кусками и только 2 колонки, поэтому большие датасеты не грузятся в память целиком.
import datetime
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

    print(report.to_string(index=False))
    for warning in warnings:
        print('ПРЕДУПРЕЖДЕНИЕ: {}'.format(warning))

    return report, warnings


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
