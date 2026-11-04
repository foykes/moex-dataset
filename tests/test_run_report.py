# Тесты отчёта о запуске и уведомлений (без реального Telegram)
import datetime
import json

import pandas as pd

import notify
import run_report


def write_dataset(path, rows):
    pd.DataFrame(rows, columns=['open', 'close', 'begin', 'ticker']).to_csv(path, index=False)


def test_report_counts_and_warnings(tmp_path):
    datasets = tmp_path / 'datasets'
    datasets.mkdir()
    write_dataset(datasets / 'fresh.csv', [
        [1, 1, '2026-11-01 00:00:00', 'SBER'], [1, 1, '2026-11-03 00:00:00', 'SBER'], [1, 1, '2026-11-02 00:00:00', 'GAZP']])
    write_dataset(datasets / 'old.csv', [[1, 1, '2026-09-01 00:00:00', 'SBER']])
    pd.DataFrame({'ticker': ['SBER', 'GAZP']}).to_csv(datasets / 'exception_list.csv', index=False)
    config = [{'filename': 'fresh'}, {'filename': 'old'}, {'filename': 'missing'}]

    report, warnings = run_report.build_report(tmp_path, config, now=datetime.datetime(2026, 11, 4, 12, 0))

    fresh = report[report['dataset'] == 'fresh'].iloc[0]
    assert fresh['rows'] == 3 and fresh['tickers'] == 2
    assert fresh['first_date'] == '2026-11-01 00:00:00' and fresh['last_date'] == '2026-11-03 00:00:00'
    assert any('old' in w and '2026-09-01' in w for w in warnings)
    assert any('missing' in w for w in warnings)
    assert any('неудачных запросов к MOEX: 2' in w for w in warnings)
    assert not any(w.startswith('fresh') for w in warnings)
    assert (datasets / 'run_report.csv').is_file()

    text = run_report.report_text('успешно', {'tech': {'ok': True, 'seconds': 120}}, report, warnings, 3700, 'cfg.json')
    assert 'fresh: 3 строк, 2 тикеров, до 2026-11-03' in text
    assert '✅ tech: 2.0 мин' in text
    assert '1 ч 1 мин' in text


def project_with_telegram(tmp_path, settings):
    project = tmp_path / 'moex-dataset'
    project.mkdir()
    if settings is not None:
        (tmp_path / 'secrets').mkdir()
        (tmp_path / 'secrets' / 'telegram.json').write_text(json.dumps(settings))
    return project


class FakeResponse:
    def raise_for_status(self):
        pass


def test_telegram_not_configured(tmp_path):
    project = project_with_telegram(tmp_path, None)
    assert notify.send_telegram('текст', project) is False


def test_telegram_sends_message(tmp_path, monkeypatch):
    project = project_with_telegram(tmp_path, {'token': 'T0KEN', 'chat_id': '42'})
    sent = []
    monkeypatch.setattr(notify.requests, 'post', lambda url, data, timeout: sent.append((url, data)) or FakeResponse())

    assert notify.send_telegram('привет', project) is True
    assert sent == [('https://api.telegram.org/botT0KEN/sendMessage', {'chat_id': '42', 'text': 'привет'})]


def test_success_message_can_be_turned_off(tmp_path, monkeypatch):
    project = project_with_telegram(tmp_path, {'token': 'T0KEN', 'chat_id': '42', 'notify_success': False})
    monkeypatch.setattr(notify.requests, 'post', lambda *a, **k: (_ for _ in ()).throw(AssertionError('не должно отправляться')))

    assert notify.send_telegram('всё хорошо', project, success=True) is False


def test_telegram_error_does_not_print_token(tmp_path, monkeypatch, capsys):
    project = project_with_telegram(tmp_path, {'token': 'T0KEN', 'chat_id': '42'})

    def failing_post(url, data, timeout):
        raise notify.requests.ConnectionError('failed: ' + url)

    monkeypatch.setattr(notify.requests, 'post', failing_post)

    assert notify.send_telegram('текст', project) is False
    assert 'T0KEN' not in capsys.readouterr().out


def test_index_html_links_published_files(tmp_path):
    datasets = tmp_path / 'datasets'
    (datasets / 'ticker_lists').mkdir(parents=True)
    write_dataset(datasets / 'big.csv', [[1, 1, '2026-11-03 10:00:00', 'SBER']])
    (datasets / 'big.csv.gz').write_bytes(b'gz')
    write_dataset(datasets / 'small.csv', [[1, 1, '2026-11-03 00:00:00', 'SBER']])
    (datasets / 'small.xlsx').write_bytes(b'xlsx')
    (datasets / 'ticker_lists' / 'moex_full.csv').write_text('x')
    config = [{'filename': 'big'}, {'filename': 'small'}]

    run_report.build_report(tmp_path, config, now=datetime.datetime(2026, 11, 4, 12, 0))
    page = (datasets / 'index.html').read_text(encoding='utf-8')

    assert 'href="big.csv.gz"' in page and 'href="big.csv"' not in page
    assert 'href="small.csv"' in page and 'href="small.xlsx"' in page
    assert 'href="ticker_lists/moex_full.csv"' in page
    assert 'Обновлено: 2026-11-04 12:00' in page
    assert '2026-11-03 — 2026-11-03' in page
