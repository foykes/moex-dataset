# Тесты moex_query без реального API MOEX: ответы подменяются заглушками
import datetime

import pytest

import data_gathering


class FakeResponse:
    def __init__(self, status_code, text=''):
        self.status_code = status_code
        self.text = text


def candles_csv(n_rows, start_time):
    """CSV в формате MOEX ISS: строка 'candles', пустая строка, заголовок, данные."""
    lines = ['candles', '', 'open;close;high;low;value;volume;begin;end']
    for i in range(n_rows):
        begin = start_time + datetime.timedelta(minutes=i)
        end = begin + datetime.timedelta(seconds=59)
        lines.append('1;2;3;0.5;100;10;{};{}'.format(begin.strftime('%Y-%m-%d %H:%M:%S'), end.strftime('%Y-%m-%d %H:%M:%S')))
    return '\n'.join(lines) + '\n'


@pytest.fixture
def fake_api(monkeypatch):
    """Подменяет session.get: отдаёт ответы из списка по очереди и запоминает запросы."""
    calls = []
    responses = []

    def fake_get(url, headers=None, timeout=None):
        calls.append(url)
        return responses.pop(0)

    monkeypatch.setattr(data_gathering.session, 'get', fake_get)
    monkeypatch.setattr(data_gathering.time, 'sleep', lambda s: None)
    monkeypatch.setattr(data_gathering, 'exception_list', [])
    return calls, responses


t0 = datetime.datetime(2025, 1, 1, 10, 0)


def test_pagination_collects_all_pages(fake_api):
    calls, responses = fake_api
    responses += [
        FakeResponse(200, candles_csv(500, t0)),
        FakeResponse(200, candles_csv(500, t0 + datetime.timedelta(minutes=500))),
        FakeResponse(200, candles_csv(123, t0 + datetime.timedelta(minutes=1000))),
    ]

    df = data_gathering.moex_query('SBER', 'Акции', '2025-01-01', '2025-12-31', 1)

    assert len(df) == 1123
    assert (df['ticker'] == 'SBER').all()
    assert [c.split('&start=')[1] for c in calls] == ['0', '500', '1000']
    assert calls[0].startswith('https://')
    assert data_gathering.exception_list == []


def test_full_last_page_then_empty_page(fake_api):
    calls, responses = fake_api
    responses += [
        FakeResponse(200, candles_csv(500, t0)),
        FakeResponse(200, candles_csv(0, t0)),
    ]

    df = data_gathering.moex_query('SBER', 'Акции', '2025-01-01', '2025-12-31', 1)

    assert len(df) == 500
    assert len(calls) == 2


def test_no_data_returns_empty(fake_api):
    calls, responses = fake_api
    responses += [FakeResponse(200, candles_csv(0, t0))]

    df = data_gathering.moex_query('SBER', 'Акции', '2025-01-01', '2025-12-31', 24)

    assert len(df) == 0
    assert data_gathering.exception_list == []


def test_failed_page_keeps_downloaded_part_and_logs_exception(fake_api):
    calls, responses = fake_api
    responses += [FakeResponse(200, candles_csv(500, t0))]
    responses += [FakeResponse(503) for _ in range(3)]

    df = data_gathering.moex_query('SBER', 'Акции', '2025-01-01', '2025-12-31', 1, max_retries=3)

    assert len(df) == 500
    assert len(calls) == 4
    assert len(data_gathering.exception_list) == 1
    exception = data_gathering.exception_list[0]
    assert exception['ticker'] == 'SBER'
    assert exception['start'] == 500
    assert '503' in exception['error']


def test_retry_succeeds_after_error(fake_api):
    calls, responses = fake_api
    responses += [FakeResponse(500), FakeResponse(200, candles_csv(10, t0))]

    df = data_gathering.moex_query('SBER', 'Акции', '2025-01-01', '2025-12-31', 1)

    assert len(df) == 10
    assert data_gathering.exception_list == []


def test_same_page_twice_stops_loop(fake_api):
    calls, responses = fake_api
    responses += [FakeResponse(200, candles_csv(500, t0)) for _ in range(2)]

    df = data_gathering.moex_query('SBER', 'Акции', '2025-01-01', '2025-12-31', 1)

    assert len(df) == 500
    assert len(calls) == 2
