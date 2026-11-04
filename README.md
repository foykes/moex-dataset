# moex-dataset

<h1 align="center">Выгрузка данных торгов акциями с Мосбиржи (MOEX)</h1>
Используется официальное API https://iss.moex.com/iss/reference/.

Выгрузки доступны на [https://moex.foykes.com/](https://moex.foykes.com/).

<h3 align="center">Данные приготовлены в срезах:</h3>

Свечи по всем акциям и депозитарным распискам Мосбиржи + технические индикаторы (TA-Lib).

| Конфиг | Наборы |
|---|---|
| `settings/datasets_config.json` | 10 и 30 лет × свечи 1 день и 1 час; 1 год × 1 день, 1 час, 10 минут, 1 минута |
| `settings/datasets_config_1_year.json` | 1 год × свечи 1 день, 1 час, 10 минут, 1 минута |

Файлы: `datasets/<период>_data_<интервал>_interval.csv` (+ `.xlsx`, если влезает в лист Excel).
Колонки: `open, close, high, low, value, volume, begin, end, ticker`, затем индикаторы (RSI14, MACD, STOCH, ADX, ATR и др.).

Также: списки тикеров (`datasets/ticker_lists/`), дивиденды с доход.ру (`datasets/dividends/dohodru/`).

<h3 align="center">Как запустить</h3>

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
MOEX_CONFIG=datasets_config_1_year.json .venv/bin/python main.py --no-upload
```

- `main.py` — весь пайплайн: сбор данных → дивиденды → индикаторы → доход.ру → загрузка на FTP и в гугл док
- `main.py --full-reload` — выгрузить все датасеты с нуля, `--no-upload` — без загрузки
- `run.sh` — запуск на сервере (лог в `logs/`, защита от двух запусков одновременно)
- Сервер: [docs/vps.md](docs/vps.md)
- Тесты: `.venv/bin/pip install -r requirements-dev.txt && .venv/bin/python -m pytest -q`

Код пишется в ноутбуках (`.ipynb`), парные `.py` синхронизирует jupytext-хук при коммите (`pre-commit install`).

<h3 align="center">Что где</h3>

| Файл | Что делает |
|---|---|
| `data_gathering` | списки тикеров, даты листинга, выгрузка свечей (с пагинацией по 500), обновление датасетов |
| `tech` | технические индикаторы по каждому тикеру |
| `dividends` | дивиденды через ISS MOEX (сейчас доступны только подписчикам ISS — шаг пропускается) |
| `dohodru_data` | дивиденды с доход.ру |
| `upload` | загрузка на FTP и 10 лет дневных свечей в гугл док |
| `dataset_files.py` | чтение/запись датасетов по тикерам (в памяти один тикер), XLSX без расхода памяти |

<h3 align="center">ToDo:</h3>

* Highs/Lows(14), Bull/Bear Power(13)
* Данные с СПБ Биржи
* Выложить на hugging face
* План доработки: [docs/finish_plan.md](docs/finish_plan.md)
