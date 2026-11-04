# Python requirements & installation

How to set up a working environment for `moex-dataset`. The project uses a
notebook-first layout (paired `.ipynb` / `.py:percent` files), so dependencies
split into a **runtime** set (to run the data pipeline) and a **dev** set
(notebooks, repo hooks, tests).

Files:

- `requirements.txt` — runtime libraries used by the `.py` modules.
- `requirements-dev.txt` — tooling (jupytext, nbconvert, ipykernel, pre-commit, pytest).

## Python version

Targeted at the version pinned in `.venv/pyvenv.cfg`: **Python 3.14**.
Should also work on 3.11+. The listed version floors are chosen to be
compatible with 3.14.

## Quick start

```bash
# from the project root
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
python -m pip install --upgrade pip

# install TA-Lib's C library FIRST (see note below), then:
pip install -r requirements.txt              # run the pipeline
pip install -r requirements-dev.txt          # + notebooks / hooks / tests
```

After a successful, verified install, lock exact versions:

```bash
pip freeze > requirements.lock.txt
```

## Runtime dependencies

| Package          | Import         | Used by                                   | Purpose |
|------------------|----------------|-------------------------------------------|---------|
| `pandas`         | `pandas`       | all modules                               | DataFrames, `read_csv`/`to_csv`, `read_html`, `to_excel` |
| `requests`       | `requests`     | `data_gathering.py`, `dohodru_data.py`    | HTTP calls to MOEX ISS API and dohod.ru |
| `TA-Lib`         | `talib`        | `tech.py`                                 | Technical indicators (RSI, MACD, ADX, …) |
| `beautifulsoup4` | `bs4`          | `dohodru_data.py`                         | Parse dohod.ru dividend pages |
| `lxml`           | `lxml`         | `dohodru_data.py`                         | Parser backend for `BeautifulSoup('lxml')` and `pandas.read_html` |
| `openpyxl`       | (engine)       | `data_gathering`, `tech`, `dividends`, `dohodru_data`, `all`, `count_check`, `tests` | `.xlsx` engine for `to_excel`/`read_excel` |
| `pygsheets`      | `pygsheets`    | `upload.py`                               | Push the daily dataset to Google Sheets |

The standard library covers the rest (`datetime`, `csv`, `json`, `os`, `sys`,
`time`, `io`, `multiprocessing`, `urllib.request`, `ftplib`, `unittest`) — no
install needed.

### TA-Lib needs a system C library

`pip install TA-Lib` compiles a wrapper around the native TA-Lib C library,
which must be present **before** the pip install. This is the most common
installation failure.

```bash
# macOS (Homebrew)
brew install ta-lib

# Debian / Ubuntu — install the dev package, or build from source:
#   https://github.com/TA-Lib/ta-lib  (download, ./configure --prefix=/usr, make, make install)
```

If pip can't find the headers after installing the C library, point it at the
install prefix, e.g. on Apple Silicon:

```bash
export TA_INCLUDE_PATH="$(brew --prefix ta-lib)/include"
export TA_LIBRARY_PATH="$(brew --prefix ta-lib)/lib"
pip install -r requirements.txt
```

## Dev dependencies

| Package      | Why |
|--------------|-----|
| `jupytext`   | Keeps `.ipynb` and `.py:percent` in sync. Pinned to `1.19.2` to match `.pre-commit-config.yaml`. |
| `nbconvert`  | `main.py` runs `jupyter nbconvert --to script ...` to regenerate modules from notebooks. Needed to run `main.py` end-to-end. |
| `ipykernel`  | Runs the notebooks (Python 3 kernel). |
| `pre-commit` | Git hook runner (`.pre-commit-config.yaml`). Enable with `pre-commit install`. |
| `pytest`     | Test runner for new tests (per `AGENTS.md`). Existing `tests.py` / `main_tests.py` use stdlib `unittest`. |

## Notes & current gaps

- The `.venv` in this repo currently has **only the dev tooling** installed
  (jupytext, ipykernel, pre-commit, …); the runtime libraries above are not
  present. Install `requirements.txt` before running any data module.
- `nbconvert` is **not** in the current `.venv`, so `main.py`'s nbconvert step
  would fail until `requirements-dev.txt` is installed. To run a single stage
  without it, call the module directly (e.g. `python data_gathering.py`).
- `upload.py` (Google Sheets / FTP) needs credentials under a sibling
  `../secrets/` directory; those are intentionally not part of this repo and
  not a Python dependency.
- Versions are lower-bound floors, not the exact tested set. Generate a lock
  file with `pip freeze` after the first successful run to make installs
  reproducible.
