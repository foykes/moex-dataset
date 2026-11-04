# Project audit

Date: 2026-05-13

Scope: read `AGENTS.md`, then inspected `data_gathering.py`, `tech.py`, `dividends.py`, `upload.py`, `settings/`, `README.md`, and current test files. No production reload, upload, or live MOEX API test was run. Code and dataset files were not changed.

## Author's coding style summary

- Notebook-first workflow with Jupytext `# %%` sections and paired `.ipynb`/`.py` files.
- Simple top-level functions, module-level globals, and procedural orchestration instead of classes.
- Pandas DataFrames are the main data structure; code favors explicit intermediate variables and loops.
- Russian comments, prints, and operational messages are part of the current style.
- Dataset schemas and filenames are implicit contracts: CSV/XLSX files are written directly under `datasets/`, with column names preserved by convention.
- Error handling is mostly `print` plus broad `except`, so failed downloads/calculations can be hard to detect automatically.
- Paths are usually built by string concatenation from `sys.path[0]` or a passed `current_path`.

## Concrete bugs and risks by file/function

### `data_gathering.py`

- `data_update()` builds `dataset_path` as `current_path + "datasets/{}.csv"` (`data_gathering.py:482`), missing the slash before `datasets`. With a normal `current_path`, existing files look absent.
- The missing-file branch in `data_update()` calls `full_reload(all_stocks_ru, interval, years, filename_j, word, current_path)` (`data_gathering.py:488`), but `full_reload()` requires `tickers_dates` (`data_gathering.py:429`). This becomes a `TypeError` as soon as the missing-file branch is reached.
- `moex()` loops with `range(1, years)` (`data_gathering.py:247`). For `years=1` it downloads nothing; for `years=10` it downloads 9 chunks. This conflicts with `main_tests.py:51-61`, which expects `moex(..., years=1, ...)` to return rows.
- `moex_tickerlists()` first prepares a stock/depositary receipt list, then overwrites `all_stocks_ru` with the full MOEX catalogue (`data_gathering.py:123-128`). If the intended dataset is all instruments, names and comments are misleading; if the intended dataset is stocks, the function currently returns too much.
- `full_reload()` assumes every ticker has one matching row in `tickers_dates` and that `issue_date` is valid (`data_gathering.py:449-458`). If `build_tickers_dates()` leaves `NaT`, `from=NaT` can be sent to MOEX; if a ticker is missing, `.values[0]` raises.
- `build_tickers_dates()` finds stopped dates only through the `shares` history endpoint (`data_gathering.py:287-289`) and filters `/iss/securities` rows to `group == "stock_shares"` (`data_gathering.py:370`). This is risky because `moex_query()` supports bonds too (`data_gathering.py:143-149`), and `moex_tickerlists()` currently returns all instruments.
- `ticker_list_actual.remove('')` (`data_gathering.py:537`) can raise `ValueError` when the empty ticker is absent.
- `moex_query()` uses `http://iss.moex.com/...` (`data_gathering.py:151`) while other code uses HTTPS. It also creates a new request each call (`data_gathering.py:169`) instead of reusing a session.
- `moex_query()` returns an empty DataFrame after all retries (`data_gathering.py:221-228`) but does not add the ticker to `exception_list`. Downstream code may treat partial downloads as normal empty data.
- Module import reads `settings/user_agents.json` and `settings/datasets_config.json` immediately from `sys.path[0]` (`data_gathering.py:24-42`). Imports from tests or other working directories can fail before any function is called.

### `tech.py`

- `STOCH_slowd` is filled from `STOCH_real_ticker_slowk` instead of `STOCH_real_ticker_slowd` (`tech.py:424-428`). This makes `STOCH_slowd` equal to the wrong series.
- `TRIX_full` is never updated because the result is assigned to `_full` (`tech.py:439-440`). The joined `TRIX` column is empty or missing useful values (`tech.py:733-734`).
- `SMA_full` is initialized and joined (`tech.py:51`, `tech.py:604-605`) but no `talib.SMA()` result is ever calculated.
- `MAVP_full` is joined twice (`tech.py:586-590`) while the actual calculation is commented out (`tech.py:310-312`).
- `calculator(filename)` uses the module-level `current_path` (`tech.py:22`, `tech.py:31-32`) even when `main(current_path)` is called with an explicit path (`tech.py:872-884`). Running from another entry point can read/write the wrong dataset directory.
- Indicator calculation assumes rows inside each ticker are already time-sorted (`tech.py:276-282`). If a dataset is unsorted, rolling indicators can be wrong without any error.
- `main()` catches every exception while calculating a file and continues (`tech.py:880-888`). This is convenient for manual runs but weak for automation because the process can finish successfully after skipped files.
- The function writes the original CSV/XLSX in place (`tech.py:846-850`). A failed or interrupted run can leave the previous dataset untouched, but there is no checksum, temp-file write, or summary of what was actually updated.

### `dividends.py`

- `divs_all` is global and is not cleared in `main()` (`dividends.py:24`, `dividends.py:71-93`). Calling `main()` twice in one process duplicates previous results.
- `headers` is defined but never used (`dividends.py:22`); `urlopen()` calls have no explicit timeout or User-Agent (`dividends.py:37`, `dividends.py:52`).
- One network or JSON error in `div_loader()` stops the whole dividends load; there is no per-ISIN retry or failure list.
- ISS responses are parsed by positional indexes (`dividends.py:42-44`, `dividends.py:55-59`). If MOEX changes column order, values can shift silently.
- `dropna(how='all')` keeps rows where only `ISIN` or only `TRADE_CODE` is missing (`dividends.py:75-78`), so requests like `q=nan` are possible.
- Multiple `secid` values for one ISIN can add duplicate dividend rows because there is no final `drop_duplicates()`.

### `upload.py`

- `gdoc_upload()` and `ftp_upload()` accept `current_path`, but both use module-level `datasets_path` created at import time (`upload.py:24`, `upload.py:33`, `upload.py:50`, `upload.py:107`). Passing another path to `main()` does not fully redirect IO.
- Google Sheet URL, service-account filename, and FTP secret filename are hardcoded (`upload.py:30`, `upload.py:46`, `upload.py:98`).
- FTP upload lists all top-level files in `datasets/` without filtering extensions (`upload.py:107-116`). The current repo contains `datasets/.DS_Store`, which would be uploaded.
- Upload blocks catch exceptions and print them (`upload.py:76-77`, `upload.py:117-118`, `upload.py:133-134`, `upload.py:149-150`), but `main()` does not return a failure summary or non-zero status.
- FTP connection setup is outside the upload `try` blocks (`upload.py:97-104`), and `quit()` is not protected by `finally` (`upload.py:152-153`).
- `gdoc_upload()` always reads `10years_data_1d_interval.csv` (`upload.py:50`) and keeps a hardcoded whitelist (`upload.py:55-60`), so publication behavior is not visible from config.

### `settings/`

- `settings/datasets_config.json` contains the full production set, including expensive 30-year minute/10-minute jobs (`settings/datasets_config.json:39-48`). Because `data_gathering.py` loads it at import, a default run is production-shaped.
- `settings/datasets_config_1_year.json` exists but is not wired into `data_gathering.main()` or documented as a safe smoke-test config.
- Config entries have no validation for required keys, interval values, filename collisions, or output paths.
- API retry count, success sleep, retry backoff, and ticker-date metadata cache behavior are hardcoded in Python instead of config.
- Some Edge User-Agent strings look malformed, with duplicated version fragments or trailing commas (`settings/user_agents.json:25`, `settings/user_agents.json:28`, `settings/user_agents.json:31-36`). This may reduce the usefulness of User-Agent rotation.

### `README.md`

- The README explains the dataset idea but not how to install dependencies, run safe partial loads, run tests, or configure secrets.
- Dataset scope is unclear: the title says stock trading data (`README.md:3`), but the current loader includes all MOEX instrument types in `moex_tickerlists()`.
- Statistics are static (`README.md:41-56`) and can drift from current local files and published data.
- The ToDo list uses `*✔️` without a space (`README.md:17-39`), which may render poorly in Markdown.
- Known limitations are not documented: MOEX API dependency, retry behavior, partial failure behavior, large XLSX limits, and publication secrets.

## Missing tests

- There is no pytest setup or dependency list. `pyproject.toml` only configures Jupytext.
- `main_tests.py` uses `unittest`, calls the real MOEX API in test methods (`main_tests.py:36`, `main_tests.py:55`), and also calls `data_gathering.moex()` at import time (`main_tests.py:65-70`).
- `tests.py` is an exploratory script, not an isolated test module. It calls real MOEX endpoints and writes `datasets/ticker_lists/ticker_dates.xlsx` (`tests.py:57`, `tests.py:89`, `tests.py:98`, `tests.py:102`).
- Current tests do not cover `get_next_header()`, retry behavior, status-code failures, malformed CSV responses, path construction, missing dataset files, ticker metadata failures, or new-ticker update behavior.
- No tests cover `tech.py` indicator correctness, especially `STOCH_slowd`, `TRIX`, `SMA`, per-ticker isolation, sorting assumptions, and re-running calculations on files that already contain indicator columns.
- No tests cover dividends parsing with mocked MOEX responses, duplicate secids, missing ISINs, or network failures.
- No tests cover upload behavior with mocked Google/FTP clients, missing secrets, file filtering, or partial upload failures.
- Settings are not validated by tests, so config typos can reach long-running production jobs.

## Performance bottlenecks

- `data_gathering.main()` refreshes the full ticker list and ticker-date metadata on every run (`data_gathering.py:565-568`). `build_tickers_dates()` can make two or three HTTP requests per ticker.
- `moex_query()` sleeps 3 seconds after every successful request (`data_gathering.py:176-177`) and retry sleep is hardcoded in function defaults (`data_gathering.py:136`, `data_gathering.py:216-219`).
- `moex_query()` does not reuse `requests.Session`; repeated candle requests pay connection setup costs.
- `full_reload()` and `data_update()` repeatedly `pd.concat()` inside loops (`data_gathering.py:461`, `data_gathering.py:531`, `data_gathering.py:550`).
- `tech.calculator()` repeatedly concatenates dozens of indicator series inside the ticker loop and then performs many `join()` calls (`tech.py:279-844`). This is likely the main local CPU/memory bottleneck.
- CSV and XLSX are both written for many datasets (`data_gathering.py:466-467`, `data_gathering.py:557-558`, `tech.py:847-850`). XLSX writes are expensive and capped by Excel row limits.
- 10-year datasets are downloaded separately from 30-year datasets. For the same interval, 10-year output may be sliceable from 30-year data after schema-preserving validation.
- `upload.main()` runs Google and FTP upload in parallel (`upload.py:173-178`), but both read large files from disk and failures are only printed.

## Documentation gaps

- No practical runbook for safe local development: install dependencies, run a small mocked test suite, run a one-year/small config, and avoid production reloads.
- No documented dataset schema for candle files, ticker lists, dividends, and technical indicators.
- No documented config glossary for `settings/datasets_config*.json` and `settings/user_agents.json`.
- No documented secrets layout for Google and FTP, or a safe way to run upload in dry-run/mock mode.
- No explanation of incremental update behavior, retry policy, skipped tickers, or how to inspect partial failures.
- No documentation of generated files that should not be committed, despite large datasets being present in the repo.

## Quick fixes suitable for small PRs

1. Add a minimal pytest scaffold and mark/remove live-network tests; use the existing CSV fixture for `moex_query()` parsing.
2. Fix `data_update()` path construction and pass `tickers_dates` to the missing-file `full_reload()` branch.
3. Fix the `moex()` year loop so `years=1` downloads one year, with a mocked test.
4. Guard `ticker_list_actual.remove('')` and missing `SUPERTYPE` lookups in `data_update()`.
5. Fix `tech.py` assignments for `STOCH_slowd` and `TRIX`, and either calculate `SMA` or document/remove the empty placeholder. Add focused indicator tests before changing output columns.
6. Make `tech.calculator()` accept/use a `current_path` argument from `tech.main()`.
7. Clear `divs_all` at the start of `dividends.main()`, drop rows missing `ISIN` or `TRADE_CODE`, and parse MOEX JSON by column names.
8. Add timeouts and small per-ISIN error collection to `dividends.py`.
9. Filter upload files by allowed extensions and skip `.DS_Store`; return or print a final failed-upload list.
10. Add a short README section for install, safe test run, and safe data update run.

## Larger follow-up tasks

- Add a lightweight metadata cache for ticker dates so normal incremental updates do not re-query every ticker every run.
- Introduce a shared, configurable request helper with `requests.Session`, retry limits, sleep/backoff settings, endpoint/ticker/date logging, and a failure report.
- Optimize `tech.calculator()` by calculating per-ticker indicator blocks, collecting frames in lists, and concatenating once while preserving existing column names.
- Decide and document dataset scope: stocks only, all securities, or separate configured universes.
- Add config validation and a safe smoke-test config path without changing the production default silently.
- Make publication configurable and testable: dataset whitelist, dry-run mode, mocked upload clients, and explicit failure status.
- Clean up README and dataset docs after tests cover current behavior and any schema-impacting changes are called out.
