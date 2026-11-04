# moex-dataset — plan to finish and run on VPS

Date: 2026-11-04. Goal: a simple, working pipeline running daily on a VPS. No big process, no rewrite.

## 1. Status

### Before Codex (`5e4eb07` on GitHub + local uncommitted fixes in this folder)
- Simple scripts: `data_gathering` → `dividends` → `tech` → `dohodru_data` → `upload` (Google Sheet + FTP), run by `main.py`.
- Local working tree has extra fixes: `Path`-based `current_path`, retries + User-Agent rotation,
  full_reload from listing date, safer date parsing in `data_update`.
- **Critical bug: no pagination.** MOEX ISS candles returns max 500 rows per request.
  Every ticker in every local dataset has exactly 500 rows (e.g. SBER daily: 2016-07 → 2018-06 only).
  So all published datasets are incomplete. Earlier year-by-year loop had the same bug for 1h/10m/1m.
- Other known issues (from Obsidian notes): slow `tech.py` (concat in loops), README stale,
  no tests, `tickers_dates` misses some tickers, XLSX everywhere, `main.py` runs `nbconvert` on every start.

### After Codex (`origin/main` = `e7800ad`, plus ~80 branches)
- ~40 PRs merged, +99k lines (80 test files, offline guards, logging framework, Windows/Linux lock files).
- `data_gathering.py` grew from 646 to ~3000 lines; `run_logging.py` 1343, `dataset_io.py` 715, `data_quality.py` 683.
- Pagination **was** added (A3), but buried in heavy abstractions.
- Never run end to end. Upload/release lanes (D2–D6), all-time (A8), tech integration (B3/B4) not done.
  Handoff itself says "No product readiness".
- Verdict: too complex to maintain by one person. Do not continue that process.

## 2. Strategy

- Base on **your own pre-Codex code** (local folder), not on Codex `main`.
- Keep Codex work safe: tag it (`codex-archive-2026-11`), do not delete branches.
- Take only *ideas* from Codex (pagination, https, timeouts), written in your simple style.
- Keep notebook/script pairs (`.ipynb` is the source, `.py` exported via jupytext).

## 3. Phases (each = one small PR)

**Phase 0 — Clean base**
- Tag Codex `main`; create branch `simple` from `5e4eb07`; commit local fixes there.
- Remove generated data from git tracking going forward (datasets live on VPS/FTP).
- Later make `simple` the new `main` (normal commit that replaces files, no force-push).

**Phase 1 — Fix data download (the core)**
- `moex_query`: page with `&start=0,500,1000…` until a page has < 500 rows. HTTPS. One `requests.Session`.
- Configurable sleep (default ~0.3–1 s instead of 3 s), keep retries + UA rotation (already there).
- Failed requests → `exception_list` with ticker/interval/dates, printed at the end and saved to CSV.
- Quick test run on 3–5 tickers locally.

**Phase 2 — Decide realistic dataset scope**
- With real pagination, 1m and 10m history is huge (SBER 1m for 10 years ≈ 2M rows, ~4000 requests).
- Proposed: daily 10y/30y, hourly 10y/30y, 10m and 1m only 1 year (`datasets_config_1_year.json` already exists).
- Download the longest horizon once, cut shorter horizons from it (your roadmap item).
- XLSX only when rows < 1,048,576, CSV always (already the rule).

**Phase 3 — Incremental update**
- `data_update`: download from last date per ticker, dedupe on `(ticker, begin)` keep last.
- New tickers get full history, not just 365 days.
- `full_reload` only by explicit flag.

**Phase 4 — tech, dividends, dohod**
- `tech.py`: compute per ticker with `groupby`, collect into a list, one `concat` at the end. Same column names.
- Check it fits VPS memory on the full daily/hourly data.
- `dividends.py` / `dohodru_data.py`: run, fix only what breaks.

**Phase 5 — Upload**
- Keep FTP + Google Sheet (10y daily, 10 columns), check cell limit before writing.
- Option: VPS serves files with nginx directly (then FTP is not needed).

**Phase 6 — Runner + VPS**
- `main.py`: no `nbconvert` at runtime; plain `print` + log file per run; non-zero exit on failure.
- VPS: Python venv, `apt install ta-lib` (or build), `secrets/` folder outside git,
  first full load once in `tmux`, then daily cron with `flock` (no parallel runs).
- Short README: how to install, run, where the files go.

**Phase 7 — Minimal tests**
- pytest with mocked responses: pagination, retries, `data_update` merge/dedupe, tech on a small fixture.

## 4. Owner decisions (2026-11-04)
1. Base on pre-Codex code. Codex `main` archived as tag `codex-archive-2026-11`. Work branch: `simple`.
2. Develop and test with `settings/datasets_config_1_year.json` (1d/1h/10m/1m, 1 year).
   Full `datasets_config.json` (10y/30y) is run later on the VPS; it may take days — that is fine.
   Config choice must be a simple switch, not a code edit.
3. Upload logic stays the same: FTP to moex.foykes.com + Google Sheet.
4. VPS: Linux, 400 GB disk, small RAM (to confirm) → keep memory use low in `tech.py`.
5. Notebooks stay the source. Sync `.py` with jupytext (pre-commit hook already exists) instead of
   `nbconvert` inside `main.py`, so the VPS just runs the committed `.py` files.
