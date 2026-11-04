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

## 5. Progress
- Phase 0 ✅ `27faa27` — clean base, tag `codex-archive-2026-11`.
- Phase 1 ✅ `38f93cf` — pagination, https, session, `request_sleep`, `datasets/exception_list.csv`.
- Phase 2+3 ✅ — `MOEX_CONFIG` switch (default 1 year), short periods cut from long ones,
  dedupe by `(ticker, begin)` keep last, new tickers get full period, files trimmed to their period,
  safe CSV/XLSX writes, fixed ISO date parsing bug (day/month swap).
- Phase 4 ✅ — tech: indicators per ticker (one concat), file list from config (`"tech": false` to skip),
  `STOCH_slowd` fixed (was a copy of slowk), `TRIX` now saved (+1 column, 80 indicators);
  XLSX written row by row (`xlsx_utils.py`, xlsxwriter) — openpyxl needed ~600 MB RAM for 16k rows;
  `MOEX_XLSX=0` turns XLSX off. dohodru: pandas 3 `read_html` fix + timeouts.
  dividends: MOEX ISS dividends are now **subscribers only** → step stops with a message, old file kept.
- Phase 5 ✅ — upload logic kept (same sheet, same 10 columns, same FTP folders). Google: size check before
  `clear()`, chunked writes (20k rows) with 3 retries — one request with ~7M cells would fail.
  FTP: upload as `name.uploading` then rename, one failed file doesn't stop the rest, timeout 300 s,
  `.tmp.` files skipped, missing folders skipped. Failures → non-zero process exit (`main()` returns list).
  Not verified live (no writes to real FTP/Google from here).
- Phase 6 ✅ — `main.py` without nbconvert (`--full-reload`, `--no-upload`, exit code 1 on failure,
  upload skipped if data/tech failed), `run.sh` (lock, logs/, `MOEX_CONFIG` default = full config),
  `docs/vps.md`, README refresh. Phase 7 (tests) done along the way: 21 pytest tests, all mocked.
- Dry run 2026-11-04 (all tickers, 1y daily only, `--no-upload`): all steps OK, 8.4 min,
  83,729 rows × 89 columns, CSV 115 MB, XLSX 72 MB, peak RAM 460 MB, 0 failed requests.
- (resolved by the decision above) Scale of the full config: ~1.4 KB per row in CSV, ~5 KB per row peak RAM (whole file in pandas).
  Rough estimates: 10y daily ~0.7M rows → ~1 GB CSV, ~3.5 GB RAM; 10y hourly ~6M rows → ~9 GB CSV, ~30 GB RAM;
  10y 1-minute ~100M rows → ~140 GB CSV per file. 10m/1m for 10/30 years do not fit 2 GB RAM / 400 GB disk / daily FTP.
- Owner decision 2026-11-04: full config = daily + hourly for 10/30 years, 10m and 1m for 1 year.
- Phase 8 ✅ — per-ticker streaming so the full config fits 2 GB RAM: `dataset_files.py`
  (`iter_ticker_groups`, `append_csv`, XLSX from CSV in chunks). `data_update` / `full_reload` / `tech`
  keep one ticker in memory. Full reload resumes after a crash (`*.partial.csv` + `*.partial.progress`).
  Measured on 2.1M synthetic hourly rows: update 11 s / ~300 MB, tech 112 s / ~300 MB (before: ~10 GB).
  XLSX is now written only by `tech` (once, with indicators); data_gathering removes the stale one.
  Rough output sizes: 10y hourly ~5–6M rows ≈ 8–9 GB CSV, 1y 1m ≈ 15M rows ≈ 20 GB CSV, total ≈ 40–50 GB.
- Notes for later: pandas 3.1 warns about `inplace=` (works, just warnings);
  `tech.main` reads every file in `datasets/` (Phase 4 → use the config list);
  default `MOEX_CONFIG` → switch to full config in Phase 6 for the VPS cron.
