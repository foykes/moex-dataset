# Running on the VPS

Linux VPS, 400 GB disk, small RAM. Commands below are for Debian/Ubuntu.

## 1. Install (once)

```bash
sudo apt update
sudo apt install -y git python3 python3-venv tmux
python3 --version        # must be 3.11 or newer (pandas 3 and TA-Lib wheels need it)
```

If Python is older than 3.11 (Ubuntu 22.04 ships 3.10), install a newer one first
(for example `sudo apt install python3.12 python3.12-venv` on Ubuntu 24.04, or `uv python install 3.12`).

```bash
cd ~
git clone --depth 1 https://github.com/foykes/moex-dataset.git   # main; --depth 1: old history is large
cd moex-dataset
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

TA-Lib installs from a ready wheel on Linux — no C library build needed.

## 2. Secrets (once)

`upload.py` reads secrets from a `secrets` folder **next to** the project folder (`~/secrets`), never from git.
Copy them from your Mac:

```bash
ssh USER@VPS 'mkdir -p ~/secrets && chmod 700 ~/secrets'
scp ~/Documents/petprojects/prod/secrets/python-304621-1e403209f05f.json USER@VPS:~/secrets/
scp ~/Documents/petprojects/prod/secrets/moex.foykes.com.json USER@VPS:~/secrets/
ssh USER@VPS 'chmod 600 ~/secrets/*'
```

### Telegram alerts (optional, recommended)

1. In Telegram open `@BotFather` → `/newbot` → copy the token.
2. Send any message to your new bot, then open `https://api.telegram.org/bot<TOKEN>/getUpdates`
   in a browser and copy `"chat":{"id": ...}`.
3. On the VPS:

```bash
cat > ~/secrets/telegram.json <<'JSON'
{"token": "<TOKEN>", "chat_id": "<CHAT_ID>", "notify_success": true}
JSON
chmod 600 ~/secrets/telegram.json
cd ~/moex-dataset && .venv/bin/python notify.py "moex-dataset: тест"
```

You get a message after every run (`"notify_success": false` = only problems): status, time per step,
rows / tickers / last date per dataset, warnings (stale data, failed MOEX requests). Also a message when
cron finds the previous run still working, and when the script crashes. No file = alerts off.

## 3. Swap (safety net)

Datasets are processed one ticker at a time (~300 MB RAM measured), so 2 GB is enough. Swap is a safety net.

```bash
sudo fallocate -l 8G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

## 4. Test run (1 year, no upload)

```bash
cd ~/moex-dataset
MOEX_CONFIG=datasets_config_1_year.json ./run.sh --no-upload
tail -f logs/run_*.log      # in a second terminal
```

## 5. First full load (runs for days)

Run inside `tmux` so it survives SSH disconnects:

```bash
tmux new -s moex
cd ~/moex-dataset && ./run.sh        # default config: datasets_config.json (1d/1h 10 and 30 years; 1d/1h/10m/1m 1 year)
# detach: Ctrl+B, then D. Re-attach later: tmux attach -t moex
```

Missing dataset files are downloaded from scratch automatically. `./run.sh --full-reload` forces it for all files.
If the run stops (reboot, crash), just start `./run.sh` again: the full load continues from the last finished ticker
(`datasets/*.partial.csv` + `*.partial.progress`).

Expected size on disk: ~20–25 GB of CSV (10y hourly ≈ 4–5 GB, 1y 1-minute ≈ 10 GB) plus `.csv.gz` copies of the big
files. FTP gets only the `.csv.gz` of big files: ~8–10 GB per run. To make the 1-minute file even smaller, set
`"tech": false` for it in `settings/datasets_config.json` (no indicators, ~10× smaller).

## 6. Daily update (cron)

```bash
crontab -e
# add:
0 4 * * * /home/USER/moex-dataset/run.sh
```

`run.sh` takes a lock: if the previous run (e.g. the first full load) is still working, the new one exits and
writes a line to `logs/skipped.log`.

## 7. Where to look

| What | Where |
|---|---|
| Run logs | `logs/run_YYYY-MM-DD_HH-MM.log` (kept 60 days), last line = exit code |
| Failed MOEX requests | `datasets/exception_list.csv` |
| Run report (rows, tickers, first/last date, size per dataset) | `datasets/run_report.csv` (also uploaded to FTP) |
| Datasets | `datasets/*.csv`, `*.xlsx`, `datasets/ticker_lists/`, `datasets/dividends/` |

Exit code `3` = some step failed (log ends with `Шаги с ошибками: [...]`); any other non-zero code = the script crashed.
If data gathering or tech fails, upload is skipped so FTP and Google keep the previous data.

## 7a. Website

The FTP root is the `/datasets/` folder of moex.foykes.com. Every run uploads `datasets/index.html`
(all files with size, rows, period and "updated on" time) → **https://moex.foykes.com/datasets/index.html**.
The old hand-written page at https://moex.foykes.com/ is not touched by the pipeline: replace it once with a
redirect or a link to `/datasets/index.html` (its links to `ticker_lists/` and `dividends/` are broken).

Big datasets (>1,048,576 rows, no XLSX) are published only as `.csv.gz`; small ones as `.csv` + `.xlsx`.

## 8. Settings (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `MOEX_CONFIG` | `datasets_config.json` in `run.sh` (`datasets_config_1_year.json` when running notebooks/py directly) | which datasets to build |
| `MOEX_XLSX` | `1` | `0` = do not write XLSX files |

Request pause: `request_sleep` at the top of `data_gathering.ipynb` (1 s). Raise it if MOEX starts returning errors/429.
Per dataset in the config, `"tech": false` skips indicators for that dataset.

## 9. Updating the code

```bash
cd ~/moex-dataset && git pull
```

Notebooks are the source; the jupytext pre-commit hook keeps `.py` in sync on commit, the VPS runs the `.py`.

Tested with: Python 3.14, pandas 3.1, numpy 2.5, TA-Lib 0.8.3, requests 2.34, xlsxwriter 3.3, openpyxl 3.1,
pygsheets 2.0.6, beautifulsoup4 4.15, lxml 6.1.
