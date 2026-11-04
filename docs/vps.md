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
git clone -b simple https://github.com/foykes/moex-dataset.git
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
cd ~/moex-dataset && ./run.sh        # default config: datasets_config.json (1d/1h 10 and 30 years, 10m/1m 1 year)
# detach: Ctrl+B, then D. Re-attach later: tmux attach -t moex
```

Missing dataset files are downloaded from scratch automatically. `./run.sh --full-reload` forces it for all files.
If the run stops (reboot, crash), just start `./run.sh` again: the full load continues from the last finished ticker
(`datasets/*.partial.csv` + `*.partial.progress`).

Expected size: ~40–50 GB of CSV/XLSX in total (10y hourly ≈ 8–9 GB, 1y 1-minute ≈ 20 GB). FTP uploads all of it
on every run, so the daily upload time depends on the VPS bandwidth. To make the 1-minute file smaller, set
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
| Datasets | `datasets/*.csv`, `*.xlsx`, `datasets/ticker_lists/`, `datasets/dividends/` |

Exit code `1` = some step failed; the log ends with `Шаги с ошибками: [...]`.
If data gathering or tech fails, upload is skipped so FTP and Google keep the previous data.

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
