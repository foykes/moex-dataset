#!/usr/bin/env bash
## Запуск пайплайна на VPS (вручную или из cron).
##   ./run.sh                 — обновление + загрузка
##   ./run.sh --no-upload     — без загрузки
##   ./run.sh --full-reload   — всё с нуля
## Пример cron (каждый день в 04:00):  0 4 * * * /home/USER/moex-dataset/run.sh
set -u
cd "$(dirname "$0")"

## Какой конфиг датасетов: datasets_config.json — 10 и 30 лет, datasets_config_1_year.json — 1 год
export MOEX_CONFIG="${MOEX_CONFIG:-datasets_config.json}"

mkdir -p logs
LOG="logs/run_$(date +%Y-%m-%d_%H-%M).log"

## Если прошлый запуск ещё идёт (полная выгрузка может идти днями) — новый не стартует
exec 9>logs/run.lock
if ! flock -n 9; then
    started="$(cat logs/run.started 2>/dev/null)"
    echo "$(date '+%F %T') прошлый запуск (старт $started) ещё работает, этот пропущен" >> logs/skipped.log
    .venv/bin/python notify.py "moex-dataset: прошлый запуск ещё работает (старт $started), сегодняшний пропущен"
    exit 0
fi
date '+%F %T' > logs/run.started

echo "Конфиг: $MOEX_CONFIG, аргументы: $*" >> "$LOG"
.venv/bin/python -u main.py "$@" >> "$LOG" 2>&1
code=$?
echo "exit code: $code" >> "$LOG"

## 0 — успех, 3 — ошибки в шагах (main.py уже написал в Telegram).
## Любой другой код — скрипт упал целиком (ошибка Python вне шагов, убит из-за нехватки памяти и т.д.)
if [ "$code" -ne 0 ] && [ "$code" -ne 3 ]; then
    .venv/bin/python notify.py "moex-dataset: скрипт упал с кодом $code, смотри $LOG"
fi

## Логи старше 60 дней удаляем
find logs -name 'run_*.log' -mtime +60 -delete

exit $code
