### Уведомления в Telegram.
### Настройки в папке secrets рядом с проектом (как у upload.py): ../secrets/telegram.json
###   {"token": "123456:ABC...", "chat_id": "123456789", "notify_success": true}
### notify_success: false — писать только об ошибках. Нет файла — уведомления выключены.
###
### Из командной строки (так пишет run.sh):  python notify.py "текст сообщения"
import json
import sys
from pathlib import Path

import requests


def telegram_settings(current_path):
    settings_path = Path(current_path).resolve().parent / 'secrets' / 'telegram.json'
    if not settings_path.is_file():
        return None
    with open(settings_path, 'r', encoding='utf-8') as f:
        return json.load(f)


### Отправить сообщение. success=True — сообщение об успешном запуске (отправляется, если notify_success)
def send_telegram(text, current_path, success=False):
    settings = telegram_settings(current_path)
    if settings is None:
        print('Telegram не настроен (нет secrets/telegram.json), сообщение не отправлено')
        return False
    if success and not settings.get('notify_success', True):
        return False

    url = 'https://api.telegram.org/bot{}/sendMessage'.format(settings['token'])
    try:
        response = requests.post(url, data={'chat_id': settings['chat_id'], 'text': text[:4000]}, timeout=30)
        response.raise_for_status()
        return True
    except Exception as e:
        # в тексте ошибки requests есть URL с токеном бота — печатаем только тип ошибки
        print('Не удалось отправить сообщение в Telegram: {}'.format(type(e).__name__))
        return False


if __name__ == '__main__':
    send_telegram(' '.join(sys.argv[1:]), Path(__file__).resolve().parent)
