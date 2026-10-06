import time, datetime
from pathlib import Path


def main():
    # Notebook-пары проверяются в dev/CI до запуска, production их не конвертирует.
    print("Запуск скрипта: {}".format(datetime.datetime.now()))
    start_time = time.time()
    current_path = str(Path(__file__).resolve().parent)

    import data_gathering, dividends, tech, dohodru_data, upload

    data_gathering.main(current_path)

    dividends.main()

    tech.main(current_path)

    url = 'https://www.dohod.ru/ik/analytics/dividend'
    dohodru_data.main(url)

    gdoc_to_write = "https://docs.google.com/spreadsheets/d/1HXXoxcDVqIrWN6QEg5ij88AxcNAKT-G-xm2UTUfQe1Q/edit?usp=sharing" ## Гугл док для сохранения данных
    upload.main(current_path,gdoc_to_write)

    print("Скрипт закончил отрабатывать: {}".format(datetime.datetime.now()))
    end_time = time.time()
    elapsed_time = end_time - start_time
    mins, secs = divmod(elapsed_time, 60)
    hours, mins = divmod(mins, 60)
    print('Скрипт полностью закончил работу.\nЗаняло времени: {} часов, {} минут, {} секунд. Суммарно в секундах: {}'. format(round(hours), round(mins), round(secs), round(elapsed_time, 3), 'сек'))


if __name__ == "__main__":
    main()
