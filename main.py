#main.py
from pathlib import Path
from DataComparer import DataComparer
from Logger import Logger
import time
import traceback


def main():
    Logger.setup_logging('app.log', 50000, True)  # Настраиваем логирование
    start_time = time.time()  # Запоминаем время начала выполнения
    Logger.log_info('Start processing')

    try:
        paths_file = Path('paths.json').resolve()  # Разрешение абсолютного пути
        if not paths_file.exists():
            Logger.log_error(f"Configuration file not found: {paths_file}")
            return
        comparer = DataComparer(paths_file, 'app.log')
        comparer.run('price_file_path', 'site_file_path', 'Код', 'IE_XML_ID')

        end_time = time.time()  # Запоминаем время окончания выполнения
        elapsed_time = end_time - start_time  # Вычисляем затраченное время
        Logger.log_info('Processing completed successfully')
        Logger.log_info(f'Elapsed time: {elapsed_time:.2f} seconds')

    except FileNotFoundError as e:
        Logger.log_error(f"File not found: {e}")
        Logger.log_error("Stack trace: " + traceback.format_exc())
    except Exception as e:
        Logger.log_error(f"An error occurred: {e}")
        Logger.log_error("Stack trace: " + traceback.format_exc())


if __name__ == "__main__":
    main()
