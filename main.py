from pathlib import Path
from DataComparer import DataComparer
from Logger import Logger
import time
import traceback


def main():
    Logger.setup_logging('app.log', 50000, True)
    start_time = time.time()
    Logger.log_info('Start processing')

    try:
        config_path = Path('paths.json').resolve()

        if not config_path.exists():
            Logger.log_error(f"Configuration file not found: {config_path}")
            return

        comparer = DataComparer(config_path, 'app.log')
        comparer.run()

        elapsed_time = time.time() - start_time
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
