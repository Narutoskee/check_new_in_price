# Logger.py
import logging
from logging.handlers import RotatingFileHandler

class Logger:
    """
    Класс Logger предоставляет статические методы для настройки и использования системы логирования.
    """

    @staticmethod
    def setup_logging(log_file, max_bytes=5000000, backup_count=5, log_level=logging.INFO, console_output=False):
        """
        Настраивает систему логирования.
        :param log_file: Путь к файлу журнала.
        :param max_bytes: Максимальный размер файла журнала в байтах.
        :param backup_count: Количество резервных копий файлов журнала.
        :param log_level: Уровень логирования.
        :param console_output: Флаг для включения вывода логов в консоль.
        """
        handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logger = logging.getLogger()
        logger.setLevel(log_level)
        logger.addHandler(handler)

        if console_output:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

    @staticmethod
    def log_debug(message):
        """Записывает отладочное сообщение в журнал."""
        logging.debug(message)

    @staticmethod
    def log_info(message):
        """Записывает информационное сообщение в журнал."""
        logging.info(message)

    @staticmethod
    def log_warning(message):
        """Записывает предупреждающее сообщение в журнал."""
        logging.warning(message)

    @staticmethod
    def log_error(message):
        """Записывает сообщение об ошибке в журнал."""
        logging.error(message)

    @staticmethod
    def log_critical(message):
        """Записывает критическое сообщение в журнал."""
        logging.critical(message)
