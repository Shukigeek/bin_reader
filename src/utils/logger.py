import sys
from logging import Formatter, Logger, StreamHandler, FileHandler, getLogger

import src.utils.config as config


class AppLogger:
    """Logger that writes to both console and file with separate formatters."""

    def __init__(
        self, name: str = config.LOGGER_SETTINGS["LOG_NAME"], log_file: str = config.LOGGER_SETTINGS["LOG_FILE"]
    ):
        self.logger: Logger = getLogger(name)
        self.console_handler: StreamHandler | None = None
        self.file_handler: FileHandler | None = None

        if not self.logger.hasHandlers():
            self._setup_console_handler()
            self._setup_file_handler(log_file)
            self.logger.setLevel(config.LOGGER_SETTINGS["LOG_LEVEL"])

    @staticmethod
    def _console_formatter() -> Formatter:
        return Formatter("%(asctime)s - %(levelname)s - %(message)s")

    @staticmethod
    def _file_formatter() -> Formatter:
        return Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s() - %(message)s")

    def _setup_console_handler(self) -> None:
        self.console_handler = StreamHandler(sys.stdout)
        self.console_handler.setFormatter(self._console_formatter())
        self.logger.addHandler(self.console_handler)

    def _setup_file_handler(self, log_file: str) -> None:
        self.file_handler = FileHandler(log_file, encoding="utf-8")
        self.file_handler.setFormatter(self._file_formatter())
        self.logger.addHandler(self.file_handler)

    def get_logger(self) -> Logger:
        return self.logger


logger = AppLogger().get_logger()
