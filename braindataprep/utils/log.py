import logging
from pathlib import Path


def setup_filelog(filename: str | Path | None) -> None:
    """
    Set file log
    """
    if not filename:
        return
    handler = logging.FileHandler(str(filename))
    handler.setFormatter(logging.Formatter(
        "(%(asctime)s)\t[%(levelname)-5.5s]\t%(message)s\t{%(name)s}"
    ))
    logging.getLogger().addHandler(handler)


class LoggingOutputSuppressor:
    """Context manager to prevent global logger from printing"""

    def __init__(self, logger) -> None:
        self.logger = logger

    def __enter__(self) -> None:
        logger = self.logger
        if isinstance(logger, str):
            logger = logging.getLogger(logger)
        self.orig_handlers = logger.handlers
        for handler in self.orig_handlers:
            logger.removeHandler(handler)

    def __exit__(self, exc, value, tb) -> None:
        logger = self.logger
        if isinstance(logger, str):
            logger = logging.getLogger(logger)
        for handler in self.orig_handlers:
            logger.addHandler(handler)
