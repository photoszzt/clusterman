import logging

_default_handler = None


def setup_logger(logging_level, logging_format):
    """Setup default logging."""
    logger = logging.getLogger("cls")
    if type(logging_level) is str:
        logging_level = logging.getLevelName(logging_level.upper())
    logger.setLevel(logging_level)
    global _default_handler
    if _default_handler is None:
        _default_handler = logging.StreamHandler()
        logger.addHandler(_default_handler)
    _default_handler.setFormatter(logging.Formatter(logging_format))
    # Setting this will avoid the message
    # is propagated to the parent logger.
    logger.propagate = False
