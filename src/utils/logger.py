import logging

def log_execution(logger, message, level=logging.WARNING):
    """
    helper function to log in case a logger is provided (does nothing when logger is None)
    level determines the nature of the message that is being loggede
        logging.INFO
        logging.WARNING
    """
    if logger:
        logger.log(level, message)
