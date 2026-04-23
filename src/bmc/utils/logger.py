import logging

def log_execution(logger, message, level=logging.WARNING, **kwargs):
    """
    Helper function to log in case a logger is provided. Falls back to print.
    Accepts kwargs (like exc_info=True) to pass directly to the logger.

    Parameters
    ----------

    logger : logging.Logger
        A logger object
    message : str
        The string that is to be recorded by the logging function 

    See Also
    --------

    src.cube.spatiotemporal.spatiotemporal_cube._setup_pipeline_logger
    """
    if logger:
        logger.log(level, message, **kwargs)
    # Setup safeguard to print message when logger is None
    else:
        print(message)