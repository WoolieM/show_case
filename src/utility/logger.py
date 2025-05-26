import logging
import os

# Set default log directory if the environment variable is not set
if 'LOCAL_LOGS' not in os.environ:
    os.environ['LOCAL_LOGS'] = './logs'  # Changed to './log' to align with our LOG_DIR

LOG_DIR = os.environ['LOCAL_LOGS']
LOG_FILE = os.path.join(LOG_DIR, "comprehensive.log")
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Ensure the log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# Create a file handler
file_handler = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter(LOG_FORMAT)
file_handler.setFormatter(formatter)

# Get the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(file_handler)

def get_logger(name, level=None):
    """
    Retrieves a logger with the specified name, configured to use the
    global file handler. You can optionally set a specific logging level
    for this logger.

    Args:
        name (str): The name of the logger (e.g., __name__, "database").
        level (int, optional): The logging level for this logger. If None,
                               it inherits from the root logger.

    Returns:
        logging.Logger: The configured logger instance.
    """
    logger = logging.getLogger(name)
    if level is not None:
        logger.setLevel(level)
    # logger.propagate = False
    return logger
