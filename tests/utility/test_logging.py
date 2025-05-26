import sys
import os


# Add the 'src' directory to Python's path
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
sys.path.insert(0, src_path)

from src.utility.logger import setup_logging, get_loggers

if __name__ == "__main__":
    # # Set the log folder (optional - defaults to './logs' relative to where you run the script)
    os.environ['LOCAL_LOGS'] = './my_test_logs'

    # Initialize the logging system using your YAML configuration
    # Assuming log_batch_json.yaml is in utility/config/
    setup_logging(log_schema='batch_json')

    # Get the root logger ('all_output')
    root_logger = get_loggers()
    root_logger.debug("This is a debug message for the file log.")
    root_logger.info("This info will go to both file and stdout.")
    root_logger.warning("Watch out, a warning!")

    # Get the metrics logger ('monitoring')
    metrics_logger = get_loggers('monitoring')
    metrics_logger.metrics({"cpu_usage": 0.85, "memory_free": "2GB"}, message="System metrics")

    # Get the root logger again to test stdout
    root_logger.error("An error occurred that will be on stdout as JSON.")

    print("Logging setup complete. Check the './my_test_logs' directory for log files.")