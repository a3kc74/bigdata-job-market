import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """Configure and return a logger with standard settings."""
    logger = logging.getLogger(name)
    
    # Initialize logger only if no handlers are configured yet
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )
        handlers = [
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("project.log")
        ]

        for handler in handlers:
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        logger.setLevel(logging.INFO)
        
    return logger
