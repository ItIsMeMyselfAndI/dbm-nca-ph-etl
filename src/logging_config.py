import logging.config
import sys
import os


def setup_logging():
    is_lambda = os.environ.get("AWS_LAMBDA_FUNCTION_NAME") is not None

    handlers_definitions = {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "stream": sys.stdout,
        }
    }

    active_handlers = ["console"]

    # add file handler only if not in lambda
    if not is_lambda:
        handlers_definitions["file"] = {
            "class": "logging.FileHandler",
            "formatter": "standard",
            "filename": "pipeline.log",
            "mode": "a",
        }
        active_handlers.append("file")

    LOG_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": handlers_definitions,
        "loggers": {
            "": {
                "handlers": active_handlers,
                "level": "INFO",
                "propagate": True,
            },
            "httpx": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False,
            },
            "httpcore": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False,
            },
        },
    }

    logging.config.dictConfig(LOG_CONFIG)
