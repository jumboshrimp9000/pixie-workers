import logging
import os
import sys
from pathlib import Path


def setup_logging() -> None:
    logging.getLogger().handlers.clear()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def get_order_logger(order_id: str) -> logging.Logger:
    log_dir = os.path.join('app', 'logs')
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(f'order_{order_id}')
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    log_file = os.path.join(log_dir, f'{order_id}.log')
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s'))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] - %(message)s'))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


def init_directories() -> None:
    for directory in [Path('app/logs')]:
        directory.mkdir(parents=True, exist_ok=True)


init_directories()
