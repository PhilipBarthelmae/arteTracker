import logging
import colorlog
from logging.handlers import RotatingFileHandler

#create logger ready for moprt into other files
log = logging.getLogger("my_logger")
log.setLevel(logging.DEBUG)

# Console handler with color
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
color_formatter = colorlog.ColoredFormatter(
    "%(asctime)s %(log_color)s[%(levelname)s] %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
    log_colors={
        'DEBUG': 'white',
        'INFO': 'cyan',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
)
console_handler.setFormatter(color_formatter)

# File handler with timestamp
file_handler = RotatingFileHandler("Data/log.txt", mode='a', maxBytes = 2_000_000, backupCount = 5)
file_handler.setLevel(logging.INFO)
plain_formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
file_handler.setFormatter(plain_formatter)

# Add handlers
log.addHandler(console_handler)
log.addHandler(file_handler)

