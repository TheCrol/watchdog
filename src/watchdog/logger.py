import logging
import sys

# ANSI escape sequences
RESET = "\033[0m"
BOLD = "\033[1m"
UNBOLD = "\033[22m"

FG_GREY = "\033[90m"
FG_WHITE = "\033[37m"
FG_YELLOW = "\033[33m"
FG_RED = "\033[31m"
FG_BLACK = "\033[38:5:232m"

BG_RED = "\033[41m"

LEVEL_STYLES = {
    logging.DEBUG: ("DEB", FG_GREY),  # grey
    logging.INFO: ("INF", ""),  # plain white
    logging.WARNING: ("WAR", FG_YELLOW),  # yellow
    logging.ERROR: ("ERR", FG_RED),  # red
    logging.CRITICAL: ("CRI", FG_BLACK + BG_RED),  # black on red bg
}


class AnsiFormatter(logging.Formatter):
    def __init__(self, use_colour: bool):
        super().__init__()
        self.use_colour = use_colour

    def format(self, record: logging.LogRecord) -> str:
        tag, format = LEVEL_STYLES.get(record.levelno, ("UNK", ""))

        # Time in grey (HH:MM)
        if self.use_colour:
            time_str = f"{FG_GREY}{self.formatTime(record, datefmt='%H:%M:%S')}{RESET}"
        else:
            time_str = f"{self.formatTime(record, datefmt='%H:%M:%S')}"

        # TAG [loggername] in bold + color
        if self.use_colour:
            tag_block = f"{BOLD}{format}[{record.name}]{UNBOLD}"
        else:
            tag_block = f"{tag} [{record.name}]"

        # Message in same color (but not bold)
        msg = record.getMessage()
        if self.use_colour:
            msg_str = f"{msg}{RESET}"
        else:
            msg_str = msg

        line = f"{time_str} {tag_block} {msg_str}"

        if record.exc_info:
            line += "\n" + super().formatException(record.exc_info)
        if record.stack_info:
            line += "\n" + self.formatStack(record.stack_info)

        return line


def setup_logger(level: int = logging.DEBUG, use_colour: bool = True) -> None:
    logging.basicConfig(level=level)
    root = logging.getLogger()
    # root.setLevel(level)

    # Kill any existing handlers so we don’t double-print
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(AnsiFormatter(use_colour))
    root.addHandler(handler)

    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("aiorun").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)
