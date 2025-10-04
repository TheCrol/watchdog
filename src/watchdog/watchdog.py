#!/usr/bin/env python3

import argparse
import asyncio
import contextlib
import logging
from typing import cast

import aiorun
import toml
from platformdirs import user_config_path, user_data_path

from .bot import Bot
from .botadmin import BotAdmin
from .db import DB
from .imagesearch import ImageSearch
from .logger import setup_logger
from .report import Report
from .start import Start
from .welcome import Welcome

log = logging.getLogger("App")


class App:
    def __init__(self):
        self.log_level, self.no_colour = self.parse_arguments()

        aiorun.run(
            self.run(),
            use_uvloop=True,
            shutdown_callback=self.on_shutdown,
            timeout_task_shutdown=5,
        )

    async def run(self):
        self.data_folder = user_data_path("watchdog", "crol")
        self.data_folder.mkdir(parents=True, exist_ok=True)
        self.config_folder = user_config_path("watchdog", "crol")
        self.config_folder.mkdir(parents=True, exist_ok=True)

        setup_logger(self.log_level, self.no_colour)

        log.info("Loading config...")
        if not self.load_config():
            asyncio.get_event_loop().stop()
            return

        self.db = DB(self)
        self.bot = Bot(self)

        log.info("Starting database...")
        await self.db.start()

        log.info("Starting telegram bot...")
        await self.bot.start()

        log.info("Starting programs...")
        self.botadmin = BotAdmin(self)

        report = Report(self)
        welcome = Welcome(self)
        imagesearch = ImageSearch(self)
        start = Start(self)

        await report.start()
        await welcome.start()
        await imagesearch.start()
        await start.start()

        log.info("Watchdog has started!")

    async def on_shutdown(self, _: asyncio.AbstractEventLoop):
        log.info("Stopping telegram bot...")
        with contextlib.suppress(AttributeError):
            await self.bot.stop()

        log.info("Stopping database...")
        with contextlib.suppress(AttributeError):
            await self.db.stop()

        log.info("Goodbye!")

    def parse_arguments(self) -> tuple[int, bool]:
        # Parse command line arguments
        # Returns (log_level, use_colour)

        parser = argparse.ArgumentParser(description="The Watchdog telegram bot")
        parser.add_argument(
            "-l",
            "--log-level",
            default="info",
            choices=["debug", "info", "warning", "error", "critical"],
            help="Set the logging level (default: info)",
        )
        parser.add_argument(
            "-nc",
            "--no-colour",
            action="store_false",
            help="Disable coloured output",
        )
        parsed = parser.parse_args()

        logger = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }
        log_level = logger.get(parsed.log_level, logging.INFO)
        no_colour: bool = parsed.no_colour
        return (log_level, no_colour)

    def load_config(self) -> bool:
        toml_file = self.config_folder / "config.toml"

        if not toml_file.exists():
            # Do an interactive setup
            config = {}
            print("Config file not found. Starting interactive setup...")
            config["bot_token"] = input("Enter your bot token: ").strip()
            admins_str = input("Enter bot admin IDs (space separated): ").strip()
            admins: list[int] = []
            for admin in admins_str.split():
                try:
                    admins.append(int(admin))
                except ValueError:
                    print(f"Invalid admin ID: {admin}. Skipping.")
            config["bot_admins"] = admins

            imghash_bin = input(
                "Enter path to imghash binary (or leave empty): "
            ).strip()
            config["imghash_bin"] = imghash_bin if imghash_bin else None

            # Save the config
            with toml_file.open("w") as f:
                toml.dump(config, f)

            print(f"Config was saved. If you need to change it, edit {toml_file}")

        else:
            # Load the file
            try:
                with toml_file.open("r") as f:
                    config = toml.load(f)
            except toml.TomlDecodeError as e:
                log.critical(f"Failed to parse config file: {e}")
                return False

        # Validate the config
        bot_token = config.get("bot_token", "")
        if not isinstance(bot_token, str) or ":" not in bot_token:
            log.critical("Invalid bot token in config")
            return False

        bot_admins = config.get("bot_admins", 0)

        imghash_bin = config.get("imghash_bin", None)

        if isinstance(bot_admins, int):
            bot_admins = [bot_admins]
        elif isinstance(bot_admins, list):
            if not all(isinstance(id, int) for id in bot_admins):
                log.critical("Invalid bot admin ID in config")
                return False
        else:
            log.critical("Invalid bot admin ID in config")
            return False

        if isinstance(imghash_bin, str):
            if imghash_bin == "":
                imghash_bin = None
        elif imghash_bin is not None:
            log.critical("Invalid imghash_bin in config. Should be string or empty")
            return False

        self.bot_token = bot_token
        self.bot_admins = cast(list[int], bot_admins)
        self.imghash_bin = imghash_bin

        return True
