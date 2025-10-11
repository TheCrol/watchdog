import asyncio
import logging
from typing import TYPE_CHECKING

from ..settings import AppConfig
from .config import LeaveGroupConfig

if TYPE_CHECKING:
    from ..watchdog import App

log = logging.getLogger("botadmin")


class BotAdmin:
    def __init__(self, app: "App"):
        self.app = app
        self.bot = app.bot
        self.db = app.db

    async def start(self):
        self.app.settings.register_config(
            AppConfig(
                button_emoji="🤖",
                name="Manage bot",
                description="Administrative actions for bot admins",
                display_order=90,
                configs=[
                    LeaveGroupConfig(self),
                ],
            )
        )

    def notify_sync(self, text: str):
        """Sync version of notify"""
        asyncio.create_task(self.notify(text))

    async def notify(self, text: str):
        """Notify all bot admins with a message"""
        message = f"❗ <b>Watchdog notification</b> ❗\n\n{text}"
        for admin_id in self.app.bot_admins:
            await self.bot.bot.send_message(admin_id, message, parse_mode="HTML")
