import logging
from typing import TYPE_CHECKING

from telegram import Update
from telegram.ext import ContextTypes

from ..bot import ChatDataRegister
from ..settings import AppConfig
from ..useful import mention_html
from .config import GroupEnableConfig, MessageConfig
from .db import DB, GroupDB

if TYPE_CHECKING:
    from ..watchdog import App

log = logging.getLogger("welcome")


class Welcome:
    def __init__(self, app: "App"):
        self.app = app
        self.bot = app.bot
        self.db = app.db

        self.registers: dict[int, ChatDataRegister] = {}

    async def start(self):
        self.config = await self.db.get_app_config("welcome", DB)

        for group_id, config in self.config.groups.items():
            if not config.enabled:
                break
            self.add_group_registers(group_id)

        self.app.settings.register_config(
            AppConfig(
                button_emoji="👋",
                name="Welcome message",
                description="Sends a welcome message to new users when they join the group",
                display_order=30,
                configs=[
                    GroupEnableConfig(self),
                    MessageConfig(self),
                ],
            )
        )

    def get_group_config(self, group_id: int) -> GroupDB:
        if group_id not in self.config.groups:
            self.config.groups[group_id] = GroupDB()
        return self.config.groups[group_id]

    def add_group_registers(self, group_id: int):
        if group_id in self.registers:
            return

        chat_data_register = self.bot.register_chat_data(self.bot_chat_data, group_id)
        self.registers[group_id] = chat_data_register

    def remove_group_registers(self, group_id: int):
        registers = self.registers.get(group_id)
        if not registers:
            return

        registers.deregister_chat_data()
        del self.registers[group_id]

    async def bot_chat_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Check for any new members joining the chat
        if (
            not update.message
            or not update.message.new_chat_members
            or not update.effective_chat
        ):
            return

        # Is this in a group we are enabled for?
        group_id = update.effective_chat.id
        config = self.get_group_config(group_id)
        if not config.enabled:
            return

        # Greet each new member
        for new_member in update.message.new_chat_members:
            if new_member.is_bot:
                continue

            name = new_member.full_name
            mention = mention_html(new_member, True)
            user_id = new_member.id

            message = config.message.format(name=name, mention=mention, user_id=user_id)

            await update.message.reply_html(message)

    async def save_db(self):
        await self.db.set_app_config("welcome", self.config)
