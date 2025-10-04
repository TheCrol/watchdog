from typing import TYPE_CHECKING

from telegram import Update
from telegram.ext import ContextTypes

from ..useful import ACCESS

if TYPE_CHECKING:
    from ..watchdog import App


class Start:
    def __init__(self, app: "App"):
        self.app = app
        self.db = app.db

    async def start(self):
        self.app.bot.register_command(
            "start",
            "Start the conversation with me",
            self.cmd_start,
            ACCESS.EVERYONE_DM,
        )

    async def cmd_start(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, args: str
    ):
        if update.effective_user is None or update.effective_message is None:
            return

        # Create a response depending on the user's role

        # This user is a regular user
        text = (
            f"Hello {update.effective_user.full_name}! I am the Watchdog bot, designed to manage the groups I am in.\n\n"
            "Use /help to see what I can do."
        )

        if update.effective_user.id in self.app.bot_admins:
            # This user is a bot admin
            groups = (group.title for group in self.db.groups.values())
            groups_str = ", ".join(groups)
            text = (
                f"Hello {update.effective_user.full_name}! You are the bot admin.\n"
                f"I am currently operating in the following groups: {groups_str}\n\n"
                "Use /help to see what I can do."
            )

        elif groups := self.db.get_groups_from_admin(update.effective_user.id):
            # This user is an admin in one or more groups
            groups_str = ", ".join(group.title for group in groups)
            text = (
                f"Hello {update.effective_user.full_name}! You are an admin in the following groups: {groups_str}\n\n"
                "Use /help to see what I can do."
            )

        await update.effective_message.reply_html(text)
