from typing import TYPE_CHECKING

from telegram import Update
from telegram.ext import ContextTypes

from ..useful import ACCESS

if TYPE_CHECKING:
    from ..watchdog import App


class Help:
    def __init__(self, app: "App"):
        self.app = app
        self.bot = app.bot

    async def start(self):
        self.bot.register_command(
            "help",
            "Show the list of available commands",
            self.cmd_help,
            ACCESS.EVERYONE_DM,
        )

    async def cmd_help(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, args: str
    ):
        if (user := update.effective_user) is None or (
            message := update.effective_message
        ) is None:
            return

        # Generate a list of commands, filter by those who are meant for admins
        commands: list[str] = []
        admin_commands: list[str] = []

        for command, items in self.app.bot.commands.items():
            for item in items:
                if self.bot.has_access_to_command(item, user.id, None):
                    text = f"/{command} - {item.description}"
                    if item.access in (ACCESS.EVERYONE_DM, ACCESS.EVERYONE):
                        commands.append(text)
                    else:
                        admin_commands.append(text)

        if not commands and not admin_commands:
            await message.reply_text("You have no access to any commands")
            return

        text = ""

        if commands:
            text = "<b>Available commands:</b>\n"
            text += "\n".join(sorted(commands))

            if admin_commands:
                text += "\n\n"

        if admin_commands:
            text += "<b>Admin specific commands:</b>\n"
            text += "\n".join(sorted(admin_commands))

        await message.reply_text(text, parse_mode="HTML")
