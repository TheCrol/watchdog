import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from telegram import Message, Update
from telegram.ext import ContextTypes

from ..bot import ChatDataRegister, CommandRegister
from ..botadmin import AppConfig, AppEnabledConfig
from ..useful import ACCESS, mention_html, pluralize

if TYPE_CHECKING:
    from ..watchdog import App

log = logging.getLogger("report")


@dataclass
class Config:
    enabled: bool


class Report:
    def __init__(self, app: "App"):
        self.app = app
        self.bot = app.bot
        self.db = app.db

        self.configs: dict[int, Config] = {}
        self.registers: dict[int, tuple[CommandRegister, ChatDataRegister]] = {}

        self.unhandled_reports: dict[int, list[Message]] = {}

    async def start(self):
        self.configs = await self.db.get_app_configs("reports", Config)

        for group_id, config in self.configs.items():
            if not config.enabled:
                break
            self.add_group_registers(group_id)

        self.app.botadmin.register_config(
            AppConfig(
                button_emoji="🚨",
                name="Reports",
                description="Allows people to @admin mention or call /admin to get attention of group admins",
                display_order=10,
                configs=[
                    AppEnabledConfig(
                        get_callback=self.botadmin_get_enabled,
                        set_callback=self.botadmin_set_enabled,
                    )
                ],
            )
        )

    def add_group_registers(self, group_id: int):
        if group_id in self.registers:
            return

        command_register = self.bot.register_command(
            "admin",
            "[reason] - Notify the admins",
            self.cmd_admin,
            ACCESS.EVERYONE,
            group_id,
        )
        chat_data_register = self.bot.register_chat_data(self.bot_chat_data, group_id)

        self.registers[group_id] = (command_register, chat_data_register)

    def remove_group_registers(self, group_id: int):
        registers = self.registers.get(group_id)
        if not registers:
            return

        command, chat_data = registers
        command.deregister_command()
        chat_data.deregister_chat_data()
        del self.registers[group_id]

    def botadmin_get_enabled(self, group_id: int) -> bool:
        if config := self.configs.get(group_id):
            return config.enabled
        return False

    async def botadmin_set_enabled(self, group_id: int, value: bool) -> None:
        if config := self.configs.get(group_id):
            config.enabled = value
        else:
            self.configs[group_id] = Config(enabled=value)

        await self.db.set_app_config("reports", group_id, self.configs[group_id])

        if value:
            self.add_group_registers(group_id)
        else:
            self.remove_group_registers(group_id)

    async def report(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, args: None | str
    ):
        if (
            update.message is None
            or update.message.text is None
            or update.effective_user is None
            or update.effective_chat is None
        ):
            return

        # This will cause the main message to be forwarded
        extra_text = args if args else None

        # This will cause the context message to be forwarded
        message_context = update.message.reply_to_message
        if message_context:
            assert message_context.from_user is not None

        notes: list[str] = []

        if message_context and message_context.from_user:
            notes.append(
                f"<b>Report about {mention_html(message_context.from_user, True)}</b>"
            )

        notes.append(f"<b>Reported by {mention_html(update.effective_user, True)}</b>")

        if update.message.link is None:
            return

        link = update.message.link
        # Remove the thread part of the link if it exists
        if "?thread=" in link:
            link = link.split("?thread=")[0]

        notes.append(f"<a href='{link}'>Direct link to the message</a>")

        if extra_text and message_context:
            notes.append("")
            notes.append(
                "The reporter's message and the message that is reported are included below:"
            )

        elif extra_text:
            notes.append("")
            notes.append("The reporter's message is included below:")

        elif message_context:
            notes.append("")
            notes.append("The message that is reported is included below:")

        else:
            notes.append("No additional context was provided")

        notes_str = "\n".join(notes)

        group_admins = self.db.get_group_admins(update.effective_chat.id)

        count: int = 0

        for admin in group_admins:
            # First send the message
            try:
                message = await context.bot.send_message(
                    admin.id,
                    f"🚨 <b>Admin required in {update.effective_chat.title}</b> 🚨\n\n{notes_str}",
                    parse_mode="HTML",
                )

                self.unhandled_reports.setdefault(update.effective_chat.id, []).append(
                    message
                )

                if extra_text:
                    # Then forward the reported message
                    await context.bot.forward_message(
                        admin.id,
                        update.effective_chat.id,
                        update.message.message_id,
                    )
                if message_context:
                    # Then forward the context message
                    await context.bot.forward_message(
                        admin.id,
                        update.effective_chat.id,
                        message_context.message_id,
                    )

                count += 1

            except Exception as e:
                log.error(f"Failed to send admin report to {admin.name}: {e}")

        await update.message.reply_html(
            f"<i>Reported to {pluralize(count, 'admin', 'admins')}</i>"
        )

    async def bot_chat_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Detect if @admin or @admins is mentioned

        if not update.message or not update.effective_user or not update.effective_chat:
            return

        # First check if this is an admin dealing with an unhandled report
        if self.db.is_admin_of_group(
            update.effective_user.id, update.effective_chat.id
        ):
            reports = self.unhandled_reports.pop(update.effective_chat.id, [])
            for report in reports:
                assert report.text is not None

                try:
                    split = report.text_html.split("\n", 1)
                    new_text = f"{split[0]}\n\n✅ Admin {mention_html(update.effective_user)} has likely responded to this\n{split[1]}"
                    await report.edit_text(
                        new_text,
                        parse_mode="HTML",
                    )
                except Exception as e:
                    log.error(f"Failed to edit unhandled report message: {e}")

        # Check if @admin has been called
        admins_called: bool = False

        for entity in update.message.entities:
            if entity.type == "mention" and update.message.text:
                mentioned_username = update.message.text[
                    entity.offset : entity.offset + entity.length
                ]
                if mentioned_username.lower() in ("@admin", "@admins"):
                    # We have an admin mention, log it
                    admins_called = True
                    break

        if not admins_called:
            return

        # Detect if this report comes from a group admin
        if self.db.is_admin_of_group(
            update.effective_user.id, update.effective_chat.id
        ):
            # Ignore reports from group admins
            await update.message.reply_html(
                "<i>Reported to 0 admins (just a simulation)</i>\n\nYou can use @admin anywhere in the message to call for an admin, or use the regular /admin command. You can also use it in a reply to the actual message you wish to report."
            )
            return

        if update.message.text and len(update.message.text) < 8:
            args = None
        else:
            args = update.message.text

        await self.report(update, context, args)

    ################
    ### Commands ###
    ################

    async def cmd_admin(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, args: str
    ):
        await self.report(update, context, args)
