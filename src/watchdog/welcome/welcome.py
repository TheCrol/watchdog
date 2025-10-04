import logging
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

from telegram import MaybeInaccessibleMessage, Message, Update
from telegram.ext import CallbackContext, ContextTypes

from ..bot import BUTTON_HANDLER, ChatDataRegister
from ..botadmin import AppConfig, AppEnabledConfig, TextConfig
from ..db import Group
from ..useful import ACCESS, mention_html

if TYPE_CHECKING:
    from ..watchdog import App

DEFAULT_MESSAGE = "Welcome to the group, {name}!"

log = logging.getLogger("welcome")


@dataclass
class Config:
    enabled: bool
    message: str = DEFAULT_MESSAGE


class Welcome:
    def __init__(self, app: "App"):
        self.app = app
        self.bot = app.bot
        self.db = app.db

        self.configs: dict[int, Config] = {}
        self.registers: dict[int, ChatDataRegister] = {}

    async def start(self):
        self.configs = await self.db.get_app_configs("welcome", Config)

        for group_id, config in self.configs.items():
            if not config.enabled:
                break
            self.add_group_registers(group_id)

        self.app.botadmin.register_config(
            AppConfig(
                button_emoji="👋",
                name="Welcome message",
                description="Sends a welcome message to new users when they join the group",
                display_order=30,
                configs=[
                    AppEnabledConfig(
                        get_callback=self.botadmin_get_enabled,
                        set_callback=self.botadmin_set_enabled,
                    ),
                    TextConfig(
                        title="Welcome Message",
                        description=(
                            "The message to send when a new user joins. "
                            "There are special placeholders available:\n\n"
                            "<i>{name}</i> - The user's full name\n"
                            "<i>{mention}</i> - A clickable user name of the user that's tied to their user id\n"
                            "<i>{user_id}</i> - The user's Telegram user ID"
                        ),
                        get_callback=self.botadmin_get_message,
                        set_callback=self.botadmin_set_message,
                    ),
                ],
            )
        )

        self.app.bot.register_command(
            "welcome",
            "See or update the welcome message",
            self.cmd_welcome,
            ACCESS.ALL_ADMINS_DM,
        )

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
        config = self.configs.get(group_id)
        if not config or not config.enabled:
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

    def botadmin_get_enabled(self, group_id: int) -> bool:
        if config := self.configs.get(group_id):
            return config.enabled
        return False

    async def botadmin_set_enabled(self, group_id: int, value: bool) -> None:
        if config := self.configs.get(group_id):
            config.enabled = value
        else:
            self.configs[group_id] = Config(enabled=value)

        await self.db.set_app_config("welcome", group_id, self.configs[group_id])

        if value:
            self.add_group_registers(group_id)
        else:
            self.remove_group_registers(group_id)

    def botadmin_get_message(self, group_id: int) -> str:
        if config := self.configs.get(group_id):
            return config.message
        return "Welcome to the group, {name}!"

    async def botadmin_set_message(self, group_id: int, value: str) -> None:
        if config := self.configs.get(group_id):
            config.message = value
        else:
            self.configs[group_id] = Config(enabled=False, message=value)

        await self.db.set_app_config("welcome", group_id, self.configs[group_id])

    def is_available_for_admin(self, user_id: int, group_id: int) -> bool:
        """Confirm that this group is available for this admin user to manage"""

        # Does this group exists
        if self.db.groups.get(group_id) is None:
            return False

        # Is this user an admin of this group
        if not self.db.is_admin_of_group(user_id, group_id):
            return False

        # Is this group enabled for welcome messages
        if group_id not in self.configs or not self.configs[group_id].enabled:
            return False

        return True

    async def show_group(
        self, user_id: int, group_id: int, message: Message | MaybeInaccessibleMessage
    ):
        # Confirm this group still exists and we are admin
        if not self.is_available_for_admin(user_id, group_id):
            await self.bot.bot.send_message(
                user_id,
                "⚠️ You have no longer access to this function (either you are no longer admin, or the group has disabled welcome messages)",
            )
            return

        group = self.db.groups.get(group_id)
        assert group is not None

        text = (
            f"The welcome message for <b>{group.title}</b>\n\n"
            f"There are special placeholders available:\n\n"
            "<i>{name}</i> - The user's full name\n"
            "<i>{mention}</i> - A clickable user name of the user that's tied to their user id\n"
            "<i>{user_id}</i> - The user's Telegram user ID\n\n"
            "This is the current welcome message:\n"
            "-----\n\n"
            f"{self.configs[group_id].message}"
        )

        buttons: list[list[tuple[str, BUTTON_HANDLER]]] = [
            [("✏️ Edit", partial(self.btn_edit_message, user_id, group.id))]
        ]

        await self.bot.send_or_replace_message_buttons(message, text, buttons)

    ################
    ### Commands ###
    ################

    async def cmd_test(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, args: str
    ):
        log.info("Message received")

    async def cmd_welcome(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, args: str
    ):
        if not update.effective_user or update.message is None:
            return

        # Get a list of groups this user is admin of, and filter it by those
        # that have welcome enabled
        groups: list[Group] = []
        for group in self.db.get_groups_from_admin(update.effective_user.id):
            if group.id in self.configs and self.configs[group.id].enabled:
                groups.append(group)

        if not groups:
            await update.message.reply_text(
                "⚠️ You are not an admin of any group with welcome messages currently enabled"
            )
            return

        # If there is only one group, show it directly
        if len(groups) == 1:
            await self.show_group(
                update.effective_user.id, groups[0].id, update.message
            )
            return

        # Show a list of groups to choose from
        buttons: list[list[tuple[str, BUTTON_HANDLER]]] = []
        for group in groups:
            buttons.append(
                [
                    (
                        group.title,
                        partial(
                            self.btn_show_group, update.effective_user.id, group.id
                        ),
                    )
                ]
            )

        text = "Select a group you are admin of to see or edit the welcome message:"
        await self.bot.send_or_replace_message_buttons(update.message, text, buttons)

    ###############
    ### Buttons ###
    ###############

    async def btn_show_group(
        self,
        user_id: int,
        group_id: int,
        update: Update,
        context: CallbackContext,
    ):
        assert (
            update.callback_query is not None
            and update.callback_query.message is not None
        )

        await self.show_group(user_id, group_id, update.callback_query.message)

    async def btn_edit_message(
        self, user_id: int, group_id: int, update: Update, context: CallbackContext
    ):
        assert (
            update.callback_query is not None
            and update.callback_query.message is not None
        )

        # Confirm this group still exists and we are admin
        if not self.is_available_for_admin(user_id, group_id):
            await self.bot.bot.send_message(
                user_id,
                "⚠️ You have no longer access to this function (either you are no longer admin, or the group has disabled welcome messages)",
            )
            return

        await update.callback_query.answer()

        group = self.db.groups.get(group_id)
        if group is None:
            return

        await self.bot.send_message_get_reply(
            update.callback_query.message,
            context,
            text=f"Send me the new welcome message for <i>{group.title}</i>. You can cancel this operation by sending /cancel.",
            callback=partial(self.reply_new_message, user_id, group_id),
        )

    ###############
    ### Replies ###
    ###############

    async def reply_new_message(
        self,
        user_id: int,
        group_id: int,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        text: str,
    ):
        if (message := update.effective_message) is None:
            return

        if text == "/cancel":
            await message.reply_text("❌ Operation cancelled")
            return

        # Confirm this group still exists and we are admin
        if not self.is_available_for_admin(user_id, group_id):
            await self.bot.bot.send_message(
                user_id,
                "⚠️ You have no longer access to this function (either you are no longer admin, or the group has disabled welcome messages)",
            )
            return

        # Update the message in the database
        config = self.configs.get(group_id)
        if config is None or not config.enabled:
            await message.reply_text(
                "⚠️ This group has disabled welcome messages, so you cannot set a welcome message"
            )
            return

        config.message = text

        await self.db.set_app_config("welcome", group_id, self.configs[group_id])

        await message.reply_text("✅ The welcome message has been updated!")
