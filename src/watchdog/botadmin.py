import asyncio
import itertools
import logging
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from telegram import MaybeInaccessibleMessage, Message, Update
from telegram.ext import CallbackContext, ContextTypes

from .bot import BUTTON_HANDLER
from .useful import ACCESS, pluralize

if TYPE_CHECKING:
    from .watchdog import App

log = logging.getLogger(__name__)


@dataclass(kw_only=True)
class SingleConfig:
    get_callback: Callable[[int], Any | Awaitable[Any]]
    set_callback: Callable[[int, Any], None | Awaitable[None]]

    async def get_value(self, group_id: int) -> Any:
        func = self.get_callback(group_id)
        return await func if isinstance(func, Awaitable) else func

    async def set_value(self, group_id: int, value: Any) -> None:
        func = self.set_callback(group_id, value)
        if isinstance(func, Awaitable):
            await func


@dataclass(kw_only=True)
class AppEnabledConfig(SingleConfig): ...


@dataclass(kw_only=True)
class TextConfig(SingleConfig):
    title: str
    description: str


@dataclass(kw_only=True)
class NumberConfig(SingleConfig):
    title: str
    description: str
    min: int | None = None
    max: int | None = None
    rounded: bool = False


@dataclass(kw_only=True)
class AppConfig:
    button_emoji: str
    name: str
    description: str
    display_order: int
    configs: list[SingleConfig]


class BotAdmin:
    def __init__(self, app: "App"):
        self.app = app
        self.bot = app.bot
        self.db = app.db
        self.bot.register_command(
            "group",
            "Configurate a group the bot is in",
            self.cmd_group,
            access=ACCESS.BOT_ADMIN_DM,
        )

        self.app_configs: list[AppConfig] = []

    def notify_sync(self, text: str):
        """Sync version of notify"""
        asyncio.create_task(self.notify(text))

    async def notify(self, text: str):
        """Notify all bot admins with a message"""
        message = f"❗ <b>Watchdog notification</b> ❗\n\n{text}"
        for admin_id in self.app.bot_admins:
            await self.bot.bot.send_message(admin_id, message, parse_mode="HTML")

    async def show_groups(self, message: Message | MaybeInaccessibleMessage):
        groups = self.app.db.groups

        if len(groups) == 0:
            await self.bot.bot.send_message(
                message.chat.id, "I am not in any groups right now"
            )
            return

        buttons = []
        for group in groups.values():
            buttons.append([(group.title, partial(self.btn_select_group, group.id))])

        await self.bot.send_or_replace_message_buttons(
            message,
            "👥 Please select a group to manage:",
            buttons,
        )

    async def show_group(
        self, group_id: int, message: Message | MaybeInaccessibleMessage
    ):
        if (group := self.app.db.groups.get(group_id)) is None:
            return

        messages_count = await self.db.count_messages(group.id)

        admin_list: list[str] = []
        for group_admin in self.db.get_group_admins(group.id):
            admin_list.append(f"- {group_admin.mention}")
        admins = "\n".join(admin_list)

        text = (
            f"<b>{group.title}</b>\n\n"
            f"👥 {pluralize(self.db.count_participants(group.id), 'person', 'people')} (that I'm aware of)\n\n"
            f"💬 {pluralize(messages_count, 'message', 'messages')} logged in the past 6 months\n\n"
            f"🛡️ {pluralize(len(admin_list), 'admin', 'admins')}:\n{admins}"
        )

        # Create a list of buttons based on the app configs we have

        def create_button(app_config: AppConfig | str) -> tuple[str, BUTTON_HANDLER]:
            if isinstance(app_config, str):
                if app_config == "leave":
                    return ("❌ Leave group", self.btn_dummy)
                elif app_config == "back":
                    return ("🔙 Back", self.btn_show_groups)
                raise ValueError("Unknown string button")
            else:
                return (
                    f"{app_config.button_emoji} {app_config.name}",
                    partial(self.btn_select_app, group.id, app_config),
                )

        buttons: list[list[tuple[str, BUTTON_HANDLER]]] = []
        for configs in itertools.batched(self.app_configs + ["leave", "back"], 2):
            row = []
            for config in configs:
                button = create_button(config)
                row.append(button)
            buttons.append(row)

        await self.bot.send_or_replace_message_buttons(message, text, buttons)

    async def show_app(
        self,
        group_id: int,
        app_config: AppConfig,
        message: Message | MaybeInaccessibleMessage,
    ):
        group = self.app.db.groups.get(group_id)
        if group is None:
            return
        text = f"<b>{app_config.name}</b>\n<i>For {group.title}</i>\n\n{app_config.description}"
        buttons: list[list[tuple[str, BUTTON_HANDLER]]] = []
        for configs in itertools.batched(app_config.configs + ["back"], 2):
            row: list[tuple[str, BUTTON_HANDLER]] = []

            for config in configs:
                if config == "back":
                    row.append(("🔙 Back", partial(self.btn_select_group, group_id)))

                elif isinstance(config, AppEnabledConfig):
                    value = await config.get_value(group_id)
                    button_text = "✅ Enabled" if value else "❌ Disabled"
                    row.append(
                        (
                            button_text,
                            partial(self.btn_app_config, group_id, app_config, config),
                        )
                    )

                elif isinstance(config, TextConfig) or isinstance(config, NumberConfig):
                    row.append(
                        (
                            config.title,
                            partial(self.btn_app_config, group_id, app_config, config),
                        )
                    )

            buttons.append(row)

        await self.bot.send_or_replace_message_buttons(message, text, buttons)

    def register_config(self, app_config: AppConfig):
        self.app_configs.append(app_config)

        # Sort the app configs by display order
        self.app_configs.sort(key=lambda ac: ac.display_order)

    ################
    ### Commands ###
    ################

    async def cmd_group(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, args: str
    ):
        assert update.message is not None

        await self.show_groups(update.message)

    ###############
    ### Buttons ###
    ################

    async def btn_select_group(
        self, group_id: int, update: Update, context: CallbackContext
    ):
        """Group has been selected"""

        assert (
            update.callback_query is not None
            and update.callback_query.message is not None
        )
        await update.callback_query.answer()
        await self.show_group(group_id, update.callback_query.message)

    async def btn_show_groups(self, update: Update, context: CallbackContext):
        """Go back to the full group list"""

        assert (
            update.callback_query is not None
            and update.callback_query.message is not None
        )
        await update.callback_query.answer()
        await self.show_groups(update.callback_query.message)

    async def btn_select_app(
        self,
        group_id: int,
        app_config: AppConfig,
        update: Update,
        context: CallbackContext,
    ):
        """Show a specific app config for a specific group"""
        assert (
            update.callback_query is not None
            and update.callback_query.message is not None
        )
        await self.show_app(group_id, app_config, update.callback_query.message)

    async def btn_app_config(
        self,
        group_id: int,
        app_config: AppConfig,
        single_config: SingleConfig,
        update: Update,
        context: CallbackContext,
    ):
        """A config button has been selected. Either toggle it or go to the config"""

        assert (
            update.callback_query is not None
            and update.callback_query.message is not None
        )

        value = await single_config.get_value(group_id)

        # The enable config. We toggle it
        if isinstance(single_config, AppEnabledConfig):
            assert isinstance(value, bool)
            await single_config.set_value(group_id, not value)

            await self.show_app(
                group_id, app_config=app_config, message=update.callback_query.message
            )

        # The text config. Show a message and add an option to edit it
        elif isinstance(single_config, TextConfig):
            prompt = f"{single_config.description}.\n\n" f"Current value:\n{value}"
            await self.bot.send_or_replace_message_buttons(
                update.callback_query.message,
                prompt,
                [
                    [
                        (
                            "✏️ Edit",
                            partial(
                                self.btn_edit_text_config,
                                app_config,
                                single_config,
                                group_id,
                            ),
                        ),
                        ("🔙 Back", partial(self.btn_select_app, group_id, app_config)),
                    ],
                ],
            )

        # A number config. Show a message and add an option to edit it
        elif isinstance(single_config, NumberConfig):
            prompt = f"{single_config.description}.\n\n" f"Current value:\n{value}"
            await self.bot.send_or_replace_message_buttons(
                update.callback_query.message,
                prompt,
                [
                    [
                        (
                            "✏️ Edit",
                            partial(
                                self.btn_edit_number_config,
                                app_config,
                                single_config,
                                group_id,
                            ),
                        ),
                        ("🔙 Back", partial(self.btn_select_app, group_id, app_config)),
                    ],
                ],
            )

    async def btn_edit_text_config(
        self,
        app_config: AppConfig,
        text_config: TextConfig,
        group_id: int,
        update: Update,
        context: CallbackContext,
    ):
        """Start editing a text config by replying to the message"""

        assert (
            update.callback_query is not None
            and update.callback_query.message is not None
        )

        await update.callback_query.answer()

        await self.bot.send_message_get_reply(
            update.callback_query.message,
            context,
            text="Send me the new text value. You can cancel this operation by sending /cancel.",
            callback=partial(self.reply_text_config, group_id, app_config, text_config),
        )

    async def btn_edit_number_config(
        self,
        app_config: AppConfig,
        number_config: NumberConfig,
        group_id: int,
        update: Update,
        context: CallbackContext,
    ):
        """Start editing a number config by replying to the message"""

        assert (
            update.callback_query is not None
            and update.callback_query.message is not None
        )

        await update.callback_query.answer()

        options: list[str] = []
        if number_config.min is not None:
            options.append(f"Min {number_config.min}")
        if number_config.max is not None:
            options.append(f"Max {number_config.max}")
        if number_config.rounded:
            options.append("Only rounded values")

        prompt_options = ""
        if options:
            prompt_options = "\n\n" + ". ".join(options)

        await self.bot.send_message_get_reply(
            update.callback_query.message,
            context,
            text=f"Send me the new numer value. You can cancel this operation by sending /cancel.{prompt_options}",
            callback=partial(
                self.reply_number_config, group_id, app_config, number_config
            ),
        )

    async def btn_dummy(self, update: Update, context: CallbackContext):
        log.debug("Dummy button pressed")

    async def reply_text_config(
        self,
        group_id: int,
        app_config: AppConfig,
        text_config: TextConfig,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        text: str,
    ):
        assert update.message is not None

        if text == "/cancel":
            await update.message.reply_text("Operation cancelled")
        else:
            await text_config.set_value(group_id, text)
            await update.message.reply_text("The value has been updated!")

        await self.show_app(group_id, app_config, update.message)

    async def reply_number_config(
        self,
        group_id: int,
        app_config: AppConfig,
        number_config: NumberConfig,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        text: str,
    ):
        assert update.message is not None

        if text == "/cancel":
            await update.message.reply_text("Operation cancelled")
        else:
            # Validate the number
            value: float | int
            if number_config.rounded:  # Integer
                try:
                    value = int(text)
                except ValueError:
                    await update.message.reply_text(
                        "Did not save the value as it was not valid (not an integer)"
                    )
                    await self.show_app(group_id, app_config, update.message)
                    return
            else:  # Float
                try:
                    value = float(text)
                except ValueError:
                    await update.message.reply_text(
                        "Did not save the value as it was not valid (not a float)"
                    )
                    await self.show_app(group_id, app_config, update.message)
                    return

            # Min number
            if number_config.min is not None and value < number_config.min:
                await update.message.reply_text(
                    f"Did not save the value as it was below the minimum of {number_config.min}"
                )
                await self.show_app(group_id, app_config, update.message)
                return

            # Max number
            if number_config.max is not None and value > number_config.max:
                await update.message.reply_text(
                    f"Did not save the value as it was above the maximum of {number_config.max}"
                )
                await self.show_app(group_id, app_config, update.message)
                return

            await number_config.set_value(group_id, value)
            await update.message.reply_text("The value has been updated!")

        await self.show_app(group_id, app_config, update.message)
