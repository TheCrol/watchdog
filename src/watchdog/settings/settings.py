import itertools
import logging
from functools import partial
from typing import TYPE_CHECKING

from telegram import MaybeInaccessibleMessage, Message, Update
from telegram.ext import CallbackContext, ContextTypes

from ..bot.bot import BUTTON_HANDLER
from ..useful import AccessRequired
from .app_config import (
    Answer,
    AppConfig,
    BackToMenuAnswer,
    ButtonsAnswer,
    ExecAnswer,
    GroupInfo,
    GroupSelect,
    InputAnswer,
    OutputAnswer,
    SingleConfig,
    UserInfo,
)

if TYPE_CHECKING:
    from ..watchdog import App

log = logging.getLogger("settings")


class Settings:
    def __init__(self, app: "App"):
        self.app = app
        self.bot = app.bot
        self.db = app.db

        self.configs: list[AppConfig] = []

    async def start(self):
        self.bot.register_command(
            "settings",
            "Manage the settings",
            self.cmd_settings,
            AccessRequired(all_admins=True),
        )

    def register_config(self, app_config: AppConfig):
        self.configs.append(app_config)
        self.configs.sort(key=lambda c: c.display_order)

    def has_access(
        self,
        user_id: int,
        app_config: AppConfig,
        setting: SingleConfig | None,
        group: GroupInfo | None,
    ) -> bool:
        """Check if a user has access to an app's settings"""
        is_bot_admin = user_id in self.app.bot_admins
        admin_of_groups = [g.id for g in self.db.get_groups_from_admin(user_id)]

        if setting is not None:
            if not setting.access.has_access(is_bot_admin, admin_of_groups, None):
                return False

            # Check if the setting requires a group
            if setting.group_select != GroupSelect.NO_GROUP_SELECT:
                if group is None:
                    return True
                return self.has_group_access(user_id, setting, group)
            return True  # User has access to this specific setting

        else:
            # No specific setting, check if the app has any settings the user can access
            for config in app_config.configs:
                if config.access.has_access(is_bot_admin, admin_of_groups, None):
                    return True  # User has access to at least one setting
            return False  # User doesn't have access to any settings in this app

    def has_group_access(
        self,
        user_id: int,
        setting: SingleConfig,
        group: GroupInfo,
    ) -> bool:
        """Check if a user has access to a specific group's settings"""
        if setting.group_select == GroupSelect.ALL_GROUPS:
            # List all groups we know of
            groups = list(self.db.groups.values())
        elif setting.group_select == GroupSelect.ADMINED_GROUPS:
            # Only admins have access, so we only ask for groups where the user is an admin
            groups = self.db.get_groups_from_admin(user_id)
        else:
            return False

        for search_group in groups:
            if search_group.id == group.id:
                return True
        return False

    def get_message_header(
        self,
        app_config: AppConfig | None = None,
        setting: SingleConfig | None = None,
        user: UserInfo | None = None,
    ) -> str:
        if app_config is None:
            return "⚙️ <b>Settings:</b>\n\n"
        elif setting is None:
            return f"⚙️ <b>Settings → {app_config.name}:</b>\n\n"
        elif user and user.group:
            return f"⚙️ <b>Settings → {app_config.name} → {setting.static_name} ({user.group.name}):</b>\n\n"
        else:
            return f"⚙️ <b>Settings → {app_config.name} → {setting.static_name}:</b>\n\n"

    async def show_settings(self, message: Message | MaybeInaccessibleMessage):
        buttons: list[list[tuple[str, BUTTON_HANDLER]]] = []

        for configs in itertools.batched(self.configs, 2):
            row: list[tuple[str, BUTTON_HANDLER]] = []
            for config in configs:
                row.append(
                    (
                        f"{config.button_emoji} {config.name}",
                        partial(self.btn_app, config),
                    )
                )
            buttons.append(row)

        text = self.get_message_header()

        text += "Select an application to manage its settings."

        await self.bot.send_or_replace_message_buttons(message, text, buttons)

    async def show_app_settings(
        self,
        message: Message | MaybeInaccessibleMessage,
        app_config: AppConfig,
        user_info: UserInfo,
        new_message: bool = False,
    ):

        buttons: list[list[tuple[str, BUTTON_HANDLER]]] = []
        configs = [
            config
            for config in app_config.configs
            if self.has_access(user_info.id, app_config, config, None)
        ]
        for configs in itertools.batched(configs + ["back"], 2):
            row: list[tuple[str, BUTTON_HANDLER]] = []
            for config in configs:
                if isinstance(config, str):
                    if config == "back":
                        row.append(("🔙 Back", self.btn_start))
                else:
                    row.append(
                        (
                            await config.get_button(user_info),
                            partial(self.btn_app_setting, app_config, config),
                        )
                    )
            buttons.append(row)

        text = self.get_message_header(app_config)

        text += f"{app_config.description}"

        await self.bot.send_or_replace_message_buttons(
            message, text, buttons, new_message
        )

    async def show_answer(
        self,
        message: Message | MaybeInaccessibleMessage,
        app_config: AppConfig,
        setting: SingleConfig,
        answer: Answer,
        user_info: UserInfo,
        new_message: bool = False,
    ):
        text = self.get_message_header(app_config, setting, user_info)

        if isinstance(answer, OutputAnswer):
            text += answer.message
            await self.bot.bot.send_message(message.chat.id, text, parse_mode="HTML")

            if answer.next_answer is not None:
                await self.show_answer(
                    message, app_config, setting, answer.next_answer, user_info, True
                )

        elif isinstance(answer, BackToMenuAnswer):
            user_info.group = None
            await self.show_app_settings(message, app_config, user_info, new_message)

        elif isinstance(answer, InputAnswer):
            text += answer.text
            await self.bot.send_message_get_reply(
                message,
                text,
                partial(self.bot_reply, app_config, setting, answer, user_info),
            )

        elif isinstance(answer, ButtonsAnswer):
            text += answer.text
            buttons: list[list[tuple[str, BUTTON_HANDLER]]] = []

            if answer.big_buttons:
                # Each button in its own row
                for btn, ans in answer.buttons:
                    buttons.append(
                        [
                            (
                                btn,
                                partial(
                                    self.btn_app_setting_exec,
                                    app_config,
                                    setting,
                                    ans,
                                    user_info,
                                ),
                            )
                        ]
                    )

            else:
                # Two buttons per row
                for configs in itertools.batched(answer.buttons, 2):
                    row: list[tuple[str, BUTTON_HANDLER]] = []
                    for btn, ans in configs:
                        row.append(
                            (
                                btn,
                                partial(
                                    self.btn_app_setting_exec,
                                    app_config,
                                    setting,
                                    ans,
                                    user_info,
                                ),
                            )
                        )
                    buttons.append(row)

            await self.bot.send_or_replace_message_buttons(
                message, text, buttons, new_message
            )

        elif isinstance(answer, ExecAnswer):
            answer = await answer.callback(user_info)
            await self.show_answer(message, app_config, setting, answer, user_info)

    async def cmd_settings(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, arg: str
    ):
        if (message := update.effective_message) is None:
            return
        await self.show_settings(message)

    async def btn_start(self, update: Update, context: CallbackContext):
        if (
            update.callback_query is None
            or (message := update.callback_query.message) is None
        ):
            return
        await self.show_settings(message)

    async def btn_app(
        self, app_config: AppConfig, update: Update, context: CallbackContext
    ):
        if (
            update.callback_query is None
            or (message := update.callback_query.message) is None
            or (user := update.effective_user) is None
        ):
            return

        user_info = UserInfo(user.id, user.full_name)

        await self.show_app_settings(message, app_config, user_info)

    async def btn_app_setting(
        self,
        app_config: AppConfig,
        setting: SingleConfig,
        update: Update,
        context: CallbackContext,
    ):
        if (
            update.callback_query is None
            or (message := update.callback_query.message) is None
            or (user := update.effective_user) is None
        ):
            return

        await update.callback_query.answer()

        if not self.has_access(user.id, app_config, setting, None):
            await self.bot.bot.send_message(
                chat_id=user.id,
                text="You don't have permission to access this setting.",
            )
            return

        user_info = UserInfo(user.id, user.full_name)

        if not setting.group_select != GroupSelect.NO_GROUP_SELECT:
            answer = await setting.on_button(user_info)
            await self.show_answer(message, app_config, setting, answer, user_info)

        else:
            # We maybe need to show a group selector first
            if setting.group_select == GroupSelect.ALL_GROUPS:
                # List all groups we know of
                groups = list(self.db.groups.values())
            elif setting.group_select == GroupSelect.ADMINED_GROUPS:
                # Only admins have access, so we only ask for groups where the user is an admin
                groups = self.db.get_groups_from_admin(user.id)
            else:
                return

            if not groups:
                await self.bot.bot.send_message(
                    chat_id=user.id,
                    text="You don't have access to any group.",
                )
                return
            elif len(groups) == 1:
                # Default to the only group
                user_info.group = GroupInfo(groups[0].id, groups[0].title)
                answer = await setting.on_button(user_info)
                await self.show_answer(message, app_config, setting, answer, user_info)

            else:
                buttons: list[list[tuple[str, BUTTON_HANDLER]]] = []
                for group in groups:
                    buttons.append(
                        [
                            (
                                group.title,
                                partial(
                                    self.btn_app_setting_group,
                                    app_config,
                                    setting,
                                    GroupInfo(group.id, group.title),
                                ),
                            )
                        ]
                    )
                buttons.append([("🔙 Back", partial(self.btn_app, app_config))])

                text = self.get_message_header(app_config, setting, user_info)
                text += "Select a group to manage this setting in."

                await self.bot.send_or_replace_message_buttons(message, text, buttons)

    async def btn_app_setting_group(
        self,
        app_config: AppConfig,
        setting: SingleConfig,
        group: GroupInfo,
        update: Update,
        context: CallbackContext,
    ):
        if (
            update.callback_query is None
            or (message := update.callback_query.message) is None
            or (user := update.effective_user) is None
        ):
            return

        await update.callback_query.answer()

        if not self.has_access(user.id, app_config, setting, group):
            await self.bot.bot.send_message(
                chat_id=user.id,
                text="You don't have permission to access this setting.",
            )
            return

        user_info = UserInfo(user.id, user.full_name, group)

        answer = await setting.on_button(user_info)
        await self.show_answer(message, app_config, setting, answer, user_info)

    async def btn_app_setting_exec(
        self,
        app_config: AppConfig,
        setting: SingleConfig,
        answer: Answer,
        user_info: UserInfo,
        update: Update,
        context: CallbackContext,
    ):
        if (
            update.callback_query is None
            or (message := update.callback_query.message) is None
            or (user := update.effective_user) is None
        ):
            return

        await update.callback_query.answer()

        if not self.has_access(user.id, app_config, setting, user_info.group):
            await self.bot.bot.send_message(
                chat_id=user.id,
                text="You don't have permission to access this setting.",
            )
            return

        await self.show_answer(message, app_config, setting, answer, user_info)

    async def bot_reply(
        self,
        app_config: AppConfig,
        setting: SingleConfig,
        answer: InputAnswer,
        user_info: UserInfo,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        value: str,
    ):
        if (user := update.effective_user) is None or (
            message := update.effective_message
        ) is None:
            return

        if not self.has_access(user.id, app_config, setting, user_info.group):
            await message.reply_html(
                "You don't have permission to access this setting."
            )
            return

        next_answer = await answer.callback(user_info, value)
        await self.show_answer(
            message, app_config, setting, next_answer, user_info, True
        )
