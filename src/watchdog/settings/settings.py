import itertools
import logging
from functools import partial
from typing import TYPE_CHECKING

from telegram import MaybeInaccessibleMessage, Message, Update
from telegram.ext import CallbackContext, ContextTypes

from ..bot.bot import BUTTON_HANDLER
from ..useful import ACCESS
from .app_config import (Answer, AppConfig, BackToMenuAnswer, ButtonsAnswer,
                         ExecAnswer, GroupInfo, InputAnswer, OutputAnswer,
                         SingleConfig, UserInfo)

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
            "settings", "Manage the settings", self.cmd_settings, ACCESS.ALL_ADMINS_DM
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
        if user_id in self.app.bot_admins:
            return True  # Bot admins have access to everything

        # Decide the access level
        access_level = ACCESS.EVERYONE
        if group is not None and self.db.is_admin_of_group(user_id, group.id):
            access_level = ACCESS.GROUP_ADMINS
        elif self.db.is_admin(user_id):
            access_level = ACCESS.ALL_ADMINS

        if setting is not None:
            return self.compare_access_level(access_level, setting.access)

        else:
            # No specific setting, check if the app has any settings the user can access
            for config in app_config.configs:
                if self.compare_access_level(access_level, config.access):
                    return True  # User has access to at least one setting
            return False  # User doesn't have access to any settings in this app

    def compare_access_level(self, subject: ACCESS, required: ACCESS) -> bool:
        """
        Check if a subject access level meets or exceeds the required access
        level. For ease of use an _DM suffixes are treated the same as their
        non-DM counterparts.
        """
        if subject == ACCESS.EVERYONE_DM:
            subject = ACCESS.EVERYONE
        elif subject == ACCESS.ALL_ADMINS_DM:
            subject = ACCESS.ALL_ADMINS
        elif subject == ACCESS.BOT_ADMIN_DM:
            subject = ACCESS.BOT_ADMIN

        if required == ACCESS.EVERYONE_DM:
            required = ACCESS.EVERYONE
        elif required == ACCESS.ALL_ADMINS_DM:
            required = ACCESS.ALL_ADMINS
        elif required == ACCESS.BOT_ADMIN_DM:
            required = ACCESS.BOT_ADMIN

        if subject == ACCESS.BOT_ADMIN:
            return True  # Bot admins have access to everything
        elif subject == ACCESS.ALL_ADMINS:
            return required in (ACCESS.EVERYONE, ACCESS.ALL_ADMINS)
        elif subject == ACCESS.GROUP_ADMINS:
            return required in (ACCESS.EVERYONE, ACCESS.ALL_ADMINS, ACCESS.GROUP_ADMINS)
        elif subject == ACCESS.EVERYONE:
            return required == ACCESS.EVERYONE
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
            await update.callback_query.answer(
                "You don't have permission to access this setting.",
                show_alert=True,
            )
            return

        user_info = UserInfo(user.id, user.full_name)

        if not setting.requires_group:
            answer = await setting.on_button(user_info)
            await self.show_answer(message, app_config, setting, answer, user_info)

        else:
            # We maybe need to show a group selector first
            if setting.access in (ACCESS.EVERYONE, ACCESS.EVERYONE_DM):
                # Everyone has access, so we ask for any of our groups
                groups = list(self.db.groups.values())
            else:
                # Only admins have access, so we only ask for groups where the user is an admin
                groups = self.db.get_groups_from_admin(user.id)

            if not groups:
                await update.callback_query.answer(
                    "You are not an admin in any group.",
                    show_alert=True,
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
            await update.callback_query.answer(
                "You don't have permission to access this setting.",
                show_alert=True,
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
            await update.callback_query.answer(
                "You don't have permission to access this setting.",
                show_alert=True,
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
