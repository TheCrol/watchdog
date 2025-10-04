import asyncio
import logging
import re
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable

from telegram import Bot as TGbot
from telegram import (
    ChatAdministratorRights,
    ChatMember,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MaybeInaccessibleMessage,
    Message,
    Update,
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    ApplicationHandlerStop,
    CallbackContext,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from ..useful import ACCESS, get_chat_name
from .command_updater import CommandUpdater

if TYPE_CHECKING:
    from ..watchdog import App

log = logging.getLogger("bot")

COMMAND_HANDLER = Callable[[Update, ContextTypes.DEFAULT_TYPE, str], Awaitable[None]]
BUTTON_HANDLER = Callable[[Update, CallbackContext], Awaitable[None]]
REPLY_HANDLER = Callable[[Update, ContextTypes.DEFAULT_TYPE, str], Awaitable[None]]
MESSAGE_HANDLER = Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]


@dataclass
class Command:
    description: str
    handler: COMMAND_HANDLER
    access: ACCESS
    group_id: int | None = None


@dataclass
class Buttons:
    when: float
    handlers: dict[str, BUTTON_HANDLER]


@dataclass
class ChatData:
    handler: MESSAGE_HANDLER
    group_id: int


class CommandRegister:
    def __init__(self, bot: "Bot", command: str, command_class: Command):
        self.bot = bot
        self.command = command
        self.command_class = command_class

    def deregister_command(self):
        self.bot.deregister_command(self.command, self.command_class)


class ChatDataRegister:
    def __init__(self, bot: "Bot", group_id: int, chat_data_class: ChatData):
        self.bot = bot
        self.group_id = group_id
        self.chat_data_class = chat_data_class

    def deregister_chat_data(self):
        self.bot.deregister_chat_data(self.group_id, self.chat_data_class)


class Bot:
    def __init__(self, app: "App"):
        self.app = app

        self.commands: dict[str, list[Command]] = {}
        self.chat_data: dict[int, list[ChatData]] = {}
        self.button_callbacks: dict[int, Buttons] = {}  # TODO: Cleanup old ones

        self.command_updater = CommandUpdater(self.app, self)

    async def start(self):
        self.telegram: Application = (
            ApplicationBuilder()
            .token(self.app.bot_token)
            .get_updates_connect_timeout(20)
            .get_updates_read_timeout(20)
            .build()
        )
        self.bot: TGbot = self.telegram.bot

        # Set the default administrator rights
        await self.bot.set_my_default_administrator_rights(
            ChatAdministratorRights(
                is_anonymous=False,
                can_manage_chat=True,
                can_delete_messages=True,
                can_manage_video_chats=False,
                can_restrict_members=False,
                can_promote_members=False,
                can_change_info=False,
                can_invite_users=False,
                can_delete_stories=False,
                can_post_stories=False,
                can_edit_stories=False,
            )
        )

        # Reply handlers
        self.telegram.add_handler(
            MessageHandler(
                filters.TEXT,
                self.bot_reply,
            ),
            group=0,
        )

        # Chat member events
        self.telegram.add_handler(
            ChatMemberHandler(self.bot_member_update, ChatMemberHandler.MY_CHAT_MEMBER),
            group=10,
        )
        self.telegram.add_handler(
            ChatMemberHandler(
                self.chat_member_update, ChatMemberHandler.ANY_CHAT_MEMBER
            ),
            group=10,
        )

        # Our activity tracker
        self.telegram.add_handler(
            MessageHandler(
                filters.ALL,
                self.activity_tracker,
            ),
            group=50,
        )

        # Callback query
        self.telegram.add_handler(
            CallbackQueryHandler(self.recv_callback_query), group=10
        )

        await self.telegram.initialize()
        assert self.telegram.updater is not None
        await self.telegram.updater.start_polling(allowed_updates=Update.ALL_TYPES)

        # The command updater task
        self.command_updater_task: None | asyncio.Task = None

        asyncio.create_task(self.telegram.start())

    async def stop(self):
        with suppress(RuntimeError):
            if self.telegram.updater is not None:
                await self.telegram.updater.stop()
            await self.telegram.stop()
            await self.telegram.shutdown()

    def member_enters_group(self, old: ChatMember, new: ChatMember) -> bool:
        IN_GROUP = ["creator", "administrator", "member", "restricted"]
        OUT_GROUP = ["left", "banned"]

        return old.status in OUT_GROUP and new.status in IN_GROUP

    def member_leaves_group(self, old: ChatMember, new: ChatMember) -> bool:
        IN_GROUP = ["creator", "administrator", "member", "restricted"]
        OUT_GROUP = ["left", "kicked"]

        return old.status in IN_GROUP and new.status in OUT_GROUP

    async def bot_member_update(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        # Determine if we've been added or remove from a group

        assert update.my_chat_member is not None

        await self.app.db.update_chat(update.my_chat_member.chat)
        await self.app.db.update_chat(update.my_chat_member.from_user)

        if update.my_chat_member.chat.type not in ["group", "supergroup"]:
            log.debug(
                f"Bot member updated in a non-group chat ({update.my_chat_member.chat.id}). Ignoring"
            )
            return

        assert update.my_chat_member.chat.title is not None, "Group chat has no title"

        if self.member_enters_group(
            update.my_chat_member.old_chat_member, update.my_chat_member.new_chat_member
        ):
            # I was added to a group
            if update.my_chat_member.from_user.id not in self.app.bot_admins:
                # A non admin added me to a group, leave immediately
                log.warning(
                    f"Added to group {get_chat_name(update.my_chat_member.chat)} by non-admin {get_chat_name(update.my_chat_member.from_user)}. Leaving"
                )
                await context.bot.send_message(
                    update.my_chat_member.chat.id,
                    "Sorry, I can only be added to groups by my bot admin",
                )
                await context.bot.leave_chat(update.my_chat_member.chat.id)
                return

            log.info(f"Added to group {get_chat_name(update.my_chat_member.chat)}")

            # Track this group in the database
            await self.app.db.add_group(
                update.my_chat_member.chat.id, update.my_chat_member.chat.title
            )

            # Request a list of all admins
            chat = update.my_chat_member.chat

            for admin in await chat.get_administrators():
                await self.app.db.add_user(admin.user)
                await self.app.db.add_user_to_group(
                    admin.user.id, chat.id, is_admin=True
                )

            self.command_updater.commands_updated()

        elif self.member_leaves_group(
            update.my_chat_member.old_chat_member, update.my_chat_member.new_chat_member
        ):
            # I was removed from a group

            if update.my_chat_member.from_user.id == context.bot.id:
                # This was done by myself, ignore
                return

            log.warning(
                f"Got removed from group {get_chat_name(update.my_chat_member.chat)} by {get_chat_name(update.my_chat_member.from_user)}"
            )

            await self.app.db.remove_group(update.my_chat_member.chat.id)

            self.command_updater.commands_updated()

    async def chat_member_update(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        assert update.chat_member is not None

        # Is this chat in our group?
        if update.chat_member.chat.id not in self.app.db.groups:
            return

        await self.app.db.update_chat(update.chat_member.from_user)
        await self.app.db.update_chat(update.chat_member.chat)

        if self.member_enters_group(
            update.chat_member.old_chat_member, update.chat_member.new_chat_member
        ):
            # A user joined the group
            log.info(
                f"User {get_chat_name(update.chat_member.new_chat_member.user)} joined group {get_chat_name(update.chat_member.chat)}"
            )

            # Record that this user is in this group
            await self.app.db.add_user(update.chat_member.new_chat_member.user)
            await self.app.db.add_user_to_group(
                update.chat_member.new_chat_member.user.id, update.chat_member.chat.id
            )

        elif self.member_leaves_group(
            update.chat_member.old_chat_member, update.chat_member.new_chat_member
        ):
            # A user left the group
            log.info(
                f"User {get_chat_name(update.chat_member.new_chat_member.user)} left group {get_chat_name(update.chat_member.chat)}"
            )

            # Remove that this user is in this group
            await self.app.db.remove_user_from_group(
                update.chat_member.new_chat_member.user.id, update.chat_member.chat.id
            )

        else:
            # Update admin status changes
            is_admin = update.chat_member.new_chat_member.status in [
                "administrator",
                "creator",
            ]
            await self.app.db.update_admin(
                update.chat_member.new_chat_member.user.id,
                update.chat_member.chat.id,
                is_admin,
            )

        self.command_updater.commands_updated()

    async def activity_tracker(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        # Update any changed chat/user info if we already have a record of them
        if update.effective_chat is not None:
            await self.app.db.update_chat(update.effective_chat)
        if update.effective_user is not None:
            await self.app.db.update_chat(update.effective_user)

        # Check if we know this user is in any of our groups
        if (
            update.effective_user is None
            or update.effective_chat is None
            or update.effective_chat.id not in self.app.db.groups
        ):
            return

        # Make sure it's not someone leaving
        if update.message is not None and update.message.left_chat_member is not None:
            return

        # Record that this user is in this group
        await self.app.db.add_user(update.effective_user)
        await self.app.db.add_user_to_group(
            update.effective_user.id, update.effective_chat.id
        )

        # Track activity if there is a message
        if update.message is not None:
            await self.app.db.record_activity(
                update.effective_user.id, update.effective_chat.id
            )

    def register_command(
        self,
        command: str,
        description: str,
        handler: COMMAND_HANDLER,
        access: ACCESS,
        group_id: int | None = None,
    ) -> CommandRegister:
        command_class = Command(description, handler, access, group_id)

        commands = self.commands.setdefault(command, [])
        commands.append(command_class)

        if len(commands):  # First time this command is registered
            self.telegram.add_handler(
                CommandHandler(command, self.recv_command), group=10
            )

        self.command_updater.commands_updated()

        return CommandRegister(self, command, command_class)

    def deregister_command(
        self,
        command: str,
        command_class: Command,
    ) -> None:
        commands = self.commands.get(command)
        if not commands:
            return

        with suppress(ValueError):
            commands.remove(command_class)

        if len(commands) == 0:
            del self.commands[command]
            # No more handlers for this command, remove the telegram handler
            self.telegram.remove_handler(
                CommandHandler(command, self.recv_command), group=10
            )

        self.command_updater.commands_updated()

    def register_chat_data(
        self, handler: MESSAGE_HANDLER, group_id: int
    ) -> ChatDataRegister:
        chat_data_class = ChatData(handler, group_id)

        chat_data = self.chat_data.setdefault(group_id, [])
        chat_data.append(chat_data_class)

        if len(chat_data):  # First time this chat data is registered
            self.telegram.add_handler(
                MessageHandler(filters.ALL, self.recv_chat_data), group=5
            )

        return ChatDataRegister(self, group_id, chat_data_class)

    def deregister_chat_data(
        self,
        group_id: int,
        chat_data_class: ChatData,
    ) -> None:
        chat_data = self.chat_data.get(group_id)
        if not chat_data:
            return

        with suppress(ValueError):
            chat_data.remove(chat_data_class)

        if len(chat_data) == 0:
            del self.chat_data[group_id]
            # No more handlers for this chat data, remove the telegram handler
            self.telegram.remove_handler(
                MessageHandler(filters.ALL, self.recv_chat_data), group=5
            )

    def has_access_to_command(
        self, command: Command, user_id: int | None, group_id: int | None
    ) -> bool:
        """
        Check if a user has access to a command in a specific group
        (or None for private)
        """
        if user_id is None:
            # No particular user
            user_id = 0

        if group_id == user_id:
            # This is a private chat
            group_id = None

        is_bot_admin = user_id in self.app.bot_admins
        is_admin = self.app.db.is_admin(user_id)
        is_group_admin = group_id is not None and self.app.db.is_admin_of_group(
            user_id, group_id
        )

        handler = self.get_first_handler(
            handlers=[command],
            group_id=group_id,
            is_bot_admin=is_bot_admin,
            is_admin=is_admin,
            is_group_admin=is_group_admin,
        )
        return handler is not None

    async def recv_command(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if (
            update.message is None
            or update.message.text is None
            or update.effective_user is None
            or update.effective_chat is None
        ):
            return

        # Extract the command and arguments
        match = re.match(
            r"^/(\w+)(?:@\w+)?(?:\s+(.*))?$", update.message.text, re.DOTALL
        )
        if not match:
            return

        command = match.group(1)
        args = match.group(2) or ""

        # Do we have any commands for this registered?
        handlers = self.commands.get(command)
        if not handlers:
            return

        group_id: int | None = None
        if update.effective_chat.type != "private":
            group_id = update.effective_chat.id

        handler = self.get_first_handler(
            handlers=handlers,
            group_id=group_id,
            is_bot_admin=update.effective_user.id in self.app.bot_admins,
            is_admin=self.app.db.is_admin(update.effective_user.id),
            is_group_admin=self.app.db.is_admin_of_group(
                update.effective_user.id, update.effective_chat.id
            ),
        )

        if handler is None:
            return

        await handler(update, context, args)

    async def recv_callback_query(self, update: Update, context: CallbackContext):
        assert update.callback_query is not None

        if (
            message := update.callback_query.message
        ):  # Callback for a message with buttons
            buttons = self.button_callbacks.get(message.message_id)
            if buttons is None:
                return

            handler = buttons.handlers.get(update.callback_query.data or "")
            if handler is None:
                return

            await handler(update, context)

    async def recv_chat_data(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if (
            update.message is None
            or update.effective_user is None
            or update.effective_chat is None
        ):
            return

        chat_data = self.chat_data.get(update.effective_chat.id)
        if not chat_data:
            return

        for chat_data_class in chat_data:
            await chat_data_class.handler(update, context)

    def get_first_handler(
        self,
        handlers: list[Command],
        group_id: int | None,
        is_bot_admin: bool,
        is_admin: bool,
        is_group_admin: bool,
    ) -> COMMAND_HANDLER | None:
        # Check from the highest level to lowest level

        access_levels: list[ACCESS] = []

        # Create a list of access levels to check in order
        if group_id is None and is_bot_admin:
            access_levels.append(ACCESS.BOT_ADMIN_DM)
            access_levels.append(ACCESS.BOT_ADMIN)

        elif group_id and is_bot_admin:
            access_levels.append(ACCESS.BOT_ADMIN)

        if group_id is None and (is_admin or is_bot_admin):
            access_levels.append(ACCESS.ALL_ADMINS_DM)
            access_levels.append(ACCESS.ALL_ADMINS)

        elif group_id and (is_admin or is_bot_admin):
            access_levels.append(ACCESS.ALL_ADMINS)

        if group_id is not None and (is_group_admin or is_bot_admin):
            access_levels.append(ACCESS.GROUP_ADMINS)

        if group_id is None:
            access_levels.append(ACCESS.EVERYONE_DM)

        access_levels.append(ACCESS.EVERYONE)

        # Pick the first one that matches
        for access in access_levels:
            for handler in handlers:
                if handler.access == access:
                    if handler.group_id is None or handler.group_id == group_id:
                        return handler.handler

        return None

    async def send_or_replace_message_buttons(
        self,
        message: Message | MaybeInaccessibleMessage,
        text: str,
        buttons: list[list[tuple[str, BUTTON_HANDLER]]],
    ):
        """
        Send a message with inline keyboard buttons, while assinging the buttons
        to actual function calls.
        """

        keyboard_markup: list[list[InlineKeyboardButton]] = []
        handlers: dict[str, BUTTON_HANDLER] = {}

        # Convert the button definitions to telegram buttons
        index: int = 0
        for row in buttons:
            keyboard_row: list[InlineKeyboardButton] = []
            for label, handler in row:
                callback_id = str(index)
                keyboard_row.append(
                    InlineKeyboardButton(label, callback_data=callback_id)
                )

                handlers[callback_id] = handler
                index += 1

            keyboard_markup.append(keyboard_row)

        if (
            isinstance(message, Message)
            and message.from_user is not None
            and message.from_user.id == self.bot.id
        ):  # We can edit this message
            # Remove all previous button callbacks
            if message.message_id in self.button_callbacks:
                del self.button_callbacks[message.message_id]

            await self.bot.edit_message_text(
                text=text,
                chat_id=message.chat.id,
                message_id=message.message_id,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard_markup),
            )

        else:
            message = await self.bot.send_message(
                chat_id=message.chat.id,
                text=text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard_markup),
            )

        self.button_callbacks[message.message_id] = Buttons(
            when=time.time(), handlers=handlers
        )

    async def send_message_get_reply(
        self,
        message: Message | MaybeInaccessibleMessage,
        context: CallbackContext,
        text: str,
        callback: REPLY_HANDLER,
    ):
        message = await self.bot.send_message(
            text=text,
            chat_id=message.chat.id,
            parse_mode="HTML",
        )
        assert isinstance(context.user_data, dict)
        context.user_data["awaiting_reply"] = callback

    async def bot_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.message is None or update.message.text is None:
            return

        assert isinstance(context.user_data, dict)

        if "awaiting_reply" not in context.user_data:
            return

        handler: REPLY_HANDLER = context.user_data["awaiting_reply"]
        del context.user_data["awaiting_reply"]

        await handler(update, context, update.message.text_html)

        raise ApplicationHandlerStop
        raise ApplicationHandlerStop
