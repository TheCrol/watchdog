import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..watchdog import App

log = logging.getLogger("botadmin")


class BotAdmin:
    def __init__(self, app: "App"):
        self.app = app
        self.bot = app.bot
        self.db = app.db

    def notify_sync(self, text: str):
        """Sync version of notify"""
        asyncio.create_task(self.notify(text))

    async def notify(self, text: str):
        """Notify all bot admins with a message"""
        message = f"❗ <b>Watchdog notification</b> ❗\n\n{text}"
        for admin_id in self.app.bot_admins:
            await self.bot.bot.send_message(admin_id, message, parse_mode="HTML")

    """async def btn_leave_group(
        self, group_id: int, update: Update, context: CallbackContext
    ):
        # Confirm that the admin wants to leave the group
        if (
            update.callback_query is None
            or (message := update.callback_query.message) is None
        ):
            return

        group = self.db.groups.get(group_id)
        if group is None:
            return

        await self.bot.send_or_replace_message_buttons(
            message,
            f"⚠️ Are you sure you want me to leave the group '{group.title}'?",
            [
                [
                    ("✅ Yes, leave", partial(self.btn_confirm_leave_group, group_id)),
                    ("❌ No, go back", partial(self.btn_select_group, group_id)),
                ]
            ],
        )

    async def btn_confirm_leave_group(
        self, group_id: int, update: Update, context: CallbackContext
    ):
        if (
            update.callback_query is None
            or (message := update.callback_query.message) is None
        ):
            return

        group = self.db.groups.get(group_id)
        if group is None:
            return

        await update.callback_query.answer()

        # Leave the group
        await self.bot.bot.leave_chat(group_id)
        await self.bot.bot.send_message(
            message.chat.id, f"👋 I have left the group '{group.title}'."
        )
        log.info(f"Left group '{group.title}' (ID: {group.id}) as requested by admin.")

        # Clean up the database
        await self.db.remove_group(group_id)"""
