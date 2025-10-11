import logging
from typing import TYPE_CHECKING

from ..settings import (
    Answer,
    BackToMenuAnswer,
    ButtonsAnswer,
    ExecAnswer,
    GroupSelect,
    OutputAnswer,
    SingleConfig,
    UserInfo,
)
from ..useful import AccessRequired

if TYPE_CHECKING:
    from .botadmin import BotAdmin

log = logging.getLogger(__name__)


class CheckAdminsConfig(SingleConfig):
    static_name = "Check admins"
    access = AccessRequired(bot_admin=True)
    group_select = GroupSelect.ALL_GROUPS

    def __init__(self, botadmin: "BotAdmin"):
        self.botadmin = botadmin

    async def get_button(self, user: UserInfo) -> str:
        return self.static_name

    async def on_button(self, user: UserInfo) -> Answer:
        # TODO: Check admins

        return OutputAnswer(
            "✅ Checked admins in all groups.", next_answer=BackToMenuAnswer()
        )


class LeaveGroupConfig(SingleConfig):
    static_name = "Leave group"
    access = AccessRequired(bot_admin=True)
    group_select = GroupSelect.ALL_GROUPS

    def __init__(self, botadmin: "BotAdmin"):
        self.botadmin = botadmin
        self.app = botadmin.app

    async def get_button(self, user: UserInfo) -> str:
        return self.static_name

    async def on_button(self, user: UserInfo) -> Answer:
        assert user.group is not None

        return ButtonsAnswer(
            text=f"⚠️ Are you sure you want me to leave {user.group.name}?",
            buttons=[
                ("✅ Yes, leave", ExecAnswer(callback=self.on_confirm_leave)),
                ("❌ No, go back", BackToMenuAnswer()),
            ],
        )

    async def on_confirm_leave(self, user: UserInfo) -> Answer:
        assert user.group is not None

        await self.app.bot.bot.leave_chat(user.group.id)

        self.botadmin.notify_sync(
            f"Left group '{user.group.name}' (ID: {user.group.id}) as requested by admin {user.name} (ID: {user.id})."
        )
        log.info(f"Left group '{user.group.name}' as requested by admin {user.name}")

        return OutputAnswer(
            f"👋 I have left the group '{user.group.name}'.",
            next_answer=BackToMenuAnswer(),
        )
