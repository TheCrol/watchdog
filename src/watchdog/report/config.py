from functools import partial
from typing import TYPE_CHECKING

from ..settings import (
    Answer,
    BackToMenuAnswer,
    ButtonsAnswer,
    ExecAnswer,
    SingleConfig,
    UserInfo,
)
from ..useful import ACCESS

if TYPE_CHECKING:
    from .report import Report


class GroupEnableConfig(SingleConfig):
    static_name = "Status"
    access = ACCESS.ALL_ADMINS

    def __init__(self, report: "Report"):
        self.report = report
        self.db = report.db

    async def get_button(self, user: UserInfo) -> str:
        # Get the configs for all the groups, that this user is an admin in
        status: list[bool] = []
        for group in self.db.get_groups_from_admin(user.id):
            config = self.report.get_group_config(group.id)
            status.append(config.enabled)

        if all(status):
            return "🟢 All enabled"
        elif any(status):
            return f"🟡 Some enabled ({sum(status)}/{len(status)})"
        else:
            return "🔴 All disabled"

    async def on_button(self, user: UserInfo) -> Answer:
        # Show a list of all groups this user is an admin in as buttons
        buttons: list[tuple[str, Answer]] = []
        for group in self.db.get_groups_from_admin(user.id):
            config = self.report.get_group_config(group.id)
            if config.enabled:
                text = f"✅ {group.title}"
            else:
                text = f"❌ {group.title}"

            buttons.append(
                (text, ExecAnswer(callback=partial(self.on_toggle, group.id)))
            )

        buttons.append(("🔙 Back", BackToMenuAnswer()))

        return ButtonsAnswer(
            text="Click the group to toggle between enable and disabling the report system.",
            buttons=buttons,
            big_buttons=True,
        )

    async def on_toggle(self, group_id: int, user: UserInfo) -> Answer:
        config = self.report.get_group_config(group_id)
        config.enabled = not config.enabled
        await self.report.save_db()

        if config.enabled:
            self.report.add_group_registers(group_id)
        else:
            self.report.remove_group_registers(group_id)

        return await self.on_button(user)


class PersonalEnableConfig(SingleConfig):
    static_name = "Receive reports"
    access = ACCESS.ALL_ADMINS

    def __init__(self, report: "Report"):
        self.report = report
        self.db = report.db

    async def get_button(self, user: UserInfo) -> str:
        config = self.report.get_user_config(user.id)
        if config.enabled:
            return "✅ You receive reports"
        else:
            return "❌ Don't receive reports"

    async def on_button(self, user: UserInfo) -> Answer:
        config = self.report.get_user_config(user.id)
        config.enabled = not config.enabled
        await self.report.save_db()

        return BackToMenuAnswer()
