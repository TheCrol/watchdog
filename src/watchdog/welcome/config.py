from functools import partial
from typing import TYPE_CHECKING

from ..settings import (
    Answer,
    BackToMenuAnswer,
    ButtonsAnswer,
    ExecAnswer,
    InputAnswer,
    SingleConfig,
    UserInfo,
)
from ..useful import ACCESS

if TYPE_CHECKING:
    from .welcome import Welcome


class GroupEnableConfig(SingleConfig):
    static_name = "Status"
    access = ACCESS.ALL_ADMINS

    def __init__(self, welcome: "Welcome"):
        self.welcome = welcome
        self.db = welcome.db

    async def get_button(self, user: UserInfo) -> str:
        # Get the configs for all the groups, that this user is an admin in
        status: list[bool] = []
        for group in self.db.get_groups_from_admin(user.id):
            config = self.welcome.get_group_config(group.id)
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
            config = self.welcome.get_group_config(group.id)
            if config.enabled:
                text = f"✅ {group.title}"
            else:
                text = f"❌ {group.title}"

            buttons.append(
                (text, ExecAnswer(callback=partial(self.on_toggle, group.id)))
            )

        buttons.append(("🔙 Back", BackToMenuAnswer()))

        return ButtonsAnswer(
            text="Click the group to toggle between enable and disabling the welcome message for.",
            buttons=buttons,
            big_buttons=True,
        )

    async def on_toggle(self, group_id: int, user: UserInfo) -> Answer:
        config = self.welcome.get_group_config(group_id)
        config.enabled = not config.enabled
        await self.welcome.save_db()

        if config.enabled:
            self.welcome.add_group_registers(group_id)
        else:
            self.welcome.remove_group_registers(group_id)

        return await self.on_button(user)


class MessageConfig(SingleConfig):
    static_name = "Message"
    access = ACCESS.GROUP_ADMINS
    requires_group = True

    def __init__(self, welcome: "Welcome"):
        self.welcome = welcome

    async def get_button(self, user: UserInfo) -> str:
        return self.static_name

    async def on_button(self, user: UserInfo) -> Answer:
        assert user.group

        config = self.welcome.get_group_config(user.group.id)
        text = (
            f"The message that will be send when a new users joins this group.\n"
            f"There are special placeholders available:\n\n"
            "<i>{name}</i> - The user's full name\n"
            "<i>{mention}</i> - A clickable user name of the user that's tied to their user id\n"
            "<i>{user_id}</i> - The user's Telegram user ID\n\n"
            "This is the current welcome message:\n"
            f"-----\n{config.message}"
        )
        return ButtonsAnswer(
            text=text,
            buttons=[
                (
                    "✏️ Edit",
                    InputAnswer(
                        text=(
                            f"Send me the new welcome message for "
                            f"<i>{user.group.name}</i>. You can cancel this "
                            f"operation by sending /cancel."
                        ),
                        callback=self.on_new_message,
                    ),
                ),
                ("🔙 Back", BackToMenuAnswer()),
            ],
        )

    async def on_new_message(self, user: UserInfo, text: str) -> Answer:
        assert user.group

        if text.strip() != "/cancel":
            config = self.welcome.get_group_config(user.group.id)
            config.message = text
            await self.welcome.save_db()

        return await self.on_button(user)
