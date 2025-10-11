from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING

from ..settings import (
    Answer,
    BackToMenuAnswer,
    ButtonsAnswer,
    ExecAnswer,
    GroupSelect,
    InputAnswer,
    SingleConfig,
    UserInfo,
)
from ..useful import AccessRequired

if TYPE_CHECKING:
    from .imagesearch import ImageSearch


class GroupEnableConfig(SingleConfig):
    static_name = "Status"
    access = AccessRequired(all_admins=True)

    def __init__(self, imagesearch: "ImageSearch"):
        self.imagesearch = imagesearch
        self.db = imagesearch.db

    async def get_button(self, user: UserInfo) -> str:
        # Get the configs for all the groups, that this user is an admin in
        status: list[bool] = []
        for group in self.db.get_groups_from_admin(user.id):
            config = self.imagesearch.get_group_config(group.id)
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
            config = self.imagesearch.get_group_config(group.id)
            if config.enabled:
                text = f"✅ {group.title}"
            else:
                text = f"❌ {group.title}"

            buttons.append(
                (text, ExecAnswer(callback=partial(self.on_toggle, group.id)))
            )

        buttons.append(("🔙 Back", BackToMenuAnswer()))

        return ButtonsAnswer(
            text="Click the group to toggle between enable and disabling the image search function.",
            buttons=buttons,
            big_buttons=True,
        )

    async def on_toggle(self, group_id: int, user: UserInfo) -> Answer:
        config = self.imagesearch.get_group_config(group_id)
        config.enabled = not config.enabled
        await self.imagesearch.save_db()

        if config.enabled:
            self.imagesearch.add_group_registers(group_id)
        else:
            self.imagesearch.remove_group_registers(group_id)

        return await self.on_button(user)


class ForbiddenTagsConfig(SingleConfig):
    static_name = "Forbidden tags"
    access = AccessRequired(all_admins=True)
    group_select = GroupSelect.ADMINED_GROUPS

    def __init__(self, imagesearch: "ImageSearch"):
        self.imagesearch = imagesearch

    async def get_button(self, user: UserInfo) -> str:
        return self.static_name

    async def on_button(self, user: UserInfo) -> Answer:
        assert user.group
        config = self.imagesearch.get_group_config(user.group.id)

        text = (
            "When an image gets found on e621, it will scan the tags. "
            "If any of the tags are in this banned list, the image will be "
            "removed, and a message will be given. Click on any of the "
            "existing tags to remove them."
        )

        buttons: list[tuple[str, Answer]] = []

        for tag in config.forbidden_tags:
            buttons.append((f"❌ {tag}", ExecAnswer(partial(self.on_remove_tag, tag))))

        buttons.append(
            (
                "➕ Add new ",
                InputAnswer(
                    text="Send me the tag you want to add to the banned list. Or /cancel to stop.",
                    callback=self.on_add_tag,
                ),
            )
        )
        buttons.append(("🔙 Back", BackToMenuAnswer()))

        return ButtonsAnswer(text=text, buttons=buttons)

    async def on_remove_tag(self, tag: str, user: UserInfo) -> Answer:
        assert user.group

        config = self.imagesearch.get_group_config(user.group.id)
        with suppress(ValueError):
            config.forbidden_tags.remove(tag)

        await self.imagesearch.save_db()

        return await self.on_button(user)

    async def on_add_tag(self, user: UserInfo, tag: str) -> Answer:
        assert user.group

        tag = tag.strip().lower()
        if tag and not tag == "/cancel":
            config = self.imagesearch.get_group_config(user.group.id)
            if tag not in config.forbidden_tags:
                config.forbidden_tags.append(tag)
                await self.imagesearch.save_db()

        return await self.on_button(user)


class DMEnabledConfig(SingleConfig):
    static_name = "DM enabled"
    access = AccessRequired(all_admins=True)

    def __init__(self, imagesearch: "ImageSearch"):
        self.imagesearch = imagesearch
        self.db = imagesearch.db

    async def get_button(self, user: UserInfo) -> str:
        if self.imagesearch.config.dm_enabled:
            return "✅ DM enabled"
        else:
            return "❌ DM disabled"

    async def on_button(self, user: UserInfo) -> Answer:
        self.imagesearch.config.dm_enabled = not self.imagesearch.config.dm_enabled
        await self.imagesearch.save_db()

        if self.imagesearch.config.dm_enabled:
            self.imagesearch.add_dm_registers()
        else:
            self.imagesearch.remove_dm_registers()

        return BackToMenuAnswer()
