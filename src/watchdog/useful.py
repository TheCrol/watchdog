from telegram import Chat, User


class AccessRequired:
    def __init__(
        self,
        bot_admin: bool = False,
        all_admins: bool = False,
        admin_of_group: bool = False,
        group_id: int | None = None,
    ):
        self.bot_admin = bot_admin
        self.admin_of_group = admin_of_group
        self.all_admins = all_admins
        self.group_id = group_id

        if admin_of_group and group_id is None:
            raise ValueError("group_id must be set if admin_of_group is True")

    def __repr__(self):
        access: str
        if self.bot_admin:
            access = "bot admins"
            if self.group_id is not None:
                access += f" in {self.group_id}"
        elif self.all_admins:
            access = "all admins"
            if self.group_id is not None:
                access += f" in {self.group_id}"
        elif self.admin_of_group and self.group_id is not None:
            access = f"admins of {self.group_id}"
        elif self.group_id is not None:
            access = f"everyone in {self.group_id}"
        else:
            access = "everyone in DMs"

        return f"<AccessRequired: {access}>"

    @property
    def priority(self) -> int:
        if self.bot_admin:
            return 3
        elif self.admin_of_group:
            return 2
        elif self.all_admins:
            return 1
        else:
            return 0

    @property
    def is_admin_access(self) -> bool:
        return self.bot_admin or self.all_admins or self.admin_of_group

    def has_access(
        self, is_bot_admin: bool, admin_of_groups: list[int], group_id: int | None
    ) -> bool:
        if self.group_id != group_id:
            return False

        if self.bot_admin:
            return is_bot_admin
        if is_bot_admin:
            return True
        if self.all_admins:
            return bool(admin_of_groups)
        if self.admin_of_group and self.group_id is not None:
            return self.group_id in admin_of_groups
        return True


def pluralize(count: int, singular: str, plural: str) -> str:
    """Return the singular or plural form based on the count."""

    return f"{count} {singular if count == 1 else plural}"


def get_chat_name(chat: Chat | User) -> str:
    """Get a human-readable name for a chat or user."""

    names: list[str] = []
    if isinstance(chat, Chat) and chat.title:
        names.append(chat.title)
    if chat.full_name:
        names.append(chat.full_name)
    if chat.username:
        names.append(f"@{chat.username}")
    names.append(f"({chat.id})")

    return " ".join(names)


def mention_html(user: User, full_mention: bool = False) -> str:
    """Generate an HTML mention for a user."""

    if full_mention and user.username:
        return f'<a href="tg://user?id={user.id}">{user.full_name}</a> @{user.username}'
    else:
        return f'<a href="tg://user?id={user.id}">{user.full_name}</a>'
