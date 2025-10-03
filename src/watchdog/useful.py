from enum import IntEnum

from telegram import Chat, User


class ACCESS(IntEnum):
    # All admin specific access levels within the group are also implied to be
    # usable inside private messages with the bot.
    # While the EVERYONE and EVERYONE_DM levels are separate.
    EVERYONE = 0  # All users in groups. Takes optional group_id
    EVERYONE_DM = 1  # Everyone in private messages
    GROUP_ADMINS = 2  # Admins of that specific group. Requires group_id
    ALL_ADMINS = 3  # Admins of all groups the bot is in. Takes optional group_id
    ALL_ADMINS_DM = 4  # Admins of all groups in private messages
    BOT_ADMIN = 5  # Bot admins in groups. Takes optional group_id
    BOT_ADMIN_DM = 6  # Bot admins in private messages


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
