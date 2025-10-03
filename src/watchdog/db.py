import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Type, TypeVar

import aiosqlite
from dacite import from_dict
from telegram import Chat
from telegram import User as TGUser

if TYPE_CHECKING:
    from .watchdog import App

log = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class Group:
    id: int
    title: str


@dataclass
class User:
    id: int
    name: str
    username: str | None

    @property
    def mention(self) -> str:
        return f"<a href='tg://user?id={self.id}'>{self.name}</a>"


@dataclass
class InGroup:
    id: int
    group_id: int
    is_admin: bool


class DB:
    def __init__(self, app: "App"):
        self.app = app

        self.groups: dict[int, Group] = {}
        self.users: dict[int, User] = {}
        self.in_group: list[InGroup] = []

    async def start(self):
        self.db = await aiosqlite.connect(
            self.app.data_folder / "watchdog.db", autocommit=False
        )
        self.db.row_factory = aiosqlite.Row

        await self.setup_schema()
        await self.read_values()

        asyncio.create_task(self.cleanup())

    async def stop(self):
        await self.db.close()

    async def setup_schema(self):
        # Set up the database schema if it doesn't exist
        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS groups (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL
            )
            """
        )

        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                username TEXT
            )
            """
        )

        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS in_group (
                id INTEGER NOT NULL,
                group_id INTEGER NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(id, group_id)
            )
            """
        )

        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                group_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL
            )
            """
        )

        await self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS apps (
                app TEXT NOT NULL,
                group_id INTEGER NOT NULL,
                config TEXT NOT NULL,
                PRIMARY KEY(app, group_id)
            )
            """
        )

        await self.db.commit()

    async def read_values(self):
        # Fetch all useful values from the database

        async with self.db.execute("SELECT * FROM groups") as cursor:
            async for row in cursor:
                group = Group(id=row["id"], title=row["title"])
                self.groups[group.id] = group

        async with self.db.execute("SELECT * FROM users") as cursor:
            async for row in cursor:
                user = User(id=row["id"], name=row["name"], username=row["username"])
                self.users[user.id] = user

        async with self.db.execute("SELECT * FROM in_group") as cursor:
            async for row in cursor:
                in_group = InGroup(
                    id=row["id"],
                    group_id=row["group_id"],
                    is_admin=bool(row["is_admin"]),
                )
                self.in_group.append(in_group)

    async def cleanup(self):
        # Periodically clean up old activity records
        while True:
            await self.cleanup_activities()
            await self.cleanup_users()
            await self.db.commit()
            await asyncio.sleep(60 * 10)  # Every 30 minutes

    async def cleanup_activities(self):
        # Clean up activity records older than 6 months
        await self.db.execute(
            "DELETE FROM activities WHERE timestamp < ?",
            (time.time() - (60 * 24 * 30 * 6),),
        )

    async def cleanup_users(self):
        # Remove users that are not in any group and have no activity recorded
        async with self.db.execute(
            """
            SELECT u.id FROM users u
            LEFT JOIN in_group ig ON u.id = ig.id
            LEFT JOIN activities a ON u.id = a.user_id
            WHERE ig.id IS NULL AND a.id IS NULL
            """
        ) as cursor:
            users_to_remove = [row["id"] async for row in cursor]

        for user_id in users_to_remove:
            log.debug(f"Removing user {user_id} from database (no groups, no activity)")
            await self.db.execute("DELETE FROM users WHERE id = ?", (user_id,))
            if user_id in self.users:
                del self.users[user_id]

    async def add_group(self, group_id: int, title: str):
        if group_id in self.groups:
            log.warning(f"Group {group_id} already exists in database, skipping add")
            return

        await self.db.execute(
            "INSERT INTO groups (id, title) VALUES (?, ?)", (group_id, title)
        )
        await self.db.commit()

        group = Group(id=group_id, title=title)
        self.groups[group.id] = group

    async def remove_group(self, group_id: int):
        if group_id not in self.groups:
            log.warning(f"Group {group_id} does not exist in database, skipping remove")
            return

        await self.db.execute("DELETE FROM groups WHERE id = ?", (group_id,))
        await self.db.execute("DELETE FROM in_group WHERE group_id = ?", (group_id,))
        await self.db.execute("DELETE FROM activities WHERE group_id = ?", (group_id,))
        await self.db.execute("DELETE FROM apps WHERE group_id = ?", (group_id,))
        await self.db.commit()

        del self.groups[group_id]

        # Also remove all in_group entries for this group
        self.in_group = [ig for ig in self.in_group if ig.group_id != group_id]

    async def update_chat(self, chat: Chat | TGUser):
        # Check if the name is changed, if so, update it

        if isinstance(chat, Chat) and (
            group := self.groups.get(chat.id)
        ):  # This is a group
            title = chat.title or "[Unnamed group]"
            if group.title != title:
                log.debug(f"Updating group {chat.id} from '{group.title}' to '{title}'")

                await self.db.execute(
                    "UPDATE groups SET title = ? WHERE id = ?", (title, chat.id)
                )
                await self.db.commit()
                group.title = title

        elif user := self.users.get(chat.id):  # This is a user
            name = chat.full_name or "[Unnamed user]"

            if user.name != name or user.username != chat.username:
                debug_previous_username = f" @{user.username}" if user.username else ""
                debug_new_username = f" @{chat.username}" if chat.username else ""
                log.debug(
                    f"Updating user {chat.id} from '{user.name}'{debug_previous_username} to '{name}'{debug_new_username}"
                )

                await self.db.execute(
                    "UPDATE users SET name = ?, username = ? WHERE id = ?",
                    (name, chat.username, chat.id),
                )
                await self.db.commit()
                user.name = name
                user.username = chat.username

    async def add_user(self, user: TGUser):
        # Add a user to our record if we don't have them yet
        if user.id in self.users:
            return

        log.debug("Adding user")

        name = user.full_name or "[Unnamed user]"
        username = user.username

        self.users[user.id] = User(id=user.id, name=name, username=username)

        # Add to the database
        await self.db.execute(
            "INSERT INTO users (id, name, username) VALUES (?, ?, ?)",
            (user.id, name, username),
        )
        await self.db.commit()

    async def add_user_to_group(
        self, user_id: int, group_id: int, is_admin: bool = False
    ):
        # Add a user to a group if we don't have them recorded in it yet

        if any(ig.id == user_id and ig.group_id == group_id for ig in self.in_group):
            return

        log.debug("Adding user to group")

        # Add to the database
        await self.db.execute(
            "INSERT INTO in_group (id, group_id, is_admin) VALUES (?, ?, ?)",
            (user_id, group_id, int(is_admin)),
        )

        # Add to the group
        in_group = InGroup(id=user_id, group_id=group_id, is_admin=False)
        self.in_group.append(in_group)

    async def record_activity(self, user_id: int, group_id: int) -> None:
        await self.db.execute(
            "INSERT INTO activities (user_id, group_id, timestamp) VALUES (?, ?, ?)",
            (user_id, group_id, time.time()),
        )
        await self.db.commit()

    async def remove_user_from_group(self, user_id: int, group_id: int):
        # Remove a user from a group if we have them recorded in it

        for index, in_group in enumerate(self.in_group):
            if in_group.id == user_id and in_group.group_id == group_id:
                log.debug("Removing user from group")
                del self.in_group[index]

                # Remove from the database
                await self.db.execute(
                    "DELETE FROM in_group WHERE id = ? AND group_id = ?",
                    (user_id, group_id),
                )
                await self.db.commit()
                return

    async def update_admin(self, user_id: int, group_id: int, is_admin: bool):
        for in_group in self.in_group:
            if in_group.id == user_id and in_group.group_id == group_id:
                if in_group.is_admin == is_admin:
                    return

                log.debug("Updating admin status")

                in_group.is_admin = is_admin

                await self.db.execute(
                    "UPDATE in_group SET is_admin = ? WHERE id = ? AND group_id = ?",
                    (int(is_admin), user_id, group_id),
                )
                await self.db.commit()
                return

    def is_admin(self, user_id: int) -> bool:
        for in_group in self.in_group:
            if in_group.id == user_id and in_group.is_admin:
                return True
        return False

    def is_admin_of_group(self, user_id: int, group_id: int) -> bool:
        for in_group in self.in_group:
            if in_group.id == user_id and in_group.group_id == group_id:
                return in_group.is_admin
        return False

    def count_participants(self, group_id: int) -> int:
        return sum(1 for ig in self.in_group if ig.group_id == group_id)

    async def count_messages(self, group_id: int) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) as count FROM activities WHERE group_id = ?", (group_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row["count"] if row else 0

    def get_group_admins(self, group_id: int) -> list[User]:
        admins = []
        for in_group in self.in_group:
            if in_group.group_id == group_id and in_group.is_admin:
                if user := self.users.get(in_group.id):
                    admins.append(user)
        return admins

    def get_all_group_admins(self) -> list[User]:
        admins = []
        for in_group in self.in_group:
            if in_group.is_admin:
                if user := self.users.get(in_group.id):
                    admins.append(user)
        return admins

    def get_groups_from_admin(self, user_id: int) -> list[Group]:
        groups = []
        for in_group in self.in_group:
            if in_group.id == user_id and in_group.is_admin:
                if group := self.groups.get(in_group.group_id):
                    groups.append(group)
        return groups

    async def get_app_configs(self, app: str, data_class: Type[T]) -> dict[int, T]:
        """Get all configurations for a specific app, for all groups"""

        configs: dict[int, T] = {}

        async with self.db.execute(
            "SELECT group_id, config FROM apps WHERE app = ?", (app,)
        ) as cursor:
            async for row in cursor:
                group_id = row["group_id"]
                config_str = row["config"]
                config = json.loads(config_str)
                data = from_dict(data_class, config)
                configs[group_id] = data
        return configs

    async def set_app_config(self, app: str, group_id: int, config: Any) -> None:
        """Set the configuration for a specific app in a specific group"""
        config_str = json.dumps(asdict(config))

        await self.db.execute(
            """
            INSERT OR REPLACE INTO apps (app, group_id, config)
            VALUES (?, ?, ?)
            """,
            (app, group_id, config_str),
        )
        await self.db.commit()
