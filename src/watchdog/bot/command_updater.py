import asyncio
import logging
import pickle
from typing import TYPE_CHECKING, Generator

from telegram import (
    BotCommandScope,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeChat,
    BotCommandScopeChatMember,
)

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..watchdog import App
    from .bot import Bot, Command

TIMEOUT = 3  # Seconds to wait for changes will be applied to Telegram
DICT_OF_SCOPES = dict[BotCommandScope, list[tuple[str, str]]]


class CommandUpdater:
    def __init__(self, app: "App", bot: "Bot"):
        self.app = app
        self.bot = bot
        self.db = app.db

        self.running_task: None | asyncio.Task = None

    def commands_updated(self):
        """Call this when the commands have been updated and need to be pushed to Telegram"""
        if self.running_task:
            # There is already a task running
            return

        self.running_task = asyncio.create_task(self.update_commands())

    async def update_commands(self):
        await asyncio.sleep(TIMEOUT)

        self.running_task = None  # Allow new tasks to be created during this run

        # Build a list of scopes
        scopes: DICT_OF_SCOPES = {}

        # A list of user_ids to check
        check_users: set[int | None] = set()
        check_users.update(self.app.bot_admins)
        check_users.update(user.id for user in self.db.get_all_group_admins())
        check_users.add(None)  # Everyone

        def list_access(
            command: "Command", group_id: int | None
        ) -> Generator[int | None, None, None]:
            for user_id in check_users:
                if self.bot.has_access_to_command(command, user_id, group_id):
                    yield user_id

        def add_scope(scope: BotCommandScope, command: str, description: str):
            scopes.setdefault(scope, []).append((command, description))

        for commands, items in self.bot.commands.items():
            for item in items:
                # Check private chats
                for user_id in list_access(item, None):
                    if user_id is None:
                        # Everyone
                        add_scope(
                            BotCommandScopeAllPrivateChats(), commands, item.description
                        )
                    else:
                        # Specific user
                        add_scope(
                            BotCommandScopeChat(user_id), commands, item.description
                        )

                # Check all group chats
                for group_id in self.db.groups.keys():
                    for user_id in list_access(item, group_id):
                        if user_id is None:
                            # Everyone
                            add_scope(
                                BotCommandScopeChat(group_id),
                                commands,
                                item.description,
                            )
                        else:
                            # Specific user
                            add_scope(
                                BotCommandScopeChatMember(group_id, user_id),
                                commands,
                                item.description,
                            )

        old_scopes = self.load_scopes()

        # Updated the scopes that have changed
        for scope, commands in self.find_changed_scopes(old_scopes, scopes).items():
            await self.bot.bot.set_my_commands(commands, scope=scope)

        # Remove old scopes
        for scope in self.find_remove_old_scopes(old_scopes, scopes):
            await self.bot.bot.delete_my_commands(scope=scope)

        # Save the scopes for later use
        self.save_scopes(scopes)

        self.running_task = None

    def save_scopes(self, scopes: DICT_OF_SCOPES):
        """Save the scopes  for later use"""
        storage = self.app.data_folder / "command_scopes.pkl"
        pickle.dump(scopes, storage.open("wb"))

    def load_scopes(self) -> DICT_OF_SCOPES:
        """Load the scopes from storage"""
        storage = self.app.data_folder / "command_scopes.pkl"
        if not storage.exists():
            return {}

        try:
            scopes: DICT_OF_SCOPES = pickle.load(storage.open("rb"))
            return scopes
        except Exception as e:
            log.error(f"Failed to load command scopes: {e}")
            return {}

    def find_changed_scopes(
        self, old_scopes: DICT_OF_SCOPES, new_scopes: DICT_OF_SCOPES
    ) -> DICT_OF_SCOPES:
        """Find the scopes that have changed between old and new"""

        changed_scopes: DICT_OF_SCOPES = {}

        for new_scope, new_commands in new_scopes.items():
            # Find the matching old scope
            if old_commands := old_scopes.get(new_scope):
                if old_commands != new_commands:
                    # Changed scope
                    changed_scopes[new_scope] = new_commands
            else:
                # New scope
                changed_scopes[new_scope] = new_commands

        return changed_scopes

    def find_remove_old_scopes(
        self, old_scopes: DICT_OF_SCOPES, new_scopes: DICT_OF_SCOPES
    ) -> list[BotCommandScope]:
        """Find the removed scopes that the new one doesn't have"""

        removed_scopes: list[BotCommandScope] = []
        for old_scope in old_scopes.keys():
            if old_scope not in new_scopes:
                # This scope is no longer used, remove it
                removed_scopes.append(old_scope)

        return removed_scopes
