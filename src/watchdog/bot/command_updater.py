import asyncio
import logging
import pickle
from typing import TYPE_CHECKING

from telegram import (
    BotCommandScope,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeChat,
    BotCommandScopeChatAdministrators,
    BotCommandScopeChatMember,
)

from ..useful import ACCESS

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..watchdog import App
    from .bot import Bot

TIMEOUT = 3  # Seconds to wait for changes will be applied to Telegram
LIST_OF_SCOPES = list[tuple[list[tuple[str, str]], BotCommandScope]]


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

        # Sort the commands by bot scope
        self.everyone_dm: list[tuple[str, str]] = []
        self.everyone: dict[int, list[tuple[str, str]]] = {}
        self.group_admins: dict[int, list[tuple[str, str]]] = {}
        self.all_admins: dict[int, list[tuple[str, str]]] = {}
        self.all_admins_dm: list[tuple[str, str]] = []
        self.bot_admin: dict[int, list[tuple[str, str]]] = {}
        self.bot_admin_dm: list[tuple[str, str]] = []

        # Split the commands by access level
        for command, items in self.bot.commands.items():
            for item in items:
                if item.access == ACCESS.EVERYONE_DM:
                    self.everyone_dm.append((command, item.description))

                elif item.access == ACCESS.EVERYONE:
                    if item.group_id is None:
                        for target_group_id in self.db.groups.keys():
                            self.everyone.setdefault(target_group_id, []).append(
                                (command, item.description)
                            )
                    else:
                        self.everyone.setdefault(item.group_id, []).append(
                            (command, item.description)
                        )

                elif item.access == ACCESS.GROUP_ADMINS:
                    if item.group_id is None:
                        log.warning(
                            f"GROUP_ADMINS command '{command}' without group_id, skipping"
                        )
                        continue
                    self.group_admins.setdefault(item.group_id, []).append(
                        (command, item.description)
                    )

                elif item.access == ACCESS.ALL_ADMINS:
                    if item.group_id is None:
                        for target_group_id in self.db.groups.keys():
                            self.all_admins.setdefault(target_group_id, []).append(
                                (command, item.description)
                            )
                    else:
                        self.all_admins.setdefault(item.group_id, []).append(
                            (command, item.description)
                        )

                elif item.access == ACCESS.ALL_ADMINS_DM:
                    self.all_admins_dm.append((command, item.description))

                elif item.access == ACCESS.BOT_ADMIN:
                    if item.group_id is None:
                        for target_group_id in self.db.groups.keys():
                            self.bot_admin.setdefault(target_group_id, []).append(
                                (command, item.description)
                            )
                    else:
                        self.bot_admin.setdefault(item.group_id, []).append(
                            (command, item.description)
                        )

                elif item.access == ACCESS.BOT_ADMIN_DM:
                    self.bot_admin_dm.append((command, item.description))

        # Back fill the commands. Higher access levels get all the commands of the lower levels
        for group_id, commands in self.group_admins.items():
            commands.extend(self.everyone.get(group_id, []))

        for group_id, commands in self.all_admins.items():
            commands.extend(self.everyone.get(group_id, []))
            commands.extend(self.group_admins.get(group_id, []))

        for group_id, commands in self.bot_admin.items():
            commands.extend(self.everyone.get(group_id, []))
            commands.extend(self.group_admins.get(group_id, []))
            commands.extend(self.all_admins.get(group_id, []))

        self.all_admins_dm.extend(self.everyone_dm)
        self.bot_admin_dm.extend(self.everyone_dm)
        self.bot_admin_dm.extend(self.all_admins_dm)

        # Now build a list of scopes
        scopes: LIST_OF_SCOPES = []

        # All private chats
        if self.everyone_dm:
            scopes.append((self.everyone_dm, BotCommandScopeAllPrivateChats()))

        # Group chats
        for group_id, commands in self.everyone.items():
            scopes.append((commands, BotCommandScopeChat(group_id)))

        # Group admins
        for group_id, commands in self.group_admins.items():
            scopes.append((commands, BotCommandScopeChatAdministrators(group_id)))

        # All admins
        for group_id, commands in self.all_admins.items():
            for user in self.db.get_all_group_admins():
                if user.id in self.app.bot_admins:
                    continue  # Bot admins will be handled later
                scopes.append((commands, BotCommandScopeChatMember(group_id, user.id)))

        # Admins in private chats
        if self.all_admins_dm:
            for user in self.db.get_all_group_admins():
                if user.id in self.app.bot_admins:
                    continue  # Bot admins will be handled later
                scopes.append((self.all_admins_dm, BotCommandScopeChat(user.id)))

        # Bot admin
        for group_id, commands in self.bot_admin.items():
            for user_id in self.app.bot_admins:
                scopes.append((commands, BotCommandScopeChatMember(group_id, user_id)))

        # Bot admins in private chats
        if self.bot_admin_dm:
            for user_id in self.app.bot_admins:
                scopes.append((self.bot_admin_dm, BotCommandScopeChat(user_id)))

        old_scopes = self.load_scopes()

        # Updated the scopes that have changed
        for command, scope in self.find_changed_scopes(old_scopes, scopes):
            log.debug(f"Updating commands for scope {scope}: {command}")
            await self.bot.bot.set_my_commands(command, scope=scope)

        # Remove old scopes
        for scope in self.find_remove_old_scopes(old_scopes, scopes):
            log.debug(f"Removing old command scope: {scope}")
            await self.bot.bot.delete_my_commands(scope=scope)

        # Save the scopes for later use
        self.save_scopes(scopes)

        self.running_task = None

    def save_scopes(self, scopes: LIST_OF_SCOPES):
        """Save the scopes  for later use"""
        storage = self.app.data_folder / "command_scopes.pkl"
        pickle.dump(scopes, storage.open("wb"))

    def load_scopes(self) -> LIST_OF_SCOPES:
        """Load the scopes from storage"""
        storage = self.app.data_folder / "command_scopes.pkl"
        if not storage.exists():
            return []

        try:
            scopes: LIST_OF_SCOPES = pickle.load(storage.open("rb"))
            return scopes
        except Exception as e:
            log.error(f"Failed to load command scopes: {e}")
            return []

    def find_changed_scopes(
        self, old_scopes: LIST_OF_SCOPES, new_scopes: LIST_OF_SCOPES
    ) -> LIST_OF_SCOPES:
        """Find the scopes that have changed between old and new"""

        changed_scopes: LIST_OF_SCOPES = []

        for new_commands, new_scope in new_scopes:
            # Find the matching old scope
            found: bool = False
            for old_commands, old_scope in old_scopes:
                if old_scope == new_scope:
                    if old_commands != new_commands:
                        # Changed scope
                        changed_scopes.append((new_commands, new_scope))
                    found = True
                    break

            if not found:
                # New scope
                changed_scopes.append((new_commands, new_scope))

        return changed_scopes

    def find_remove_old_scopes(
        self, old_scopes: LIST_OF_SCOPES, new_scopes: LIST_OF_SCOPES
    ) -> list[BotCommandScope]:
        """Find the removed scopes that the new one doesn't have"""

        removed_scopes: list[BotCommandScope] = []
        for _, old_scope in old_scopes:
            if not any(
                isinstance(old_scope, type(new_scope)) and old_scope == new_scope
                for _, new_scope in new_scopes
            ):
                # This scope is no longer used, remove it
                removed_scopes.append(old_scope)

        return removed_scopes
