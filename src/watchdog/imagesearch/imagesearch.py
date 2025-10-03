import asyncio
import logging
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp
from telegram import Update, User
from telegram.ext import ContextTypes

from ..bot import ChatDataRegister
from ..botadmin import AppConfig, AppEnabledConfig, TextConfig
from ..useful import mention_html, pluralize
from .constants import DEFAULT_BANNED_TAGS, SELFTEST_HASH, ImageCheck
from .matching import Matching

if TYPE_CHECKING:
    from ..watchdog import App

log = logging.getLogger("imagesearch")


@dataclass
class Config:
    enabled: bool = False
    forbidden_tags: list[str] = field(
        default_factory=lambda: DEFAULT_BANNED_TAGS.copy()
    )


class ImageSearch:
    def __init__(self, app: "App"):
        self.app = app
        self.bot = app.bot
        self.db = app.db

        self.configs: dict[int, Config] = {}
        self.registers: dict[int, ChatDataRegister] = {}

        self.test_image_path = Path(__file__).parent / "selftest.jpg"

        self.ongoing_image_checks: dict[int, dict[str, list[ImageCheck]]] = {}

    async def start(self):
        if self.app.imghash_bin is None:
            log.warning(
                "Image search binary path is not configured. Image search will be disabled"
            )
            return

        if not await self.perform_selftest():
            return

        self.configs = await self.db.get_app_configs("imagesearch", Config)

        self.matching = Matching(self.app)
        self.matching.start()

        for group_id, config in self.configs.items():
            if not config.enabled:
                break
            self.add_group_register(group_id)

        self.app.botadmin.register_config(
            AppConfig(
                button_emoji="🖼️",
                name="Image sourching",
                description="Automatically tries to find the source of furry art being sent. Can optionally remove images with certain tags from e621",
                display_order=70,
                configs=[
                    AppEnabledConfig(
                        get_callback=self.botadmin_get_enabled,
                        set_callback=self.botadmin_set_enabled,
                    ),
                    TextConfig(
                        title="Forbidden tags",
                        description="List of e621 tags that are forbidden. If an image is found with any of these tags, it will be removed. Separate multiple tags with spaces",
                        get_callback=self.botadmin_get_forbidden_tags,
                        set_callback=self.botadmin_set_forbidden_tags,
                    ),
                ],
            )
        )

    async def perform_selftest(self) -> bool:
        """Perform a self-test to ensure the image search binary is working correctly"""
        if self.app.imghash_bin is None:
            log.warning(
                "Image search binary path is not configured. Image search will be disabled"
            )
            return False

        hash_value = await self.get_hash(self.test_image_path)
        if hash_value != SELFTEST_HASH:
            log.error(
                f"Image search self-test failed. Expected hash {SELFTEST_HASH}, got {hash_value}. Image search will be disabled"
            )
            return False

        log.debug("Image search self-test passed")

        return True

    async def get_hash(self, path: Path) -> int | None:
        """Get the image hash for a given image file using the external binary"""
        assert self.app.imghash_bin is not None

        try:
            result = await asyncio.create_subprocess_exec(
                self.app.imghash_bin,
                str(path.absolute()),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            output, _ = await result.communicate()
            output = output.strip()

            try:
                return int(output)
            except ValueError:
                log.error(f"Image search binary returned non-integer output: {output}")
                return None
        except FileNotFoundError:
            log.error(f"Image search binary not found at path: {self.app.imghash_bin}")
            return None
        except Exception as e:
            log.error(f"Error executing image search binary: {e}")
            return None

    def add_group_register(self, group_id: int):
        if group_id in self.registers:
            return

        chat_data = self.bot.register_chat_data(self.bot_chat_data, group_id)

        self.registers[group_id] = chat_data

    def remove_group_register(self, group_id: int):
        registers = self.registers.pop(group_id, None)
        if not registers:
            return
        registers.deregister_chat_data()

    async def bot_chat_data(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not update.message or not update.effective_user or not update.effective_chat:
            return

        # Check if this is an image upload
        if not update.message.photo:
            return

        photo = update.message.photo[-1]
        asyncio.create_task(
            self.check_image(
                group_id=update.effective_chat.id,
                user=update.effective_user,
                message_id=update.message.message_id,
                file_id=photo.file_id,
                caption=update.message.caption,
                media_group_id=update.message.media_group_id,
            )
        )

    async def check_image(
        self,
        group_id: int,
        user: User,
        message_id: int,
        file_id: str,
        caption: str | None,
        media_group_id: str | None,
    ):
        image_check = ImageCheck(caption=caption, message_id=message_id)

        # This is part of a media group
        if media_group_id:
            group_image_checks = self.ongoing_image_checks.setdefault(group_id, {})
            image_checks = group_image_checks.setdefault(media_group_id, [])
            image_checks.append(image_check)
        else:
            # This is just a single image
            image_checks = [image_check]

        try:
            tg_file = await self.bot.bot.get_file(file_id)
        except Exception as e:
            log.error(f"Failed to get file for image search: {e}")
            image_check.unknown = True
            await self.finish_image_check(
                group_id,
                user,
                media_group_id,
                image_checks,
            )
            return

        # Download the file to a temporary location
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_path = Path(tmpdirname) / "image.jpg"

            # Download the image
            try:
                await tg_file.download_to_drive(custom_path=tmp_path)
            except Exception as e:
                log.error(f"Failed to download file for image search: {e}")
                image_check.unknown = True
                await self.finish_image_check(
                    group_id,
                    user,
                    media_group_id,
                    image_checks,
                )
                return

            # Hash the image
            hash_value = await self.get_hash(tmp_path)
            if hash_value is None:
                image_check.unknown = True
                await self.finish_image_check(
                    group_id,
                    user,
                    media_group_id,
                    image_checks,
                )
                return

            # Match this hash against the our database
            matches = await self.matching.find_hash_matches(hash_value)

            # Scan any e621 matches for banned tags
            for match in matches:
                if match.site == "e621":
                    banned_tags = await self.has_banned_e621_tags(group_id, match.id)
                    if banned_tags:
                        # Remove the image and send a message
                        image_check.deleted = banned_tags
                        try:
                            await self.bot.bot.delete_message(
                                chat_id=group_id,
                                message_id=message_id,
                            )
                        except Exception as e:
                            log.error(f"Failed to delete message: {e}")

                        await self.finish_image_check(
                            group_id, user, media_group_id, image_checks
                        )
                        return

            if not matches:
                image_check.unknown = True
            else:
                image_check.results = matches

            await self.finish_image_check(
                group_id,
                user,
                media_group_id,
                image_checks,
            )

    async def finish_image_check(
        self,
        group_id: int,
        user: User,
        media_group_id: str | None,
        image_checks: list[ImageCheck],
    ):
        # Check if all checks are complete
        if not all(ic.is_finished for ic in image_checks):
            return

        if media_group_id:
            self.ongoing_image_checks[group_id].pop(media_group_id, None)

        # Are there any deleted message?
        # Collect all the banned tags that were found
        banned_tags: set[str] = set()
        amount_deleted: int = 0
        for ic in image_checks:
            if ic.deleted:
                banned_tags.update(ic.deleted)
                amount_deleted += 1

        if amount_deleted > 0:
            if len(banned_tags) == 1:
                reason = f"because '{banned_tags.pop()}' is not allowed in this group."
            elif len(banned_tags) == 2:
                reason = f"because '{' and '.join(banned_tags)}' are not allowed in this group."
            else:
                tags_list = list(banned_tags)
                reason = f"because '{', '.join(tags_list[:-1])}, and {tags_list[-1]}' are not allowed in this group."

            if len(image_checks) == 1:
                msg = f"❌ {mention_html(user)}: Unfortunately, this image was removed {reason}"
            else:
                msg = f"❌ {mention_html(user)}: Unfortunately, {pluralize(amount_deleted, 'image was', 'images were')} removed {reason}"

            # Send the message
            await self.bot.bot.send_message(
                chat_id=group_id,
                text=msg,
                parse_mode="HTML",
            )

        message: list[str] = []
        for index, ic in enumerate([ic for ic in image_checks if not ic.deleted]):
            if ic.results is None:
                continue

            if len(message) > 0:
                message.append("")  # Blank line between images

            single_result: list[str] = []

            if len(image_checks) == 1:
                if len(ic.results) == 1:
                    single_result.append("Found this match:")
                else:
                    single_result.append("Found these matches:")

            if len(image_checks) > 1:
                if len(ic.results) == 1:
                    single_result.append(f"Match for image #{index + 1}:")
                else:
                    single_result.append(f"Matches for image #{index + 1}:")

            # Get the results sorted by timestamp (first result is the oldest)
            results = sorted(ic.results, key=lambda r: r.posted_at)
            for result in results:
                if result.match == 0:
                    likeness = ""
                else:
                    likeness = " <i>(close match)</i>"
                single_result.append(
                    f"By <b>{result.artist}</b>: {self.convert_to_url(result.site, result.id)}{likeness}"
                )

            message.append("\n".join(single_result))

        if len(message) != 0:
            # Find the one with the lowest message ID to reply to
            reply_to_message_id = min(
                ic.message_id for ic in image_checks if ic.results
            )
            await self.bot.bot.send_message(
                chat_id=group_id,
                text="\n".join(message),
                reply_to_message_id=reply_to_message_id,
                disable_web_page_preview=True,
                parse_mode="HTML",
            )

    async def has_banned_e621_tags(self, group_id: int, id: int) -> list[str]:
        if config := self.configs.get(group_id):
            forbidden_tags = set(config.forbidden_tags)
            if not forbidden_tags:
                return []

            post_tags = await self.fetch_e621_tags(id)
            found_tags = forbidden_tags.intersection(post_tags)
            return list(found_tags)
        return []

    async def fetch_e621_tags(self, id: int) -> list[str]:
        """Fetch tags for an e621 post using the e621 API"""
        async with aiohttp.ClientSession(
            headers={"User-Agent": "Watchdog/1.0"}
        ) as session:
            async with session.get(f"https://e621.net/posts/{id}.json") as resp:
                if resp.status != 200:
                    log.error(f"Failed to fetch e621 post {id}: HTTP {resp.status}")
                    return []
                data = await resp.json()
                tags = data.get("post", {}).get("tags", {}).get("general", [])
                return tags

    def convert_to_url(self, site: str, id: int) -> str:
        if site == "e621":
            return f"<a href='https://e621.net/posts/{id}'>e621</a>"
        elif site == "furaffinity":
            return f"<a href='https://www.furaffinity.net/view/{id}/'>Furaffinity</a>"
        elif site == "weasyl":
            return f"<a href='https://www.weasyl.com/view/{id}'>Weasyl</a>"
        else:
            return f"{site} #{id}"

    def botadmin_get_enabled(self, group_id: int) -> bool:
        if config := self.configs.get(group_id):
            return config.enabled
        return False

    async def botadmin_set_enabled(self, group_id: int, value: bool) -> None:
        if config := self.configs.get(group_id):
            config.enabled = value
        else:
            self.configs[group_id] = Config(enabled=value)

        await self.db.set_app_config("imagesearch", group_id, self.configs[group_id])

        if value:
            self.add_group_register(group_id)
        else:
            self.remove_group_register(group_id)

    def botadmin_get_forbidden_tags(self, group_id: int) -> str:
        if config := self.configs.get(group_id):
            return " ".join(config.forbidden_tags)
        return ""

    async def botadmin_set_forbidden_tags(self, group_id: int, value: str) -> None:
        tags = value.split()
        if config := self.configs.get(group_id):
            config.forbidden_tags = tags
        else:
            self.configs[group_id] = Config(forbidden_tags=tags)

        await self.db.set_app_config("imagesearch", group_id, self.configs[group_id])
