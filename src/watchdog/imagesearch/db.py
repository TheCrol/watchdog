from pydantic import BaseModel

from .constants import DEFAULT_BANNED_TAGS


class GroupDB(BaseModel):
    enabled: bool = False
    forbidden_tags: list[str] = DEFAULT_BANNED_TAGS


class DB(BaseModel):
    groups: dict[int, GroupDB] = {}
    dm_enabled: bool = False
