from pydantic import BaseModel

from .constants import DEFAULT_BANNED_TAGS


class GroupConfig(BaseModel):
    enabled: bool = False
    forbidden_tags: list[str] = DEFAULT_BANNED_TAGS


class Config(BaseModel):
    groups: dict[int, GroupConfig] = {}
