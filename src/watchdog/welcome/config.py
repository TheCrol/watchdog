from pydantic import BaseModel

from .constants import DEFAULT_MESSAGE


class GroupConfig(BaseModel):
    enabled: bool = False
    message: str = DEFAULT_MESSAGE


class Config(BaseModel):
    groups: dict[int, GroupConfig] = {}
