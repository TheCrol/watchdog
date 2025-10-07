from pydantic import BaseModel

from .constants import DEFAULT_MESSAGE


class GroupDB(BaseModel):
    enabled: bool = False
    message: str = DEFAULT_MESSAGE


class DB(BaseModel):
    groups: dict[int, GroupDB] = {}
