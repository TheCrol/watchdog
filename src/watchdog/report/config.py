from pydantic import BaseModel


class GroupConfig(BaseModel):
    enabled: bool = False


class Config(BaseModel):
    groups: dict[int, GroupConfig] = {}
