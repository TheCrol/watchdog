from pydantic import BaseModel


class GroupDB(BaseModel):
    enabled: bool = False


class UserDB(BaseModel):
    enabled: bool = True


class DB(BaseModel):
    groups: dict[int, GroupDB] = {}
    users: dict[int, UserDB] = {}
