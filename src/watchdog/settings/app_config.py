from dataclasses import dataclass
from typing import Awaitable, Callable

from ..useful import ACCESS


@dataclass
class GroupInfo:
    id: int
    name: str


@dataclass
class UserInfo:
    id: int
    name: str
    group: None | GroupInfo = None


class Answer: ...


@dataclass
class OutputAnswer(Answer):
    """Show a message, optionally followed by another answer"""

    message: str
    next_answer: None | Answer = None


@dataclass
class BackToMenuAnswer(Answer):
    """Go back to the main menu"""

    message: None | str = None


@dataclass
class InputAnswer(Answer):
    """Ask the user for text input"""

    text: str
    callback: Callable[[UserInfo, str], Awaitable[Answer]]


@dataclass
class ButtonsAnswer(Answer):
    """Show a list of buttons"""

    text: str
    buttons: list[tuple[str, Answer]]
    big_buttons: bool = False  # If true, buttons are shown in a single column


@dataclass
class ExecAnswer(Answer):
    """Execute a callback"""

    callback: Callable[[UserInfo], Awaitable[Answer]]


class SingleConfig:
    static_name: str
    access: ACCESS
    requires_group: bool = False

    async def get_button(self, user: UserInfo) -> str: ...

    async def on_button(self, user: UserInfo) -> Answer: ...


@dataclass
class AppConfig:
    button_emoji: str
    name: str
    description: str
    display_order: int
    configs: list[SingleConfig]
