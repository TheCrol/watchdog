from dataclasses import dataclass

SELFTEST_HASH = -3722692567699475621
CHECK_DB_EVERY = 60 * 60  # Check the database every hour
GET_DB_DUMP_URL = "https://api-next.fuzzysearch.net/v1/dump/latest"
DEFAULT_BANNED_TAGS = ["cub", "young"]
HASH_MATCH_THRESHOLD = (
    3  # Minimum number of bits that must match to be considered a match
)


@dataclass
class ImageCheckResult:
    site: str
    artist: str
    id: int
    posted_at: int
    match: int


@dataclass
class ImageCheck:
    message_id: int
    caption: str | None
    results: None | list[ImageCheckResult] = None
    deleted: None | list[str] = None  # list of banned tags that caused deletion
    unknown: bool = False  # No match found

    @property
    def is_finished(self) -> bool:
        return self.results is not None or self.deleted is not None or self.unknown
