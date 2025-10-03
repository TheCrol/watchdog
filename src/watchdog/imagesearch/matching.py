import array
import asyncio
import gzip
import heapq
import itertools as it
import logging
import mmap
import pickle
import struct
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO

import aiohttp
import numpy as np

from .constants import CHECK_DB_EVERY, GET_DB_DUMP_URL, ImageCheckResult

log = logging.getLogger("imagesearch.db_fetch")

if TYPE_CHECKING:
    from ..watchdog import App


@dataclass
class DBCache:
    last_checked: int = 0
    last_url: None | str = None
    is_downloading: bool = False


class Matching:
    def __init__(self, app: "App"):
        self.app = app

        self.data_folder = self.app.data_folder
        self.cache_file = self.data_folder / "cache.pkl"
        self.progress_compressed_dbdump_file = (
            self.data_folder / "dbdump-inprogress.csv.gz"
        )
        self.progress_dbdump_file = self.data_folder / "dbdump-inprogress.csv"
        self.dbdump_file = self.data_folder / "dbdump.csv"
        self.pairs_file = self.data_folder / "pairs-inprogress.bin"
        self.run_folder = self.data_folder / "runs"
        self.progress_hash_lookup_file = self.data_folder / "hash-lookup-inprogress.bin"
        self.progress_offset_lookup_file = (
            self.data_folder / "offset-lookup-inprogress.bin"
        )
        self.hash_lookup_file = self.data_folder / "hash-lookup.bin"
        self.offset_lookup_file = self.data_folder / "offset-lookup.bin"

        self.hash_memmap: None | np.memmap = None
        self.offset_memmap: None | np.memmap = None

        try:
            self.hash_memmap = np.memmap(self.hash_lookup_file, dtype="int64", mode="r")
            self.offset_memmap = np.memmap(
                self.offset_lookup_file, dtype="int64", mode="r"
            )
        except FileNotFoundError:
            pass

        self.generate_masks()

    def start(self):
        self.data_folder.mkdir(parents=True, exist_ok=True)
        self.load_dbcache()
        asyncio.create_task(self.periodic_db_check())

    def generate_masks(self):
        self.mask1 = np.fromiter((1 << i for i in range(64)), dtype=np.uint64, count=64)
        self.mask2 = np.fromiter(
            ((1 << i) | (1 << j) for i, j in it.combinations(range(64), 2)),
            dtype=np.uint64,
        )
        self.mask3 = np.fromiter(
            (
                (1 << i) | (1 << j) | (1 << k)
                for i, j, k in it.combinations(range(64), 3)
            ),
            dtype=np.uint64,
        )

    async def find_hash_matches(self, hash_value: int) -> list[ImageCheckResult]:
        return await asyncio.to_thread(self.find_hash_matches_task, hash_value)

    def find_hash_matches_task(self, hash_value: int) -> list[ImageCheckResult]:
        """Find all image checks that match a given hash value"""
        if self.hash_memmap is None or self.offset_memmap is None:
            log.warning("Hash or offset memmap not initialized")
            return []

        if not self.dbdump_file.exists():
            log.error("Database dump file does not exist")
            return []

        t_u64 = np.uint64(np.int64(hash_value))  # flip-safe
        t = np.uint64(np.int64(hash_value))

        variants = np.empty(
            1 + len(self.mask1) + len(self.mask2) + len(self.mask3), dtype=np.uint64
        )
        i = 0
        variants[i]
        variants[i] = t
        i += 1
        variants[i : i + len(self.mask1)] = t ^ self.mask1
        i += len(self.mask1)
        variants[i : i + len(self.mask2)] = t ^ self.mask2
        i += len(self.mask2)
        variants[i : i + len(self.mask3)] = t ^ self.mask3

        v_i64 = variants.view(np.int64)
        v_i64.sort()
        v_i64 = np.unique(
            v_i64
        )  # just in case (there can be overlaps in theory with signed wrap)

        # vectorized binary search for all variants at once
        left = np.searchsorted(self.hash_memmap, v_i64, side="left")
        right = np.searchsorted(self.hash_memmap, v_i64, side="right")
        hits = right > left
        if not np.any(hits):
            return []

        matches: list[ImageCheckResult] = []

        with open(self.dbdump_file, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            for left, right in zip(left[hits], right[hits]):
                for off in self.offset_memmap[left:right]:
                    mm.seek(int(off))

                    line = mm.readline().decode("utf-8", "replace").rstrip("\n")
                    parts = line.split(",")
                    site = parts[0]
                    id = int(parts[1])
                    artist = parts[2]
                    hash = int(parts[3])
                    posted_at = self.datetime_str_to_timestamp(parts[4])
                    deleted = parts[7] == "true"

                    # Get the hamming distance
                    dists = bin(t_u64 ^ np.uint64(np.int64(hash))).count("1")

                    if not deleted:
                        matches.append(
                            ImageCheckResult(
                                site=site,
                                id=id,
                                artist=artist,
                                posted_at=posted_at,
                                match=dists,
                            )
                        )

        return matches

    def datetime_str_to_timestamp(self, dt_str: str) -> int:
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"):
            try:
                return int(datetime.strptime(dt_str, fmt).timestamp())
            except ValueError:
                continue
        log.error(f"{dt_str} is not in a recognized datetime format")
        return 0

    def load_dbcache(self):
        """Load the DB cache from a pickle file"""
        try:
            data = pickle.load(open(self.cache_file, "rb"))
            self.dbcache = data
        except (FileNotFoundError, EOFError, pickle.UnpicklingError):
            log.info("No existing cache file found. Creating a new one")
            self.dbcache = DBCache()
            self.save_dbcache()

    def save_dbcache(self):
        """Save the DB cache to a pickle file"""
        with open(self.cache_file, "wb") as f:
            pickle.dump(self.dbcache, f)

    async def periodic_db_check(self):
        """Periodically check for new images to download and process"""
        while True:
            # Is it time to check for a database update?
            if self.dbcache.last_checked + CHECK_DB_EVERY < time.time():
                url = await self.get_db_dump_url()
                self.dbcache.last_checked = int(time.time())
                self.save_dbcache()
                if url is not None and url != self.dbcache.last_url:
                    # Remove any old downloading file
                    if self.dbcache.is_downloading:
                        self.progress_compressed_dbdump_file.unlink(missing_ok=True)

                    self.dbcache.last_url = url
                    self.dbcache.is_downloading = True
                    self.save_dbcache()

                    # Download a new database dump
                    await self.download_and_process_db_dump()

                elif self.dbcache.is_downloading and self.dbcache.last_url is not None:
                    # Continue downloading
                    await self.download_and_process_db_dump()

            elif self.dbcache.is_downloading and self.dbcache.last_url is not None:
                # Continue downloading
                await self.download_and_process_db_dump()

            await asyncio.sleep(CHECK_DB_EVERY)

    async def get_db_dump_url(self) -> None | str:
        """Check for a new database dump url"""

        log.debug("Checking for new DB dump URL")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(GET_DB_DUMP_URL) as resp:
                    if resp.status != 200:
                        log.error(
                            f"Failed to fetch DB dump URL. Status code: {resp.status}"
                        )
                        return

                    url = await resp.text()

                    return url
        except Exception as e:
            log.error(f"Error fetching DB dump URL: {e}")
            return

    async def download_and_process_db_dump(self):
        """Download and process the database dump"""
        if not await self.download_db_dump():
            return

        if not await self.unpack_db_dump():
            return

        await self.process_db_dump()

    async def download_db_dump(self) -> bool:
        """Download or resume downloading the database dump from the URL in the dbcache"""
        assert self.dbcache.last_url is not None

        log.info(f"Downloading database dump from {self.dbcache.last_url}")

        headers = {}
        if self.progress_compressed_dbdump_file.exists():
            existing_size = self.progress_compressed_dbdump_file.stat().st_size
            headers["Range"] = f"bytes={existing_size}-"
            log.info(f"Resuming download from byte {existing_size}")
        else:
            existing_size = 0

        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=60, sock_read=60)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.dbcache.last_url, headers=headers) as resp:
                    if resp.status == 416:
                        log.info("Database dump already fully downloaded")
                        return True

                    if resp.status not in (200, 206):
                        log.error(
                            f"Failed to download DB dump. Status code: {resp.status}"
                        )
                        return False

                    mode = "ab" if existing_size > 0 else "wb"
                    with open(self.progress_compressed_dbdump_file, mode) as f:
                        last_log_time = time.time()
                        downloaded_size = existing_size
                        total_size = (
                            int(resp.headers.get("Content-Length", 0)) + existing_size
                        )

                        async for chunk in resp.content.iter_chunked(1024 * 1024):
                            f.write(chunk)
                            downloaded_size += len(chunk)

                            # Log progress every 5 seconds
                            if time.time() - last_log_time >= 10:
                                percentage = (
                                    (downloaded_size / total_size) * 100
                                    if total_size > 0
                                    else 0
                                )
                                log.debug(
                                    f"Download progress: {downloaded_size / (1024 * 1024):.2f} MB "
                                    f"({percentage:.2f}%) downloaded"
                                )
                                last_log_time = time.time()

            log.info("Database dump download completed")

            return True

        except asyncio.TimeoutError:
            log.error("Timeout error while downloading DB dump")
            return False

        except Exception as e:
            log.error(f"Error downloading DB dump: {e}")
            return False

    async def unpack_db_dump(self) -> bool:
        """Unpack the downloaded database dump"""

        self.progress_dbdump_file.unlink(missing_ok=True)

        if not self.progress_compressed_dbdump_file.exists():
            log.error("No compressed DB dump file found to unpack")
            return False

        log.debug("Unpacking database dump")
        return await asyncio.to_thread(
            self.unpack_db_dump_task,
            self.progress_compressed_dbdump_file,
            self.progress_dbdump_file,
        )

    def unpack_db_dump_task(self, source: Path, destination: Path) -> bool:
        try:
            with gzip.open(source, "rb") as f_in:
                with destination.open("wb") as f_out:
                    while chunk := f_in.read(1024 * 1024 * 10):  # Read in 10MB chunks
                        f_out.write(chunk)

            log.debug("Database dump unpacking successfully")
            return True
        except Exception as e:
            log.error(f"Error unpacking DB dump: {e}")
            return False

    async def process_db_dump(self):
        log.debug("Processing database dump")
        if not self.progress_dbdump_file.exists():
            log.error("No unpacked DB dump file found to process")
            return

        log.debug("Converting DB dump to (hash, offset) pairs...")
        await asyncio.to_thread(
            self.process_db_pairs_task,
            self.progress_dbdump_file,
            self.pairs_file,
        )

        log.debug("Converting to K-way merge runs...")
        await asyncio.to_thread(
            self.process_db_kway_task, self.pairs_file, self.run_folder
        )

        log.debug("Merge the runs into a single sorted database file...")
        await asyncio.to_thread(
            self.process_db_merge_runs_task,
            self.run_folder,
            self.progress_hash_lookup_file,
            self.progress_offset_lookup_file,
        )

        # Move the processed files to their final names
        self.hash_lookup_file.unlink(missing_ok=True)
        self.progress_hash_lookup_file.replace(self.hash_lookup_file)

        self.offset_lookup_file.unlink(missing_ok=True)
        self.progress_offset_lookup_file.replace(self.offset_lookup_file)

        self.dbdump_file.unlink(missing_ok=True)
        self.progress_dbdump_file.replace(self.dbdump_file)

        # Cleanup intermediate files
        self.pairs_file.unlink(missing_ok=True)
        for file in self.run_folder.iterdir():
            if file.is_file():
                file.unlink()
        self.run_folder.rmdir()
        self.progress_compressed_dbdump_file.unlink(missing_ok=True)

        self.dbcache.is_downloading = False
        self.save_dbcache()

        self.hash_memmap = np.memmap(self.hash_lookup_file, dtype="int64", mode="r")
        self.offset_memmap = np.memmap(self.offset_lookup_file, dtype="int64", mode="r")

        log.debug("DB update complete")

    def process_db_pairs_task(self, dbdump_file: Path, pairs_file: Path):
        # Convert the CSV DB dump to a binary file of (hash, offset) pairs
        pairs_file.unlink(missing_ok=True)
        with (
            dbdump_file.open("rb", buffering=1024 * 1024) as f,
            pairs_file.open("wb", buffering=1024 * 1024) as out,
        ):
            off = 0
            header = f.readline()
            off += len(header)
            pack = struct.Struct("<qq").pack  # (hash, offset)

            while True:
                start = off
                line = f.readline()
                if not line:
                    break
                off += len(line)
                # super-fast hash parse: 4th column; adjust if commas appear earlier fields
                try:
                    h = int(line.decode("utf-8", "ignore").split(",", 4)[3])
                except Exception:
                    continue
                out.write(pack(h, start))

    def process_db_kway_task(self, pairs_file: Path, run_folder: Path):
        PAIR_SIZE = 16  # 8 bytes for hash + 8 bytes for offset
        PAIRS_PER_CHUNK = 5_000_000  # 80 MB chunks

        # First empty the run_folder if it exists
        if run_folder.exists():
            for file in run_folder.iterdir():
                if file.is_file():
                    file.unlink()
        else:
            run_folder.mkdir(parents=True, exist_ok=True)

        with open(pairs_file, "rb", buffering=1024 * 1024) as f:
            run_idx = 0
            while True:
                buf = f.read(PAIRS_PER_CHUNK * PAIR_SIZE)
                if not buf:
                    break
                # load into two parallel arrays without python objects
                a = array.array("q")
                a.frombytes(buf)  # length = 2 * n_pairs
                # reshape view: pairs of (hash, offset)
                n = len(a) // 2
                # build list of indices and sort by hash using Timsort (low overhead)
                idx = list(range(n))
                idx.sort(key=lambda i: a[2 * i])  # compares just the hash

                # write sorted run as raw pairs again
                run_path = run_folder / f"{run_idx:05d}.bin"
                with open(run_path, "wb", buffering=1024 * 1024) as out:
                    for i in idx:
                        out.write(a[2 * i : 2 * i + 2].tobytes())
                run_idx += 1

    def process_db_merge_runs_task(
        self, run_folder: Path, hash_lookup_file: Path, offset_lookup_file: Path
    ):
        pair = struct.Struct("<qq")
        runs: list[BinaryIO] = []
        for file in sorted(run_folder.iterdir()):
            f = file.open("rb", buffering=1024 * 1024)
            runs.append(f)

        heap: list[tuple[int, int, int]] = []
        for i, f in enumerate(runs):
            b = f.read(pair.size)
            if b:
                h, o = pair.unpack(b)
                heap.append((h, i, o))
        heapq.heapify(heap)

        with (
            hash_lookup_file.open("wb", buffering=1024 * 1024) as hh,
            offset_lookup_file.open("wb", buffering=1024 * 1024) as oo,
        ):
            while heap:
                h, i, o = heapq.heappop(heap)
                hh.write(struct.pack("<q", h))
                oo.write(struct.pack("<q", o))
                b = runs[i].read(pair.size)
                if b:
                    nh, no = pair.unpack(b)
                    heapq.heappush(heap, (nh, i, no))

        for f in runs:
            f.close()
