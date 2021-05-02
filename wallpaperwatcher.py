#!/usr/bin/env python3
"""
wallpaperwatcher.py
By: John-Michael O'Brien
Date: 7/18/2020

This script will monitor a number of subreddits and multi reddits and
produce a set of files in specified folders for posts with images that
match, within some tolerance the aspect ratio of a target resolution
and are the same as, or bigger than, that specified resolution.

This is great for collecting wallpapers from Reddit on a continuous
basis.
"""


import logging
from typing import List, Dict, Set, Generator, Tuple, Optional
from dataclasses import dataclass, field
import os
import sys
import asyncio
import argparse
import signal
import shutil
import hashlib
import sqlite3
from pathlib import PurePath, Path
from tempfile import NamedTemporaryFile

import tornado.ioloop
import tornado.locks
import praw
from praw.models.reddit.submission import Submission
from praw.models.util import stream_generator
import jsons

from FileDownload import FileDownload
from SafeFilename import SafeFilename
from config_objects import WallpaperConfig, RedditAuthInfo, TargetConfig

logger: logging.Logger
logger = logging.getLogger("wallpaperwatcher.WallpaperWatcher")

USER_AGENT = "python:com.jimthecactus.wallpaperwatcher:0.0.2 (by /u/PM_ME_YOUR_BEST_MIO)"

@dataclass
class RedditImage():
    """ Holds information about an image offered by reddit """
    url: str
    width: int
    height: int
    aspect_ratio: float = field(init=False, repr=False)
    def __post_init__(self):
        self.aspect_ratio = float(self.width) / float(self.height)

class WallpaperWatcher():
    _client: praw.Reddit
    _config: WallpaperConfig
    _downloads_sym: tornado.locks.Semaphore
    _io_loop: tornado.ioloop.IOLoop
    _running: bool
    _started: bool
    _sources: Dict[str, stream_generator]
    _targets_by_source: Dict[str, List[TargetConfig]]
    _interval: tornado.ioloop.PeriodicCallback
    _conn: sqlite3.Connection

    def __init__(self, credentials: RedditAuthInfo, config: WallpaperConfig):
        # Create an IO Loop
        self._running = False
        self._started = False
        self._io_loop = tornado.ioloop.IOLoop.current()
        self._config = config
        self._client = praw.Reddit(
            client_id=credentials.client_id,
            client_secret=credentials.client_secret,
            user_agent=USER_AGENT)
        self._downloads_sym = tornado.locks.Semaphore(config.max_downloads)
        self._sources = {}
        self._targets_by_source = {}
        self.interval = tornado.ioloop.PeriodicCallback(
            callback=self._update,
            callback_time=self._config.update_interval,
            jitter=0.1)
        Path("data").mkdir(parents=True, exist_ok=True)
        db_location = config.database_file
        self._conn = sqlite3.connect(db_location)
        cur = self._conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS filehashes (
                hash text NOT NULL,
                target text NOT NULL,
                filename text NOT NULL,
                PRIMARY KEY (hash, target)
            )
        """)
        self._conn.commit()

    def start(self):
        if self._started:
            raise ValueError("Cannot start. Already started.")
        self._started = True
        # Register our entrypoint
        self._io_loop.add_callback(self._async_init)
        # And get this party started.
        self._io_loop.start()

    async def _shutdown(self) -> None:
        """ Stops the monitoring process """
        # Do a shutdown
        logger.warning("Shutting Down...")
        self._io_loop.stop()

    def stop_from_signal(self):
        self._io_loop.add_callback_from_signal(self._shutdown)

    def stop(self):
        self._io_loop.add_callback(self._shutdown)

    def _check_for_missing_and_orphan_sources(self):
        """ Verifies that every source is used at least once and that no
        sources are referenced by a target that aren't declared in sources. """

        # Copy the source list into the reference list, checking for dupes along the way.
        loaded_sources: Set[str] = set()
        unused_sources: Set[str] = set()
        for source in self._sources:
            if source in loaded_sources:
                raise ValueError(f"Duplicate source name '{source}' found!")
            loaded_sources.add(source)
            unused_sources.add(source)


        # Go through all of our sources
        for target_name, target in self._config.targets.items():
            # If a source is used in the target that isn't in the loaded sources.
            if not loaded_sources.issuperset(target.sources):
                raise ValueError(f"Source(s) {set(target.sources).difference(loaded_sources)}"\
                    + f" referenced in target '{target_name}' don't exist.")
            # Remove any sources we used in this target from our unused list.
            unused_sources.difference_update(target.sources)

        if unused_sources:
            raise ValueError(f"Source(s) {unused_sources} are never used!")


    async def _async_init(self):
        logger.info("Starting streams...")
        for subreddit_name in self._config.sources.subreddits:
            self._sources[subreddit_name] = self._client.subreddit(subreddit_name). \
                                      stream.submissions(pause_after=-1)

        for source_name, multi in self._config.sources.multis.items():
            self._sources[source_name] = self._client.multireddit(multi.user, multi.multi). \
                                   stream.submissions(pause_after=-1)

        self._check_for_missing_and_orphan_sources()

        # Initialize the source list
        for source in self._sources:
            self._targets_by_source[source] = []

        for target_name, target in self._config.targets.items():
            for source in target.sources:
                self._targets_by_source[source].append((target_name, target))


        logging.info("Starting initial update...")
        #await self._update(skip_existing=True)
        await self._update()

        self.interval.start()

        self._running = True

        logger.info("Started.")

    @staticmethod
    def _fetch_posts_from_streams(sources: Dict[str, stream_generator]) -> Generator[Tuple[str, Submission], None, None]:
        try:
            source_name: str
            source: stream_generator

            for source_name, source in sources.items():
                logger.info("Fetching posts for source '%s'", source_name)

                try:
                    post: Submission = next(source)
                    while post is not None:
                        yield (source_name, post)
                        post = next(source)
                except Exception as exc: # pylint: disable=broad-except
                    # We want exceptions here to be non-fatal
                    logger.error("Failed to process source '%s'.", source_name, exc_info=exc)

        except Exception as exc: # pylint: disable=broad-except
            # We want exceptions here to be non-fatal
            logger.error("Error fetching/processing posts! Not all posts may have been checked!",
                        exc_info=exc)

    @staticmethod
    def _get_images_from_post(post: Submission) -> Generator[RedditImage, None, None]:
        """ Processes submissions retrieved from reddit, identifies any
        appropriate images, and downloads them. Returns a list of download
        tasks to be awaited. """

        logger.info("Processing post in r/%s: %s", post.subreddit.display_name, post.title)

        # Catch the obvious thing where we are looking at something that we can't handle.
        try:
            preview = post.preview
            images = preview['images']
        except AttributeError:
            logger.info("No images found.")
            return

        for image in images:
            source_image = jsons.load(image['source'], RedditImage)
            logger.info("Found an image (%d x %d): %s",
                        source_image.width,
                        source_image.height,
                        source_image.url)
            yield source_image

    @staticmethod
    def _is_image_suitable(image: RedditImage, target: TargetConfig, nsfw: bool, tolerance: float) -> bool:
        if image.height < target.size.height:
            logger.debug("Wrong height. Want %d, found %d", image.height, target.size.height)
            return False
        if image.width < target.size.width:
            logger.debug("Wrong width. Want %d, found %d", image.width, target.size.width)
            return False
        aspect_error = abs((image.aspect_ratio-target.size.aspect_ratio)/target.size.aspect_ratio)
        # And determine if it is in tolerances
        if aspect_error > tolerance:
            logger.debug("Aspect ratio out of range. Want < %0.3f, found %0.3f", tolerance, aspect_error)
            return False
        if nsfw and not target.allow_nsfw:
            logger.debug("Image is NSFW which is denied")
            return False
        return True

    @staticmethod
    async def _compute_file_hash(source_file: Path):
        """ Computes the SHA256 hash of a file and returns it's hex string """
        CHUNK_SIZE = 65536
        hasher = hashlib.sha256()
        with source_file.open("rb") as fd:
            chunk = fd.read(CHUNK_SIZE)
            while chunk:
                hasher.update(chunk)
                # Yield to allow other things to work and not hog the CPU.
                await asyncio.sleep(0)
                chunk = fd.read(CHUNK_SIZE)
        return hasher.hexdigest()


    @staticmethod
    async def _copy_to_destination(source_file: str, directory: str,
                                prefix: str, suffix: str, skip_existing: bool = False,
                                source_hash: Optional[str] = None) -> Optional[str]:
        """ Copies a file to a number of destination folders """
        # start with the directory we were given
        dirpath = Path(directory)
        # Make sure it exists.
        dirpath.mkdir(parents=True, exist_ok=True)

        # Build up the final image name
        location = dirpath / f"{prefix}{suffix}"

        # Check if that file already exists.
        if location.exists():
            # If we've been told to just skip on duplicates, we can stop now.
            if skip_existing:
                logger.info("Some file already exists and we were told to skip existing filenames.")
                return None

            if source_hash and source_hash == await WallpaperWatcher._compute_file_hash(location):
                logger.info("Exact same file already exists. Skipping.")
                return None

            logger.warning("File '%s' already exists. File will be renamed.", str(location))
            # try infixes until we find a good filename.
            # This is non-atomic and unsafe. Don't go playing in the folder while we do this.
            count = 1
            while True:
                location = dirpath / f"{prefix} ({count}){suffix}"
                if not location.exists():
                    break
                count += 1
        # And copy our temporary file into it's final resting place.
        logger.debug("Copying from '%s' to '%s'", source_file, str(location))
        shutil.copyfile(source_file, str(location))

        return location

    async def _download_file_to_targets(self, url: str, targets: Dict[str, TargetConfig],
                                        skip_existing: bool):
        """ Waits for a free download slot to open up and downloads an image """
        try:
            # Grab a handle on our download throttle
            async with self._downloads_sym:
                with NamedTemporaryFile("wb", delete=False) as spool_file:
                    try:
                        # Prep a download
                        pending_download = FileDownload(url, spool_file)
                        # and wait for it to finish
                        result = await pending_download.start()

                        # Close up the file so we can copy it.
                        spool_file.close()

                        polite_print(f"Downloaded {url}'.")
                        logger.info("Downloaded %s", url)

                        filename = SafeFilename(url, result.headers['Content-type'])

                        for target_name, target in targets.items():
                            cur = self._conn.cursor()
                            cur.execute("SELECT filename FROM filehashes WHERE hash=? AND target=?;",
                                        (pending_download.get_hash(), target_name))
                            result = cur.fetchone()
                            if result is not None:
                                logger.info("File has been downloaded before as '%s'", str(result[0]))
#                                if Path(result[0]).exists():
#                                    logger.info("Skiping since it already exists.")
#                                    continue
                                continue

                            filename = await self._copy_to_destination(
                                spool_file.name,
                                target.path,
                                filename.prefix,
                                filename.suffix,
                                source_hash=pending_download.get_hash(),
                                skip_existing=skip_existing)

                            if filename is not None:
                                cur.execute("INSERT INTO filehashes(hash, target, filename) values (?,?,?);",
                                            (pending_download.get_hash(), target_name, str(filename)))

                            logger.info("Saved file as %s", filename)
                    finally:
                        if not spool_file.closed:
                            spool_file.close()
                        os.unlink(spool_file.name)
                        self._conn.commit()
        except Exception as exc: # pylint: disable=broad-except
            # We want exceptions here to be non-fatal
            logger.error("Unable to download 'url'.", exc_info=exc)


    async def _update(self, skip_existing: bool=False):
        downloads: List[asyncio.Task] = []

        # Get posts from sources
        for source_name, post in self._fetch_posts_from_streams(self._sources):
            image: RedditImage
            # Get images from posts
            for image in self._get_images_from_post(post):
                url: str = image.url
                targets: Dict[str, TargetConfig] = {}
                # Get the targets for that source
                for target_name, target in self._targets_by_source[source_name]:
                    # Check the image against the targets
                    if WallpaperWatcher._is_image_suitable(image, target, post.over_18, self._config.aspect_ratio_tolerance):
                        targets[target_name]=target
                # If nobody wants this image, skip the download.
                if len(targets) == 0:
                    continue
                logger.info(f"Image is appropriate for {targets.keys()}")
                downloads.append(self._download_file_to_targets(url, targets, skip_existing))

        if downloads:
            await asyncio.wait(downloads)


def polite_print(*args, **kwargs) -> None:
    """ Proxy for print that is muted if we've been asked to be quiet """
    if not cmdline_args.quiet:
        print(*args, **kwargs)

# We don't use the signals ourselves.
def handle_sigterm(signum, sighandler) -> None: # pylint: disable=unused-argument
    """ Handles the SIGTERM signal to stop. """
    # Register a callback inside the main loop
    my_watcher.stop_from_signal()

def parse_cmd_args() -> argparse.Namespace:
    """ Parses out the arguments understood by the program """
    parser = argparse.ArgumentParser(
        description="Scans a number of subreddits for changes and downloads images" \
                    + " that match the criteria of a wallpaper")
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "-v", "--verbose",
        dest="verbosity",
        action="count",
        default=0,
        help="Enables verbose output. Use more 'v's for more verbosity (up to -vvv)")
    verbosity.add_argument(
        "-q", "--quiet",
        dest="quiet",
        action="store_true",
        help="Disables all output except for critical and error level logs.")
    parser.add_argument(
        "--config",
        dest="config_file",
        default="wallpaper_config.yaml",
        help="Specifies a configuration file. 'wallpaper_config.yaml' will be used otherwise.")
    parser.add_argument(
        "--auth",
        dest="auth_file",
        default="auth_info.yaml",
        help="Specifies an authorization key file. 'auth_info.yaml' will be used otherwise.")
    parser.add_argument(
        "--log",
        dest="log_file",
        help="Specifies the target log file. By default log output will go to stdout."
    )
    return parser.parse_args()

if __name__ == "__main__":
    cmdline_args = parse_cmd_args()

    # Windows workaround for Python 3.8 and Tornado 6.0.4.
    # See https://www.tornadoweb.org/en/stable/index.html#installation
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Setup our logging
    logging.basicConfig(filename=cmdline_args.log_file)
    global_logger = logging.getLogger("wallpaperwatcher")
    if cmdline_args.verbosity == 1:
        global_logger.setLevel(logging.INFO)
    elif cmdline_args.verbosity == 2:
        global_logger.setLevel(logging.DEBUG)
    elif cmdline_args.verbosity > 2:
        # Super extra debug level logging. Log EVERYTHING.
        logging.getLogger().setLevel(logging.DEBUG)
    else: # args.verbosity <= 0:
        global_logger.setLevel(logging.WARNING)

    # If we're supposed to be quiet
    if cmdline_args.quiet:
        # And we're not logging to a file
        if cmdline_args.log is None:
            # Limit our output to only fatal stuff.
            global_logger.setLevel(logging.CRITICAL)

    global_logger.info("Initializing.")

    signal.signal(signal.SIGTERM, handle_sigterm)

    global_logger.info("Loading Configuration...")
    loaded_config = WallpaperConfig.from_file(cmdline_args.config_file)
    global_logger.debug("Loaded: %s", loaded_config)

    global_logger.info("Loading Auth Tokens...")
    auth_info = RedditAuthInfo.from_file(cmdline_args.auth_file)
    global_logger.debug("Loaded: %s", auth_info)

    my_watcher = WallpaperWatcher(auth_info, loaded_config)
    my_watcher.start()

    polite_print("Wallpaper Downloader Stopped.")
