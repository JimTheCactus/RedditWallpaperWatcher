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
from typing import List, Dict
from dataclasses import dataclass
import os
import sys
import asyncio
import argparse
import signal

import tornado.ioloop
import tornado.locks
import praw
from praw.models.reddit.submission import Submission
from praw.models.util import stream_generator
import jsons

from downloader import download_image
from config_objects import WallpaperConfig, RedditAuthInfo

logger: logging.Logger
logger = logging.getLogger("wallpaperwatcher")

@dataclass
class RedditImage():
    """ Holds information about an image offered by reddit """
    url: str
    width: int
    height: int

client: praw.Reddit
config: WallpaperConfig
sources: Dict[str, stream_generator]
downloads_sym: tornado.locks.Semaphore
cmdline_args: argparse.Namespace

USER_AGENT = "python:com.jimthecactus.wallpaperwatcher:v0.0.1 (by /u/PM_ME_YOUR_BEST_MIO)"

def polite_print(*args, **kwargs) -> None:
    """ Proxy for print that is muted if we've been asked to be quiet """
    if not cmdline_args.quiet:
        print(*args, **kwargs)

async def do_download(url: str, dest_directories: List[str], skip_existing: bool) -> None:
    """ Waits for a free download slot to open up and downloads an image """
    try:
        # Grab a handle on our download throttle
        async with downloads_sym:
            # Since it's our turn, download the image
            filenames = await download_image(url, dest_directories, skip_existing=skip_existing)

        polite_print(f"Downloaded {filenames}'.")
        logger.info("Downloaded %s", filenames)
    except Exception as exc: # pylint: disable=broad-except
        # We want exceptions here to be non-fatal
        logger.error("Unable to download 'url'.", exc_info=exc)

async def process_post(post: Submission, source_name: str,
                      skip_existing: bool) -> List[asyncio.Task]:
    """ Processes submissions retrieved from reddit, identifies any
    appropriate images, and downloads them. Returns a list of download
    tasks to be awaited. """

    logger.info("Processing post in r/%s: %s", post.subreddit.display_name, post.title)

    downloads = []

    # Catch the obvious thing where we are looking at something that we can't handle.
    try:
        preview = post.preview
        images = preview['images']
    except AttributeError:
        logger.info("No images found.")
        return downloads

    for image in images:
        source_image = jsons.load(image['source'], RedditImage)
        logger.info("Found an image (%d x %d): %s",
                    source_image.width,
                    source_image.height,
                    source_image.url)

        # Compute the aspect ration
        aspect_ratio = float(source_image.width)/float(source_image.height)

        destinations = {}
        for target_name, target in config.targets.items():
            if source_name not in target.sources:
                continue
            if source_image.height < target.size.height:
                continue
            if source_image.width < target.size.width:
                continue
            # And determine if it is in tolerances
            if abs((aspect_ratio-target.size.aspect_ratio)/target.size.aspect_ratio) \
                    > config.aspect_ratio_tolerance:
                continue
            if post.over_18 and not target.allow_nsfw:
                logger.debug("Image is NSFW. %s doesn't allow NSFW images. Skipping.", target_name)
                continue
            destinations[target_name] = target.path

        if destinations:
            logger.info("Image is suitable for %s!", ",".join(destinations.keys()))

            # Start a download but don't await it. Just catch the future for later.
            downloads.append(
                asyncio.create_task(
                    do_download(source_image.url, destinations.values(), skip_existing)
                )
            )

    return downloads

async def fetch_latest(skip_existing: bool = False) -> None:
    """ Fetches any pending posts and downloads any appropriate images """

    try:
        source_name: str
        source: stream_generator
        downloads = []

        for source_name, source in sources.items():
            logger.info("Fetching posts for source '%s'", source_name)

            try:
                post: Submission = next(source)
                while post is not None:
                    # We want to fail nicely per post.
                    try:
                        downloads += await process_post(post, source_name, skip_existing)
                    except Exception as exc:  # pylint: disable=broad-except
                        # We want exceptions here to be non-fatal
                        logger.error("Failed to process post '%s'.", post.title, exc_info=exc)

                    post = next(source)
            except Exception as exc: # pylint: disable=broad-except
                # We want exceptions here to be non-fatal
                logger.error("Failed to process source '%s'.", source_name, exc_info=exc)

        # If we have any pending downloads
        if len(downloads) > 0:
            # Make sure they all finish before we say we're ready to go again.
            await asyncio.wait(downloads)

    except Exception as exc: # pylint: disable=broad-except
        # We want exceptions here to be non-fatal
        logger.error("Error fetching/processing posts! Not all posts may have been checked!",
                     exc_info=exc)

async def fetch_and_repeat() -> None:
    """ Does a preliminary fetch and then repeats the fetch on a
    regularinterval """
    logger.info("Doing initial fetch...")
    # Do an initial fetch
    await fetch_latest(skip_existing=True)

    logger.info("Registering timed fetch...")
    interval = tornado.ioloop.PeriodicCallback(
        callback=fetch_latest,
        callback_time=config.update_interval,
        jitter=0.1)
    interval.start()
    logger.info("Startup complete.")

def check_for_missing_and_orphan_sources():
    """ Verifies that every source is used at least once and that no
    sources are referenced by a target that aren't declared in sources. """
    loaded_sources = set()
    if config.sources.subreddits:
        loaded_sources |= set(config.sources.subreddits.keys())
    if config.sources.multis:
        loaded_sources |= set(config.sources.multis.keys())
    if len(loaded_sources) < 1:
        raise ValueError(f"No sources found in the configuration.")

    unused_sources: set = loaded_sources.copy()

    # Go through all of our sources
    for target_name, target in config.targets.items():
        # If a source is used in the target that isn't in the loaded sources.
        if not loaded_sources.issuperset(target.sources):
            raise ValueError(f"Source(s) {set(target.sources).difference(loaded_sources)}"\
                + f" referenced in target '{target_name}' don't exist.")
        # Remove any sources we used in this target from our unused list.
        unused_sources.difference_update(target.sources)

    if unused_sources:
        raise ValueError(f"Source(s) {unused_sources} are never used!")

async def shutdown() -> None:
    """ Stops the monitoring process """
    # Do a shutdown
    logger.warning("Shutting Down...")
    tornado.ioloop.IOLoop.current().stop()

# We don't use the signals ourselves.
def handle_sigterm(signum, sighandler) -> None: # pylint: disable=unused-argument
    """ Handles the SIGTERM signal to stop. """
    # Register a callback inside the main loop
    io_loop.add_callback_from_signal(shutdown)

async def main() -> None:
    """ Configures the system and launches the main loop """
    global client
    global config
    global sources
    global downloads_sym

    try:
        signal.signal(signal.SIGTERM, handle_sigterm)

        logger.info("Loading Configuration...")
        config = WallpaperConfig.from_file(cmdline_args.config_file)
        check_for_missing_and_orphan_sources()
        logger.debug("Loaded: %s", config)

        logger.info("Loading Auth Tokens...")
        auth_info = RedditAuthInfo.from_file(cmdline_args.auth_file)
        logger.debug("Loaded: %s", auth_info)

        logger.info("Initializing...")

        downloads_sym = tornado.locks.Semaphore(config.max_downloads)

        client = praw.Reddit(
            client_id=auth_info.client_id,
            client_secret=auth_info.client_secret,
            user_agent=USER_AGENT)

        logger.info("Starting streams...")
        sources = {}
        if config.sources.subreddits:
            for subreddit_name in config.sources.subreddits:
                sources[subreddit_name] = client.subreddit(subreddit_name). \
                                        stream.submissions(pause_after=-1)

        if config.sources.multis:
            for source_name, multi in config.sources.multis.items():
                sources[source_name] = client.multireddit(multi.user, multi.multi). \
                                    stream.submissions(pause_after=-1)

        logger.info("Launching main loop...")
        tornado.ioloop.IOLoop.current().add_callback(fetch_and_repeat)
        polite_print("Wallpaper Downloader Started.")

    except Exception as exc: # pylint: disable=broad-except
        # When we error we want to take the whole thing down.
        logger.critical("FATAL! Error initializing!", exc_info=exc)
        sys.exit(1)

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
    if cmdline_args.verbosity == 1:
        logger.setLevel(logging.INFO)
    elif cmdline_args.verbosity == 2:
        logger.setLevel(logging.DEBUG)
    elif cmdline_args.verbosity > 2:
        # Super extra debug level logging. Log EVERYTHING.
        logging.getLogger().setLevel(logging.DEBUG)
    else: # args.verbosity <= 0:
        logger.setLevel(logging.WARNING)

    # If we're supposed to be quiet
    if cmdline_args.quiet:
        # And we're not logging to a file
        if cmdline_args.log is None:
            # Limit our output to only fatal stuff.
            logger.setLevel(logging.CRITICAL)

    logger.info("Initializing.")

    # Create an IO Loop
    io_loop = tornado.ioloop.IOLoop.current()

    # Register our entrypoint
    io_loop.add_callback(main)

    # And get this party started.
    io_loop.start()

    polite_print("Wallpaper Downloader Stopped.")
