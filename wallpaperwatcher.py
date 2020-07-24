#!/usr/bin/env python3
import logging
from typing import List, Dict, Optional, Any, Set
from dataclasses import dataclass, field
import asyncio
import os
import json
import jsons
import sys
import asyncio
from pathlib import Path
import argparse
import signal

import tornado.ioloop
import tornado.locks
import praw
from praw.models.reddit.subreddit import SubredditStream
from praw.models.reddit.submission import Submission
from praw.models.util import stream_generator
import yaml

import downloader

logger: logging.Logger
logger = logging.getLogger("wallpaperwatcher")

@dataclass
class RedditAuthInfo():
    """ Holds Reddit Authentication Values """
    client_id: str
    client_secret: str

    @staticmethod
    def from_file(filename:str) -> "RedditAuthInfo":
        with open(filename, "r") as f:
            auth = jsons.load(yaml.load(f, Loader=yaml.BaseLoader), RedditAuthInfo)
        return auth

@dataclass
class Size():
    """ Holds a size """
    width: int
    height: int

@dataclass
class SubredditConfig():
    """ Holds per subreddit configuration data like target folders """
    folder: str
    # This space left blank for future expansion

@dataclass
class WallpaperConfig():
    """ Holds the configuration including subreddits and image qualifications """
    subreddits: Dict[str, Optional[SubredditConfig]]
    target_size: Size
    aspect_ratio_tolerance: float
    max_downloads: int
    update_interval: int

    @staticmethod
    def from_file(filename:str) -> "WallpaperConfig":
        with open(filename, "r") as f:
            return jsons.load(yaml.load(f, Loader=yaml.BaseLoader), WallpaperConfig)

@dataclass
class RedditImage():
    """ Holds information about an image offered by reddit """
    url: str
    width: int
    height: int

client: praw.Reddit
config: WallpaperConfig
sub_streams: Dict[str, stream_generator]
target_aspect_ratio: float
downloads_sym: tornado.locks.Semaphore
cmdline_args: argparse.Namespace

USER_AGENT = "python:com.jimthecactus.wallpaperwatcher:v0.0.1 (by /u/PM_ME_YOUR_BEST_MIO)"

def polite_print(*args, **kwargs):
    """Proxy for print that is muted if we've been asked to be quiet"""
    if not cmdline_args.quiet:
        print(*args, **kwargs)

def die(*args, **kwargs):
    """Emits a critical log and exits with a non-zero status. All arguments will be
    passed to logger.critical."""
    logger.critical(*args, **kwargs)
    exit(1)

async def process_post(post: Submission, subreddit: str, options: SubredditConfig) -> List[asyncio.Task]:
    """Processes submissions retrieved from reddit, identifies any
    appropriate images, and downloads them. Returns a list of download
    tasks to be awaited."""

    logger.info("Processing post r/%s: %s", post.subreddit.display_name, post.title, )

    downloads = []

    # Catch the obvious thing where we are looking at something that we can't handle.
    try:
        preview = post.preview
        images = preview['images']
    except AttributeError:
        logger.info("No images found.")
        return downloads

    for image in images:
        source = jsons.load(image['source'], RedditImage)
        logger.info("Found an image (%d x %d): %s", source.width, source.height, source.url)

        if source.height < config.target_size.height:
            continue
        if source.width < config.target_size.width:
            continue

        # Compute the aspect ration
        aspect_ratio = float(source.width)/float(source.height)
        # And determine if it is in tolerances
        if abs((aspect_ratio-target_aspect_ratio)/aspect_ratio) > config.aspect_ratio_tolerance:
            continue

        logger.info("Image is suitable for wallpaper!")
        if options is not None:
            folder = options.folder
        else:
            folder = "."

        # Start a download but don't await it. Just catch the future for later.
        downloads.append(asyncio.create_task(do_download(source.url, folder)))

    return downloads

async def do_download(url: str, directory: str) -> None:
    """Waits for a free download slot to open up and downloads an image"""
    try:
        # Grab a handle on our download throttle
        async with downloads_sym:
            # Since it's our turn, download the image
            filename = await downloader.download_image(url, directory)

        polite_print(f"Downloaded '{filename}'.")
        logger.info("Downloaded '%s'", filename)
    except Exception as exc:
        logger.error("Unable to download 'url'.", exc_info=exc)

async def fetch_latest() -> None:
    """Fetches any pending posts and downloads any appropriate images"""
    global sub_streams
    global client

    try:
        sub_stream: stream_generator
        downloads = []
        for subreddit, sub_stream in sub_streams.items():
            logger.info("Fetching posts for %s", subreddit)

            try:
                post: Submission = next(sub_stream)
                while post is not None:
                    # We want to fail nicely per post.
                    try:
                        downloads += await process_post(post, subreddit, config.subreddits[subreddit])
                    except Exception as exc:
                        print("Uhhhh?")
                        logger.error("Failed to process post '%s'.", post.title, exc_info=exc)

                    post = next(sub_stream)
            except Exception as exc:
                logger.error("Failed to process subreddit '%s'.", subreddit, exc_info=exc)

        # If we have any pending downloads
        if len(downloads) > 0:
            # Make sure they all finish before we say we're ready to go again.
            await asyncio.wait(downloads)
    except Exception as exc:
        logger.error("Error fetching/processing posts! Not all posts may have been checked!", exc_info=exc)

async def fetch_and_repeat():
    """ Does a preliminary fetch and then repeats the fetch on a
    regularinterval """
    logger.info("Doing initial fetch...")
    # Do an initial fetch
    await fetch_latest()

    logger.info("Registering timed fetch...")
    interval = tornado.ioloop.PeriodicCallback(
        callback=fetch_latest,
        callback_time=config.update_interval,
        jitter=0.1)
    interval.start()
    logger.info("Startup complete.")


async def main() -> None:
    """Configures the system and launches the watcher loops"""
    global client
    global config
    global sub_streams
    global target_aspect_ratio
    global downloads_sym

    try:
        signal.signal(signal.SIGTERM, handle_sigterm)

        logger.info("Loading Configuration...")
        config = WallpaperConfig.from_file(cmdline_args.config_file)
        logger.debug("Loaded: %s", config)

        logger.info("Loading Auth Tokens...")
        auth_info = RedditAuthInfo.from_file(cmdline_args.auth_file)
        logger.debug("Loaded: %s", auth_info)

        logger.info("Initializing...")

        target_aspect_ratio = float(config.target_size.width)/float(config.target_size.height)
        logger.debug("Computed aspect ratio of %0.3f", target_aspect_ratio)
        downloads_sym = tornado.locks.Semaphore(config.max_downloads)

        client = praw.Reddit(
            client_id=auth_info.client_id,
            client_secret=auth_info.client_secret,
            user_agent=USER_AGENT)

        logger.info("Starting streams...")
        sub_streams = {}
        for subreddit in config.subreddits:
            sub_streams[subreddit] = client.subreddit(subreddit).stream.submissions(pause_after=-1)

        logger.info("Launching main loop...")
        tornado.ioloop.IOLoop.current().add_callback(fetch_and_repeat)
        polite_print("Wallpaper Downloader Started.")

    except Exception as exc:
        die("FATAL! Error initializing!", exc_info=exc)

async def shutdown():
    # Do a shutdown
    logger.warning("Shutting Down...")
    tornado.ioloop.IOLoop.current().stop()

def handle_sigterm(signum, sighandler):
    # Register a callback inside the main loop
    io_loop.add_callback_from_signal(shutdown)

def parse_cmd_args() -> argparse.Namespace:
    """ Parses out the arguments understood by the program """
    parser = argparse.ArgumentParser(
        description="Scans a number of subreddits for changes and downloads images"
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
    if os.name=="nt":
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
