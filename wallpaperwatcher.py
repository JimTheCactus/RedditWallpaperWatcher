from typing import List, Dict, Optional, Any, Set
from dataclasses import dataclass, field
import logging
import asyncio
import os
import json
import jsons
import sys
import asyncio
from pathlib import Path

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
            auth = jsons.load(yaml.load(f), RedditAuthInfo)
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
            return jsons.load(yaml.load(f), WallpaperConfig)

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

USER_AGENT = "python:com.jimthecactus.wallpaperwatcher:v0.0.1 (by /u/PM_ME_YOUR_BEST_MIO)"

async def process_post(post: Submission, subreddit: str, options: SubredditConfig) -> List[asyncio.Task]:
    """Processes submissions retrieved from reddit, identifies any appropriate images, and downloads them.
    returns a list of download tasks to be awaited."""

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
    # Grab a handle on our download throttle
    async with downloads_sym:
        # Since it's our turn, download the image
        await downloader.download_image(url, directory)

async def fetch_latest() -> None:
    """Fetches any pending posts and downloads any appropriate images"""
    global sub_streams
    global client

    sub_stream: stream_generator
    downloads = []
    for subreddit, sub_stream in sub_streams.items():
        logger.info("Fetching posts for %s", subreddit)

        post: Submission = next(sub_stream)
        while post is not None:
            # We want to fail nicely per post.
            try:
                downloads += await process_post(post, subreddit, config.subreddits[subreddit])
            except Exception as exc:
                logger.warning("Failed to process post.", exc_info=exc)

            post = next(sub_stream)

    if len(downloads) > 0:
        await asyncio.wait(downloads)

async def main():
    """Configures the system and launches the watcher loops"""
    global client
    global config
    global sub_streams
    global target_aspect_ratio
    global downloads_sym

    logging.info("Loading Configuration")
    config = WallpaperConfig.from_file("wallpaper_config.yaml")

    auth_info = RedditAuthInfo.from_file("auth_info.yaml")
    client = praw.Reddit(client_id=auth_info.client_id, client_secret=auth_info.client_secret, user_agent=USER_AGENT)

    logging.info("Initializing.")
    target_aspect_ratio = float(config.target_size.width)/float(config.target_size.height)

    logger.info("Starting streams.")
    sub_streams = {}
    for subreddit in config.subreddits:
        sub_streams[subreddit] = client.subreddit(subreddit).stream.submissions(pause_after=-1)

    downloads_sym = tornado.locks.Semaphore(config.max_downloads)

    logger.info("Doing initial fetch.")
    # Do an initial fetch
    await fetch_latest()

    logger.info("Watching for new posts.")
    interval = tornado.ioloop.PeriodicCallback(callback=fetch_latest, callback_time=config.update_interval)
    interval.start()


if __name__ == "__main__":
    logging.info("Initializing.")

    # Windows workaround for Python 3.8 and Tornado 6.0.4.
    # See https://www.tornadoweb.org/en/stable/index.html#installation
    if os.name=="nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Setup our logging
    logging.basicConfig()
    #logger.setLevel(logging.DEBUG)
    logger.setLevel(logging.INFO)

    # Create an IO Loop
    io_loop = tornado.ioloop.IOLoop.current()

    # Register our entrypoint
    io_loop.add_callback(main)

    # And get this party started.
    io_loop.start()