from typing import List, Dict, Optional, Any, Set
from dataclasses import dataclass
import logging
import asyncio
import os
import json
import jsons
import sys
from pathlib import Path

import tornado.ioloop

from asyncreddit import RedditAuthInfo, RedditTokenInfo, RedditClient

logger: logging.Logger
logger = logging.getLogger("wallpaperwatcher")


@dataclass
class Size():
    x: int
    y: int

@dataclass
class WallpaperConfig():
    subreddits: List[str]
    target_size: Size
    aspect_ratio_tolerance: float

wallpaper_dir: Path
client: RedditClient
config: WallpaperConfig
last_post: Dict[str, Optional[str]]

USER_AGENT = "python:com.jimthecactus.wallpaperwatcher:v0.0.1 (by /u/PM_ME_YOUR_BEST_MIO)"

async def async_pause():
    stream = tornado.iostream.PipeIOStream(sys.stdin.fileno())
    await stream.read_until(b"\n")
    stream.close()

async def reauth():
    # Get a token
    token = await client.get_new_token()
    # And register that we want to update it 240 seconds before it expires
    tornado.ioloop.IOLoop.current().call_later(token.expires_in-240, reauth)

async def process_post(post: Dict[str, Any]) -> None:
    # TODO: Do a thing here that does stuff with post data.
    logger.info("Saw post %s", post['data']['name'])

async def fetch_latest() -> None:
    global last_post

    for subreddit in config.subreddits:
        logger.info("Fetching posts for %s", subreddit)
        # While there's still subreddits with more posts.
        while True:
            # If we have a last post on file for this subreddit, ask to start after that.
            args = {'count': 1000}
            if last_post[subreddit] is not None:
                args['before']=last_post[subreddit]

            # Fecth the posts
            result = await client.call_reddit_api_json(f"{subreddit}/new.json", **args)

            # Basic sanity check.
            if result['kind'] != "Listing":
                raise Exception("Not a listing?!")
            posts = result['data']['children']

            # If we didn't get any posts, skip processing.
            if len(posts) < 1:
                # We don't need to look at this subreddit anymore
                break

            for post in posts:
                await process_post(post)

            # Finally, update our last post record
            last_post[subreddit] = posts[0]['data']['name']

            logger.info("Before: %s, After %s", result['data']['before'],result['data']['after'])
            if result['data']['before'] == None:
                break
            else:
                tornado.ioloop.time.sleep(1)


async def login_and_start_loop():
    global client
    global wallpaper_dir
    global config
    global last_post

    client = RedditClient(USER_AGENT, RedditAuthInfo.from_file("auth_info.json"))
    with open("wallpaper_config.json", "r") as f:
        config = jsons.load(json.load(f), WallpaperConfig)
    
    # Pre-fill our last posts
    last_post={}
    for subreddit in config.subreddits:
        last_post[subreddit] = None

    # Setup our path
    wallpaper_dir = Path("wallpapers")
    wallpaper_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Logging in.")
    # Grab an auth token
    await reauth()

    logger.info("Doing initial fetch.")
    # Do an initial fetch
    await fetch_latest()

    logger.info("Done.")
    tornado.ioloop.IOLoop.current().stop()

    #logger.info("Launching main loop.")
    #interval = tornado.ioloop.PeriodicCallback(callback=fetch_latest, callback_time=60000)
    #interval.start()    


if __name__ == "__main__":
    logging.info("Initializing.")

    # Windows workaround for Python 3.8 and Tornado 6.0.4.
    # See https://www.tornadoweb.org/en/stable/index.html#installation
    if os.name=="nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Setup our logging
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

    # Create an IO Loop
    io_loop = tornado.ioloop.IOLoop.current()

    # Register our entrypoint
    io_loop.add_callback(login_and_start_loop)    

    # And get this party started.
    io_loop.start()