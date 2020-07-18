import typing
import logging
import asyncio
import time
import os
import json
import contextlib
import sys
from urllib.parse import urlencode
from dataclasses import dataclass
from pathlib import Path

import tornado.ioloop
import tornado.web
import tornado.httpclient

logger: logging.Logger
logger = logging.getLogger("wallpaperwatcher")

@dataclass
class RedditAuthInfo():
    client_id: str
    client_secret: str

@dataclass
class RedditTokenInfo():
    access_token: str
    token_type: str
    expires_in: float
    scope: str

USER_AGENT = "python:com.jimthecactus.wallpaperwatcher:v0.0.1 (by /u/PM_ME_YOUR_BEST_MIO)"
with open("auth_info.json", "r") as f:
    CLIENT_AUTH = RedditAuthInfo(**json.load(f))
REDDIT_BASE_URL="https://oauth.reddit.com/"

io_loop: tornado.ioloop.IOLoop
interval: tornado.ioloop.PeriodicCallback
wallpaper_dir: Path
token: RedditTokenInfo
token = None

@contextlib.contextmanager
def get_async_client(*args, **kwargs) -> tornado.httpclient.AsyncHTTPClient:
    http_client = tornado.httpclient.AsyncHTTPClient(*args, **kwargs)
    try:
        yield http_client
    finally:
        http_client.close()

async def call_reddit_api(endpoint, **kwargs):
    logger.debug("call_reddit_api: enter")

    global token

    with get_async_client() as client:
        qargs = {"raw_json":"1"}
        qargs.update(kwargs)
        query = "?" + urlencode(qargs)
        uri = f"{REDDIT_BASE_URL}{endpoint}{query}"

        logger.debug("call_reddit_api: begin query(uri='%s')", uri)
        try:
            response = await client.fetch(uri,
                user_agent=USER_AGENT,
                headers={"Authorization": f"{token.token_type} {token.access_token}"}
                )
        except tornado.httpclient.HTTPClientError as exc:
            # If we got an unauthorized error
            if exc.code == 401 or exc.code == 403:
                logger.error("Token was rejected! Resetting token.")
                token = None
            # And regardless, rethrow. (We can't recover from that, but we
            # can at least safely discount our old token)
            raise
        result = response.buffer
        logger.debug("call_reddit_api: query returned. %s", response.body)

    logger.debug("call_reddit_api: exit")
    return result

async def call_reddit_api_json(endpoint, **kwargs):
    return json.load(await call_reddit_api(endpoint, **kwargs))

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        bubba = self.get_argument("bubba", default=None)

        if bubba is None:
            self.send_error(400, message="'bubba' query parameter missing")
            return

        result = {'now': time.localtime()}
        logger.info(f"Got request for path '{self.request.uri}'")
        self.write(json.dumps(result))
    
    def write_error(self, status_code:int, message: str = "", **kwargs):
        self.set_status(status_code)
        self.write(f"<html><head><title>Exception</title></head><body><h1>{status_code}: {message}</h1></body></html>")

async def async_pause():
    stream = tornado.iostream.PipeIOStream(sys.stdin.fileno())
    await stream.read_until(b"\n")
    stream.close()

async def call_timer():
    logger.info("Fetching Value")
    try:
        result = await call_reddit_api_json("", bubba="gump")
    except Exception as exc:
        logger.error("Failed to fetch time struct!", exc_info=exc)
        return

    logger.info("Fetched: %s", result['now'])

async def get_new_token():
    logger.debug("get_token: enter")

    global token
    ACCESS_URI="https://www.reddit.com/api/v1/access_token"

    client: tornado.httpclient.AsyncHTTPClient
    with get_async_client() as client:
        qargs = {
            "grant_type": "client_credentials"
        }

        body = urlencode(qargs)
        
        logger.debug("get_token: begin query(uri='%s')", ACCESS_URI)
        response = await client.fetch(ACCESS_URI,            
            method='POST',
            body=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            user_agent=USER_AGENT,
            auth_mode="basic",
            auth_username=CLIENT_AUTH.client_id,
            auth_password=CLIENT_AUTH.client_secret,
            )
        logger.debug("get_token: query returned. %s", response.body)
        result = json.load(response.buffer)

    token = RedditTokenInfo(**result)
    logger.info(f"Token is good for: {result['expires_in']}")
    io_loop.call_later(token.expires_in-240, get_new_token)
    
    logger.debug("get_token: exit")

async def login_and_start_loop():
    await get_new_token()

    #interval = tornado.ioloop.PeriodicCallback(callback=call_timer, callback_time=1000)
    #interval.start()

    result = await call_reddit_api_json("r/moescape/new.json", limit=3)
    logger.debug(json.dumps(result, indent=2))

def get_server_app():
    return tornado.web.Application([
        (r"/", MainHandler),
    ])

if __name__ == "__main__":
    # Windows workaround for Python 3.8 and Tornado 6.0.4.
    # See https://www.tornadoweb.org/en/stable/index.html#installation
    if os.name=="nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Setup our logging
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)

    # Setup our path
    wallpaper_dir = Path("wallpapers")
    wallpaper_dir.mkdir(parents=True, exist_ok=True)

    io_loop = tornado.ioloop.IOLoop.current()

    app = get_server_app()
    app.listen(8080)

    io_loop.add_callback(login_and_start_loop)    
    io_loop.start()