from typing import Optional, Awaitable, Dict, Any
import logging
import contextlib
from dataclasses import dataclass
import json
from urllib.parse import urlencode
from io import BytesIO

import tornado.ioloop
import tornado.httpclient
import tornado.httputil

@dataclass
class RedditAuthInfo():
    client_id: str
    client_secret: str

    @staticmethod
    def from_file(filename:str) -> "RedditAuthInfo":
        with open(filename, "r") as f:
            auth = RedditAuthInfo(**json.load(f))
        return auth

@dataclass
class RedditTokenInfo():
    access_token: str
    token_type: str
    expires_in: float
    scope: str

logger: logging.Logger
logger = logging.getLogger("wallpaperwatcher.asyncreddit")

@contextlib.contextmanager
def _get_async_client(*args, **kwargs) -> tornado.httpclient.AsyncHTTPClient:
    http_client = tornado.httpclient.AsyncHTTPClient(*args, **kwargs)
    try:
        yield http_client
    finally:
        http_client.close()

class RedditClient():
    def __init__(self,
                 user_agent:str,
                 auth_info: RedditAuthInfo,
                 token: Optional[RedditTokenInfo]=None, 
                 base_url:Optional[str]="https://oauth.reddit.com/",
                 auth_url:Optional[str]="https://www.reddit.com/api/v1/access_token"):
        self._user_agent = user_agent
        self._auth_info = auth_info
        self._base_url = base_url
        self._auth_url = auth_url
        self._token = token

    async def call_reddit_api(self, endpoint, **kwargs) -> BytesIO:
        logger.debug("call_reddit_api: enter")

        with _get_async_client() as client:
            qargs = {"raw_json":"1"}
            qargs.update(kwargs)
            query = "?" + urlencode(qargs)
            uri = f"{self._base_url}{endpoint}{query}"

            logger.debug("call_reddit_api: begin query(uri='%s')", uri)
            response: tornado.httpclient.HTTPResponse = await client.fetch(uri,
                user_agent=self._user_agent,
                headers={"Authorization": f"{self._token.token_type} {self._token.access_token}"}
                )
            result = response.buffer
            logger.debug("call_reddit_api: query returned. %s", response.body)

        logger.debug("call_reddit_api: exit")
        return result

    async def call_reddit_api_json(self, endpoint, **kwargs) -> Dict[str, Any]:
        response = await self.call_reddit_api(endpoint, **kwargs)
        result = json.load(response)
        response.close()        
        return result

    async def get_new_token(self) -> RedditTokenInfo:
        logger.debug("get_token: enter")

        client: tornado.httpclient.AsyncHTTPClient
        with _get_async_client() as client:
            qargs = {
                "grant_type": "client_credentials"
            }

            body = urlencode(qargs)
            
            logger.debug("get_token: begin query(uri='%s')", self._auth_url)
            response = await client.fetch(self._auth_url,            
                method='POST',
                body=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                user_agent=self._user_agent,
                auth_mode="basic",
                auth_username=self._auth_info.client_id,
                auth_password=self._auth_info.client_secret,
                )
            logger.debug("get_token: query returned. %s", response.body)
            result = json.load(response.buffer)
            response.buffer.close()

        self._token = RedditTokenInfo(**result)

        logger.debug("get_token: exit")
        return self._token


