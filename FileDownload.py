"""
FileDownload.py
By: John-Michael O'Brien
Date: 7/27/2020

Provides utilites for asynchronously downloading a file with
minimal CPU intervention in a coroutine based system.
"""

import logging
from typing import List, BinaryIO, Optional, Union
import contextlib
import hashlib

import urllib3.util.url
import tornado.httpclient

logger: logging.Logger
logger = logging.getLogger("wallpaperwatcher.FileDownload")

@contextlib.contextmanager
def _get_async_client(*args, **kwargs) -> tornado.httpclient.AsyncHTTPClient:
    """ Creates a context manager that makes sure to close the HTTP client when it's done """
    http_client = tornado.httpclient.AsyncHTTPClient(*args, **kwargs)
    try:
        yield http_client
    finally:
        http_client.close()

class FileDownload():
    _hasher: hashlib.sha256
    _uri: str
    _file: BinaryIO
    _bytes_received: int
    _bytes_total: int
    _max_bytes: int
    _headers: tornado.httputil.HTTPHeaders
    _hash: Optional[str]
    _done: bool
    _started: bool
    _got_status: bool

    def __init__(self,uri: str, filestream: BinaryIO):
        self._hasher = hashlib.sha256()
        self._client = tornado.httpclient.AsyncHTTPClient()
        self._uri = uri
        self._file = filestream
        self._bytes_received = 0
        self._bytes_total = 0
        self._max_bytes = 50*1024*1024 # 50 MB
        self._headers = tornado.httputil.HTTPHeaders()
        self._hash = None
        self._done = False
        self._started = False
        self._got_status = False

    def _process_header(self, data: str):
        logger.debug("Got header: %s", data.strip())
        if self._got_status:
            self._headers.parse_line(data)
        else:
            # Ignore the first line.
            self._got_status = True

    def _process_chunk(self, data: bytes):
        self._bytes_received += len(data)
        if self._bytes_received > self._max_bytes:
            raise Exception("Maximum download size exceeded!")
        self._hasher.update(data)
        self._file.write(data)

    async def start(self) -> tornado.httpclient.HTTPResponse:
        if self._started:
            raise ValueError("Cannot start. Download already started.")
        logger.info("Downloading %s", self._uri)
        self._started = True
        with _get_async_client() as client:
            result = await client.fetch(self._uri,
                                          header_callback=self._process_header,
                                          streaming_callback=self._process_chunk)

        # Cache the result of our hasher.
        self._hash = self._hasher.hexdigest()

        # Mark that we're finished
        self._done = True
        logger.info("Download complete. Hash is %s", self._hash)

        # Return the request, populated with our headers.
        result.headers = self._headers
        return result

    def is_done(self) -> bool:
        return self._done

    def is_started(self) -> bool:
        return self._started

    def get_progress(self) -> float:
        if self._done:
            return 100.0

        total = float(self._headers.get("Content-Length", "0"))
        if total == 0:
            return -self._bytes_received

        return float(self._bytes_received)/float(total) * 100.0

    def get_hash(self) -> str:
        if not self._hash:
            raise ValueError("Download isn't complete. No hash has been computed.")
        return self._hash

    def get_mime_type(self) -> str:
        if "Content-Type" not in self._headers:
            raise ValueError("Content-Type headers have not been received yet.")
        return self._headers["Content-Type"]