"""
downloader.py
By: John-Michael O'Brien
Date: 7/18/2020

Provides utilites for asynchronously downloading multiple files with
minimal CPU intervention in a coroutine based system.
"""

import logging
from typing import List
import contextlib
import shutil
from tempfile import NamedTemporaryFile
from pathlib import Path, PurePath
import re

import urllib3.util.url
import tornado.httpclient

logger: logging.Logger
logger = logging.getLogger("wallpaperwatcher.downloader")

known_types = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/svg+xml": ".svg",
    "image/x-icon": ".ico",
    "image/bmp": ".bmp",
    "image/apng": ".apng"
}

@contextlib.contextmanager
def _get_async_client(*args, **kwargs) -> tornado.httpclient.AsyncHTTPClient:
    """ Creates a context manager that makes sure to cloase the HTTP client when it's done """
    http_client = tornado.httpclient.AsyncHTTPClient(*args, **kwargs)
    try:
        yield http_client
    finally:
        http_client.close()

async def copy_to_destinations(source_file: str, dest_directories: List[str],
                               prefix: str, suffix: str, skip_existing: bool=False) -> List[str]:
    """ Copies a file to a number of destination folders """
    locations = []
    for directory in dest_directories:
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
                continue

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
        locations.append(str(location))
    return locations


async def download_image(url: str, dest_directories: List[str], skip_existing: bool) -> List[str]:
    """ Downloads an image and saves it to one or more folders. """
    client: tornado.httpclient.AsyncHTTPClient

    with NamedTemporaryFile("wb", delete=False) as target_file:
        logger.info("Starting download of '%s' to '%s'.", url, target_file.name)
        with _get_async_client() as client:
            result = await client.fetch(url, streaming_callback=target_file.write)
            # Make sure we flush our data to disk.
            target_file.close()

            # Parse out the content type so we can infer suffixes as necessary.
            if 'Content-Type' in result.headers:
                content_type = result.headers['Content-Type']
            else:
                content_type = ""
            logger.debug("Discovered Content-Type: %s", content_type)

        # Parse out the URL
        url_info = urllib3.util.url.parse_url(url)
        # and get the path element.
        path = PurePath(url_info.path)
        # Make the path banally simple and 128 characters or less.
        prefix = re.sub(r'[^A-Za-z0-9_\.]', '', path.stem)[:128]

        # If we don't have an extension
        suffix = path.suffix
        if suffix == "":
            # But we recognize the content type from the headers
            if content_type in known_types:
                # Use the extension associated with that type.
                suffix = known_types[content_type]
                logger.debug("Using extension '%s' since the file didn't have one.", suffix)
            else:
                logger.warning("No extension found and could not infer one for '%s'!", url)

        locations = await copy_to_destinations(target_file.name, dest_directories, prefix, suffix, skip_existing)

    logger.info("Download complete.")

    return locations
