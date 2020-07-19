import logging
import json
import time
import contextlib
import shutil
from tempfile import NamedTemporaryFile
from pathlib import Path, PurePath
import urllib3.util.url
import re

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

async def download_image(url: str, directory: str):
    """ Downloads an image and saves it to a folder. """
    client: tornado.httpclient.AsyncHTTPClient

    logger.info("Starting download of '%s' to '%s'.", url, directory)
    with NamedTemporaryFile("wb") as f:
        with _get_async_client() as client:
            result = await client.fetch(url, streaming_callback=f.write)
            if 'Content-Type' in result.headers:
                content_type = result.headers['Content-Type']
            else:
                content_type = ""
            logger.debug("Discovered Content-Type: %s", content_type)

        # start with the directory we were given
        dirpath = Path(directory)
        # Make sure it exists.
        dirpath.mkdir(parents=True, exist_ok=True)
        # Parse out the URL
        url_info = urllib3.util.url.parse_url(url)
        # and get the path element.
        path = PurePath(url_info.path)
        # Make the path banally simple and 128 characters or less.
        prefix = re.sub(r'[^A-Za-z0-9_\.]',r'', path.stem)[:128]

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

        # Build up the final image name
        location = dirpath / f"{prefix}{suffix}"
        # Add infixes until we find a good filename.
        # This is non-atomic and unsafe. Don't go playing in the folder while we do this.
        count = 1
        warned = False
        while location.exists():
            if not warned:
                logger.warning("File '%s' already exists. File will be renamed.", str(location))
                warned = True
            location = dirpath / f"{prefix} ({count}){suffix}"
            count += 1
        # And copy our temporary file into it's final resting place.
        logger.debug("Copying from '%s' to '%s'", f.name, str(location))
        shutil.copyfile(f.name, str(location))
    logger.info("Download complete.")

async def main():
    await download_image(
        url="https://preview.redd.it/lpitob52ql951.jpg?auto=webp&s=aa11358055431dc140fe29907d840f0b10221c7f",
        directory=Path("."))

if __name__=="__main__":
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    io_loop = tornado.ioloop.IOLoop.current()
    io_loop.run_sync(main)