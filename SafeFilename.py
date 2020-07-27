"""
SafeFilename.py
By: John-Michael O'Brien
Date: 7/27/2020

A structure for parsing and storing filenames.
"""

from pathlib import PurePath, Path
import re

import urllib3

class SafeFilename():
    uri: str
    prefix: str
    suffix: str
    def __init__(self, uri: str, mime_type: str):
        self.uri = uri
        # Parse out the content type so we can infer suffixes as necessary.
        # Parse out the URL
        url_info = urllib3.util.url.parse_url(self.uri)
        # and get the path element.
        path = PurePath(url_info.path)
        # Make the path banally simple and 32 characters or less.
        self.prefix = re.sub(r'[^A-Za-z0-9_\.]', '_', path.stem)[:32]
        # Cache the suffix (may be blank)
        self.suffix = path.suffix
