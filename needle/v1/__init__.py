"""
This module provides NeedlClient class for interacting with Needle API.
"""

from typing import Optional
import os

from needle.utils import make_needle_search_url
from needle.v1.models import (
    NeedleConfig,
    NeedleBaseClient,
)
from needle.v1.collections import NeedleCollections
from needle.v1.files import NeedleFiles


NEEDLE_DEFAULT_URL = "https://needle.app"


class NeedleClient(NeedleBaseClient):
    """
    A client for interacting with the Needle API.

    This class provides a high-level interface for interacting with the Needle API,
    including managing collections and performing searches.

    Initialize the client with an API key and an optional URL.
    If no API key is provided, the client will use the `NEEDLE_API_KEY` environment variable.
    If no URL is provided, the client will use the default Needle API URL, that is https://needle.app.

    Attributes:
        collections (NeedleCollections): A client for managing collections within the Needle API.
    """

    def __init__(
        self,
        api_key: Optional[str] = os.environ.get("NEEDLE_API_KEY"),
        url: Optional[str] = NEEDLE_DEFAULT_URL,
        _search_url: Optional[str] = None,
    ):
        if not _search_url:
            _search_url = make_needle_search_url(url)

        config = NeedleConfig(api_key, url, search_url=_search_url)
        headers = {"x-api-key": config.api_key}
        super().__init__(config, headers)

        # sub clients
        self.collections = NeedleCollections(config, headers)
        self.files = NeedleFiles(config, headers)
