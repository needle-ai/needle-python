from .collections import NeedleCollections
from .files import NeedleFiles
from needle.v2.models import (
    NeedleConfig,
    NeedleBaseClient,
)
from typing import Optional
import os
from urllib.parse import urlparse, urlunparse

__all__ = ["NeedleCollections", "NeedleFiles"]


class NeedleClient(NeedleBaseClient):
    """
    A client for interacting with the Needle API.

    This class provides a high-level interface for interacting with the Needle API,
    including managing collections and performing searches.

    Initialize the client with an API key and an optional URL.
    If no API key is provided, the client will use the `NEEDLE_API_KEY` environment variable.
    If no URL is provided, the client will use the default Needle API URL, that is https://needle-ai.com.

    Attributes:
        collections (NeedleCollections): A client for managing collections within the Needle API.
        files (NeedleFiles): A client for managing files within the Needle API.
    """

    def __init__(
        self,
        api_key: Optional[str] = os.environ.get("NEEDLE_API_KEY"),
        url: Optional[str] = "https://needle-ai.com",
        _search_url: Optional[str] = None,
    ):
        if not _search_url:
            parsed_url = urlparse(url)
            new_netloc = f"search.{parsed_url.netloc}"
            _search_url = urlunparse(parsed_url._replace(netloc=new_netloc))

        config = NeedleConfig(api_key, url, search_url=_search_url)
        headers = {"x-api-key": config.api_key}
        super().__init__(config, headers)

        # sub clients
        self.collections = NeedleCollections(config, headers)
        self.files = NeedleFiles(config, headers)