"""
This module contains the data models used in the Needle API client.
"""

from typing import Any, Optional, Literal
from dataclasses import dataclass, asdict
import json


@dataclass(frozen=True)
class NeedleConfig:
    """
    Configuration for the Needle API client.
    """

    api_key: Optional[str]
    url: Optional[str]
    search_url: Optional[str]


@dataclass(frozen=True)
class NeedleBaseClient:
    """
    Base client for interacting with the Needle API. Not intended to be used directly.
    """

    config: NeedleConfig
    headers: dict


FileType = Literal["application/pdf"]


@dataclass()
class Error(BaseException):
    """
    Error response from the Needle API. An object of this class is raised when an API request fails.
    """

    code: int
    message: str
    data: Optional[Any] = None

    def __str__(self):
        return json.dumps(asdict(self), allow_nan=False)


@dataclass(frozen=True)
class Collection:
    """
    Represents a collection in the Needle API.
    A collection is a group of files that can be searched together.
    """

    id: str
    name: str
    embedding_model: str
    embedding_dimensions: str
    search_queries: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class FileToAdd:
    """
    Represents file metadata, used when adding new files to a collection in the Needle API.
    """

    name: str
    url: str


CollectionFileStatus = Literal["pending", "indexed", "error"]


@dataclass(frozen=True)
class CollectionFile:
    """
    Represents a file in the Needle API. Note that a file can be part of multiple collections.
    """

    id: str
    name: str
    type: FileType
    url: str
    user_id: str
    connector_id: str
    size: int
    md5_hash: str
    created_at: str
    updated_at: str
    status: CollectionFileStatus


@dataclass(frozen=True)
class CollectionDataStats:
    """
    Represents data statistics of a collection in the Needle API.
    """

    status: Optional[str]
    files: int
    bytes: int


@dataclass(frozen=True)
class CollectionStats:
    """
    Represents statistics of a collection in the Needle API.
    """

    data_stats: list[CollectionDataStats]
    chunks_count: int
    characters: int
    users: int


@dataclass(frozen=True)
class SearchResult:
    """
    Represents a search result from the Needle API.
    """

    content: str
    file_id: str
