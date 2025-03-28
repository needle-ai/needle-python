"""
This module provides NeedleCollections class for interacting with Needle API's collections endpoint.
"""

from typing import Optional

import requests

from needle.v1.models import (
    NeedleConfig,
    NeedleBaseClient,
    Collection,
    Error,
    SearchResult,
    CollectionStats,
    CollectionDataStats,
)
from needle.v1.collections.files import NeedleCollectionsFiles


class NeedleCollections(NeedleBaseClient):
    """
    A client for interacting with the Needle API's collections endpoint.

    This class provides methods to create and manage collections within the Needle API.
    It uses a requests session to handle HTTP requests with a default timeout of 120 seconds.
    """

    def __init__(self, config: NeedleConfig, headers: dict):
        super().__init__(config, headers)

        self.endpoint = f"{config.url}/api/v1/collections"
        self.search_endpoint = f"{config.search_url}/api/v1/collections"

        # requests config
        self.session = requests.Session()
        self.session.headers.update(headers)
        self.session.timeout = 120

        # sub clients
        self.files = NeedleCollectionsFiles(config, headers)

    def create(self, name: str, file_ids: Optional[list[str]] = None):
        """
        Creates a new collection with the specified name and file IDs.

        Args:
            name (str): The name of the collection.
            file_ids (Optiona[list[str]]): A list of file IDs to include in the collection.

        Returns:
            Collection: The created collection object.

        Raises:
            Error: If the API request fails.
        """
        req_body = {"name": name, "file_ids": file_ids}
        resp = self.session.post(
            f"{self.endpoint}",
            json=req_body,
        )
        body = resp.json()
        if resp.status_code >= 400:
            error = body.get("error")
            raise Error(**error)
        c = body.get("result")
        return Collection(
            id=c.get("id"),
            name=c.get("name"),
            embedding_model=c.get("embedding_model"),
            embedding_dimensions=c.get("embedding_dimensions"),
            search_queries=c.get("search_queries"),
            created_at=c.get("created_at"),
            updated_at=c.get("updated_at"),
        )

    def get(self, collection_id: str):
        """
        Retrieves a collection by its ID.

        Args:
            collection_id (str): The ID of the collection to retrieve.

        Returns:
            Collection: The retrieved collection object.

        Raises:
            Error: If the API request fails.
        """
        resp = self.session.get(f"{self.endpoint}/{collection_id}")
        body = resp.json()
        if resp.status_code >= 400:
            error = body.get("error")
            raise Error(**error)
        c = body.get("result")
        return Collection(
            id=c.get("id"),
            name=c.get("name"),
            embedding_model=c.get("embedding_model"),
            embedding_dimensions=c.get("embedding_dimensions"),
            search_queries=c.get("search_queries"),
            created_at=c.get("created_at"),
            updated_at=c.get("updated_at"),
        )

    def list(self):
        """
        Lists all collections.

        Returns:
            list[Collection]: A list of all collections.

        Raises:
            Error: If the API request fails.
        """
        resp = self.session.get(self.endpoint)
        body = resp.json()
        if resp.status_code >= 400:
            error = body.get("error")
            raise Error(**error)
        return [
            Collection(
                id=c.get("id"),
                name=c.get("name"),
                embedding_model=c.get("embedding_model"),
                embedding_dimensions=c.get("embedding_dimensions"),
                search_queries=c.get("search_queries"),
                created_at=c.get("created_at"),
                updated_at=c.get("updated_at"),
            )
            for c in body.get("result")
        ]

    def search(
        self,
        collection_id: str,
        text: str,
        top_k: Optional[int] = None,
        offset: Optional[int] = None,
    ):
        """
        Searches within a collection based on the provided parameters.

        Args:
            params (SearchCollectionRequest): The search parameters.

        Returns:
            list[dict]: The search results.

        Raises:
            Error: If the API request fails.
        """
        endpoint = f"{self.search_endpoint}/{collection_id}/search"
        req_body = {
            "text": text,
            "top_k": top_k,
            "offset": offset,
        }
        resp = self.session.post(endpoint, headers=self.headers, json=req_body)
        body = resp.json()
        if resp.status_code >= 400:
            error = body.get("error")
            raise Error(**error)
        return [
            SearchResult(
                content=r.get("content"),
                file_id=r.get("file_id"),
            )
            for r in body.get("result")
        ]

    def get_stats(self, collection_id: str):
        """
        Retrieves statistics of a collection.

        Args:
            collection_id (str): The ID of the collection to retrieve statistics for.

        Returns:
            dict: The collection statistics.

        Raises:
            Error: If the API request fails.
        """
        endpoint = f"{self.endpoint}/{collection_id}/stats"
        resp = self.session.get(endpoint, headers=self.headers)
        body = resp.json()
        if resp.status_code >= 400:
            error = body.get("error")
            raise Error(**error)

        result = body.get("result")
        data_stats = [
            CollectionDataStats(
                status=ds.get("status"),
                files=ds.get("files"),
                bytes=ds.get("bytes"),
            )
            for ds in result.get("data_stats")
        ]
        return CollectionStats(
            data_stats=data_stats,
            chunks_count=result.get("chunks_count"),
            characters=result.get("characters"),
            users=result.get("users"),
        )
