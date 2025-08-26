"""
This module provides NeedleCollectionsFiles class for interacting with 
Needle API's collection files endpoint.
"""

from dataclasses import asdict
from typing import List, Optional
import requests

from needle.v1 import NeedleBaseClient, NeedleConfig
from needle.v1.models import FileToAdd, Error, CollectionFile
from time import sleep


class NeedleCollectionsFiles(NeedleBaseClient):
    """
    A client for interacting with the Needle API's collection files endpoint.

    This class provides methods to create and manage collection files within the Needle API.
    It uses a requests session to handle HTTP requests with a default timeout of 120 seconds.
    """

    def __init__(self, config: NeedleConfig, headers: dict):
        super().__init__(config, headers)

        self.collections_endpoint = f"{config.url}/api/v1/collections"

        # requests config
        self.session = requests.Session()
        self.session.headers.update(headers)
        self.session.timeout = 120

    def add(self, collection_id: str, files: list[FileToAdd]):
        """
        Adds files to a specified collection. Added files will be automatically indexed and after be available for search within the collection.

        Args:
            collection_id (str): The ID of the collection to which files will be added.
            files (list[FileToAdd]): A list of FileToAdd objects representing the files to be added.

        Returns:
            list[CollectionFile]: A list of CollectionFile objects representing the added files.

        Raises:
            Error: If the API request fails.
        """

        endpoint = f"{self.collections_endpoint}/{collection_id}/files"
        req_body = {"files": [asdict(f) for f in files]}
        resp = self.session.post(endpoint, json=req_body)
        body = resp.json()
        
        if resp.status_code >= 400:
            error = body.get("error")
            raise Error(**error)
        return [
            CollectionFile(
                id=cf.get("id"),
                name=cf.get("name"),
                type=cf.get("type"),
                url=cf.get("url"),
                user_id=cf.get("user_id"),
                connector_id=cf.get("connector_id"),
                size=cf.get("size"),
                md5_hash=cf.get("md5_hash"),
                created_at=cf.get("created_at"),
                updated_at=cf.get("updated_at"),
                status=cf.get("status"),
            )
            for cf in body.get("result")
        ]

    def list(self, collection_id: str):
        """
        Lists all files in a specified collection.

        Args:
            collection_id (str): The ID of the collection whose files will be listed.

        Returns:
            list[CollectionFile]: A list of CollectionFile objects representing the files in the collection.

        Raises:
            Error: If the API request fails.
        """
        if collection_id is not None:
            self.collection_id = collection_id
        endpoint = f"{self.collections_endpoint}/{self.collection_id}/files"
        resp = self.session.get(endpoint)
        body = resp.json()
        
        if resp.status_code >= 400:
            error = body.get("error")
            raise Error(**error)
        return [
            CollectionFile(
                id=cf.get("id"),
                name=cf.get("name"),
                type=cf.get("type"),
                url=cf.get("url"),
                user_id=cf.get("user_id"),
                connector_id=cf.get("connector_id"),
                size=cf.get("size"),
                md5_hash=cf.get("md5_hash"),
                created_at=cf.get("created_at"),
                updated_at=cf.get("updated_at"),
                status=cf.get("status"),
            )
            for cf in body.get("result")
        ]
    
    def delete(self, collection_id: str, file_ids: List[str]) -> None:
        """Delete files from current collection.

        Args:
            collection_id (str): The ID of the collection from which files will be deleted.
            file_ids (List[str]): List of file IDs to delete.
            
        Raises:
            ValueError: If no collection ID is set.
            Error: If the API request fails.
        """
        if not collection_id:
            raise ValueError("No collection ID set")

        endpoint = f"{self.collections_endpoint}/{collection_id}/files"
        req_body = {"file_ids": file_ids}
        resp = self.session.delete(endpoint, json=req_body)

        if resp.status_code >= 400:
            error = resp.json().get("error")
            raise Error(**error)
