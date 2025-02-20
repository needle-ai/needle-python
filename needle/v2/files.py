"""
This module provides NeedleFiles class for interacting with 
Needle API's collection files endpoint.
"""

from dataclasses import asdict
import requests
from time import sleep
from typing import List, Optional

from needle.v2.models import NeedleConfig, FileToAdd, Error, CollectionFile


class NeedleFiles:
    """
    A client for interacting with the Needle API's collection files endpoint.

    This class provides methods to create and manage collection files within the Needle API.
    It uses a requests session to handle HTTP requests with a default timeout of 120 seconds.
    """

    def __init__(self, config: NeedleConfig, headers: dict):
        self.config = config
        self.headers = headers
        self.collections_endpoint = f"{config.url}/api/v1/collections"

        # requests config
        self.session = requests.Session()
        self.session.headers.update(headers)
        self.session.timeout = 120
        self.collection_id = None

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
        
    def update_collection_id(self, collection_id: str) -> None:
        """Set the self.collection ID
        
        Args:
            collection_id: New collection ID to use
        """
        self.collection_id = collection_id

    def list(self, collection_id: str = None) -> list[CollectionFile]:
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
    
    def delete_file(self, file_ids: List[str]) -> None:
        """Delete files from current collection.

        Args:
            file_ids: List of file IDs to delete
            
        Raises:
            ValueError: If no collection ID is set
            requests.RequestException: If deletion request fails
        """
        if not self.collection_id:
            raise ValueError("No collection ID set")

        endpoint = f"{self.collections_endpoint}/{self.collection_id}/files"
        req_body = {"file_ids": file_ids}
        resp = self.session.delete(endpoint, json=req_body)
        
        if resp.status_code >= 400:
            raise  requests.RequestException(f"File deletion failed: {resp.status_code} - {resp.text}")
        else:
            f"File deletion successful"
            
    def add_file_from_url(
        self, 
        file_url: str, 
        overwrite: bool = False
    ) -> None:
        """Add files to collection from URL.
        
        Args:
            file_url: URL of the file to add
            collection_name: Optional collection name to use instead of ID
            overwrite: Whether to overwrite existing files
            
        Raises:
            ValueError: If collection cannot be determined or file exists
        """
        if not self.collection_id:
            raise ValueError("Collection must be specified by ID or name")
            
        files_info = {f.name: f.id for f in self.list()}
        file_id = files_info.get(file_url)

        if file_id:
            if not overwrite:
                raise ValueError(f"File {file_url} already exists in collection {self.collection_id}")
            self.delete_file([file_id])

        self.add(
            collection_id=self.collection_id,
            files=[FileToAdd(name=file_url, url=file_url)]
        )

        self._wait_for_indexing(file_name = file_url)
        print(f"Successfully added {file_url} to collection {self.collection_id}")

    def _wait_for_indexing(self, check_interval: int = 5, file_name:str = None) -> None:
        """Wait for all files in collection to be indexed.
        
        Args:
            check_interval: Seconds to wait between checks
        """
        if file_name is not None:
            all_files = [f for f in self.list(self.collection_id) if f.name == file_name]
        else:
            all_files = self.list(self.collection_id)
        while True:
            if all(f.status in ("indexed", "error")  for f in all_files):
                error_files = [f.name for f in all_files if f.status == "error"]
                if error_files:
                    print(f"Files failed to index: {', '.join(error_files)}")
                break      
            sleep(check_interval)