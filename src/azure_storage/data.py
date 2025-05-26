from typing import Optional, List, Dict, Any, Optional
from azure.core.credentials import TokenCredential
from azure.storage.blob.aio import BlobServiceClient
import os
import sys
#For Notebooke Path
module_path = os.path.abspath(os.path.join('..','..'))
if module_path not in sys.path:
    sys.path.append(module_path)
from src.azure_storage.connection import get_azure_credential
from src.utility.utility import load_config

config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = load_config(config_path)
# Load the configuration file
STORAGE_ACCOUNT_NAME = config['storage_account_name']

class Blob:
    """
    A class to interact with Blob storage.
    """
    def __init__(self, storage_account_name: str = STORAGE_ACCOUNT_NAME) -> None:
        """
        Initializes a Blob object.

        Establishes a connection to the specified Azure Blob storage account.

        Args:
            storage_account_name: The name of the Azure Blob storage account.
                Defaults to the value of the STORAGE_ACCOUNT_NAME constant.

        Raises:
            ValueError: If the storage account name is invalid or not provided.
        """

        self._storage_account_name: str = storage_account_name
        # Get credential
        self.cred: TokenCredential = None
        self._blob_service_client: Optional[BlobServiceClient] = None  # Consistent naming
        self._connect_blob(storage_account_name)


    def _connect_blob(self, storage_account_name: str) -> None:
        """
        Connects to the Azure Blob storage account.

        Retrieves Azure credentials and creates a BlobServiceClient instance.

        Args:
            storage_account_name: The name of the Azure Blob storage account.

        Raises:
            Exception: If there's an issue establishing the connection.  (Consider more specific exception types)
        """
        if self.cred is None:
            self.cred = get_azure_credential()

        blob_url = f"https://{storage_account_name}.blob.core.windows.net"
        self._blob_service_client = BlobServiceClient(blob_url, self.cred)


    async def __aexit__(self, *excinfo):
        """
        Asynchronous context manager exit.

        Closes the BlobServiceClient if it has been initialized. This ensures
        that the connection to the Azure Blob storage is properly closed,
        releasing any resources.

        Args:
            *excinfo: Exception information if an exception occurred within
                the `async with` block.  This is a tuple containing the
                exception type, the exception instance, and the traceback
                object.  While this method doesn't directly use the exception
                information, it's included as part of the context manager
                protocol.

        Returns:
            None.
        """
        if self._blob_service_client:
            await self._blob_service_client.close()
        return None

    async def get_container_names(self) -> List[str]:
        """Asynchronously retrieves a list of container names from the storage account.

        This method iterates through the containers in the storage account associated with
        this Blob object and collects their names into a list.  Because the operation is
        asynchronous, it must be called with `await`.

        Returns:
            A list of strings, where each string is the name of a container.
            Returns an empty list if no containers are found or if an error occurs
            during the listing process.

        Raises:
            (Consider adding specific exceptions you might catch here, e.g., if there are potential
             issues with the Azure connection or permissions). For example:
             azure.core.exceptions.ResourceNotFoundError: If a container is not found.
             azure.core.exceptions.ClientAuthenticationError: If there are authentication issues.
        """
        container_names = []
        try:
            #async for container in self._blob_.list_containers():
            async for container in self._blob_service_client.list_containers():
                container_names.append(container.name)
            return container_names
        except Exception as e: # Catch a general exception for other errors
            print(f"An error occurred: {e}")
            return [] # Return an empty list
        

    async def get_blob_names(self, container_name: str, start_with: Optional[str] = None) -> List[str]:
        """
        Asynchronously retrieves a list of blob names from a container.

        This method iterates through the blobs in the specified container and collects
        their names into a list. Because the operation is asynchronous, it must be
        called with `await`.

        Args:
            container_name: The name of the container from which to retrieve blob names.

        Returns:
            A list of strings, where each string is the name of a blob.
            Returns an empty list if no blobs are found or if an error occurs
            during the listing process.

        Raises:
            azure.core.exceptions.ResourceNotFoundError: If the container is not found.
            azure.core.exceptions.ClientAuthenticationError: If there are authentication issues.
            Exception: If any other error occurs during the process.
        """
        blob_names = []
        try:
            #container_client = self._blob.get_container_client(container_name)  # Get container client
            container_client = self._blob_service_client.get_container_client(container_name)  # Get container client
            
            async for blob in container_client.list_blobs(start_with):  # Use async for and list_blob_names()
                blob_names.append(blob.name) # Access name from the BlobProperties object
            return blob_names
        except Exception as e:
            print(f"An error occurred: {e}")
            return []  # Return an empty list