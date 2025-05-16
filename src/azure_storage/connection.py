from azure.core.credentials import AccessToken
from azure.identity import  DefaultAzureCredential
from azure.identity.aio import DefaultAzureCredential as AioDefaultAzureCredential
import os
import sys
from typing import Optional
from adlfs import AzureBlobFileSystem
from dataclasses import dataclass, field
#For Notebooke Path
module_path = os.path.abspath(os.path.join('..','..'))
if module_path not in sys.path:
    sys.path.append(module_path)

def get_azure_credential() -> DefaultAzureCredential:
    """
    Authentication for Azure functionalities.

    Args:
        None

    Returns:
        ChainedTokenCredential: Azure credential
    """
    # According to
    # https://docs.microsoft.com/en-us/dotnet/api/overview/azure/identity-readme#environment-variables

    cred = DefaultAzureCredential(
        exclude_shared_token_cache_credential=True,
        # This is to avoid using the cached token
        # https://stackoverflow.com/questions/67165101/azure-chainedtokencredential-fails-after-password-change
        exclude_visual_studio_code_credential=True,  # To Avoid the following bug
        # VisualStudioCodeCredential: Azure Active Directory error
        #'(invalid_grant) AADSTS700082: The refresh token has expired due to inactivity. The token was issued on
    )

    return cred

def get_aio_azure_credential() -> AioDefaultAzureCredential:
    """
    (Async ver, for adlfs use) Authentication for Azure functionalities.

    Args:
        None

    Returns:
        cred (object): Azure credential
    """
    # According to
    # https://docs.microsoft.com/en-us/dotnet/api/overview/azure/identity-readme#environment-variables

    cred = AioDefaultAzureCredential(
        exclude_shared_token_cache_credential=True,
        # This is to avoid using the cached token
        # https://stackoverflow.com/questions/67165101/azure-chainedtokencredential-fails-after-password-change
        exclude_visual_studio_code_credential=True,  # To Avoid the following bug
        # VisualStudioCodeCredential: Azure Active Directory error
        #'(invalid_grant) AADSTS700082: The refresh token has expired due to inactivity. The token was issued on
    )

    return cred


def get_token(scopes=["https://graph.microsoft.com/.default"]) -> AccessToken:
    """
    Generate Azure access token

    Args:
        scopes (list, optional): The scope for the access token. Defaults to ["https://graph.microsoft.com/.default"].

    Returns:
        AccessToken: An Azure AccessToken Object
    """
    return get_azure_credential().get_token(*scopes)


class AzBlob:
    """
    A class for Azure Blob Storage interaction using lazy initialization.
    """
    account_name: str
    connection_timeout: Optional[int] = 600
    kwargs: dict = field(default_factory=dict)
    _fs: Optional[AzureBlobFileSystem] = field(default=None, init=False, repr=False)


    @property
    def fs(self) -> AzureBlobFileSystem:
        """
        Lazyly initializes and returns the AzureBlobFileSystem object.
        """
        if self._fs is None:
            self._fs = self._connect_to_fs()
        return self._fs

    def _connect_to_fs(self) -> AzureBlobFileSystem:
        """
        Connect to the Azure Blob Storage as a file system.

        Returns:
            fs (AzureBlobFileSystem): The file system object
        """
        
        cred = get_aio_azure_credential()
        fs = AzureBlobFileSystem(
            account_name=self.account_name,
            credential=cred,
            connection_timeout=self.connection_timeout,
            **self.kwargs
        )
        return fs