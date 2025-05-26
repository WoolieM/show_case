from abc import ABC, abstractmethod
from typing import Dict, Optional, List, Iterable, AsyncGenerator, Tuple
import pandas as pd
import os
import sys
from dataclasses import dataclass, field
from adlfs import AzureBlobFileSystem
from azure.identity.aio import DefaultAzureCredential
import pyarrow
import pyarrow.parquet as pq
import time
from datetime import datetime
from pytz import timezone
#For Notebooke Path
module_path = os.path.abspath(os.path.join('..','..'))
if module_path not in sys.path:
    sys.path.append(module_path)



class BaseDestinationService(ABC):
    """
    Abstract base class for services that load data into a destination.
    """
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    async def load_data(self, g_data: AsyncGenerator[Tuple, None]) -> None:
        """
        Asynchronously loads data from the provided generator into the destination.

        Args:
            g_data: An asynchronous generator yielding tuples of data. The first
                    item in the generator is expected to be a tuple with the
                    string 'columns' as the first element and a list of column
                    names as the second element. Subsequent tuples represent
                    rows of data.
        """
        raise NotImplementedError()

@dataclass
class ParquetSemiAsync(BaseDestinationService):
    """
    A service that asynchronously writes data to a Parquet file in chunks and at intervals.
    """
    path: str
    """The local file path where the Parquet file will be written."""
    chunk_size: int = 500
    """The maximum number of rows to buffer before writing to disk."""
    flush_interval: float = 60.0
    """The maximum time (in seconds) to wait before flushing the buffer to disk, even if the chunk size is not reached."""
    write_options: Optional[Dict] = field(default_factory=dict)
    """Additional write options to pass to pyarrow.parquet.write_table."""
    buffer: List[Dict] = field(default_factory=list)
    """A buffer to hold data before writing to Parquet."""
    _last_flush_time: Optional[float] = field(default=None, init=False)
    """The timestamp of the last buffer flush."""
    _is_initialized: bool = field(default=False, init=False) # To track if columns are received


    def _write_table(self, table: pyarrow.Table) -> None:
        """
        Writes a PyArrow Table to the Parquet file.

        Args:
            table: The PyArrow Table to write.
        """
        pq.write_table(table, self.path, **self.write_options)

    def _flush_buffer(self) -> None:
        """
        Converts the buffer to a Pandas DataFrame and writes it to the Parquet file.
        Clears the buffer and updates the last flush time.
        """
        if self.buffer:
            df = pd.DataFrame(self.buffer).dropna(axis=1, how="all")
            table = pyarrow.Table.from_pandas(df)
            self._write_table(table)
            self.buffer = []
            self._last_flush_time = time.time()

    async def load_data(self, g_data: AsyncGenerator[Tuple, None]) -> None:
        """
        Asynchronously loads data from the provided generator, buffers it, and writes to a Parquet file.

        Args:
            g_data: An asynchronous generator yielding tuples of data. The first
                    item in the generator is expected to be a tuple with the
                    string 'columns' as the first element and a list of column
                    names as the second element. Subsequent tuples represent
                    rows of data. Missing values in the rows are converted to pandas.NA.
        """
        columns: Optional[List[str]] = None
        async for row in g_data:
            if row[0] == 'columns':
                columns = row[1]
            else:
                if columns:
                    row_dict = dict(zip(columns, row))
                    self.buffer.append(
                        {k: v if v is not None else pd.NA for k, v in row_dict.items()}
                    )

                    if len(self.buffer) >= self.chunk_size:
                        self._flush_buffer()

                    # Time-based flush check
                    if self._last_flush_time is not None and (time.time() - self._last_flush_time) >= self.flush_interval:
                        self._flush_buffer()
                    elif self._last_flush_time is None:
                        self._last_flush_time = time.time() # Initialize on first row

        # Ensure any remaining data in the buffer is flushed at the end
        self._flush_buffer()


class AzureParquet(ParquetSemiAsync):
    """
    A service that asynchronously writes data to a Parquet file on Azure Blob Storage in chunks and at intervals.
    """
    def __init__(
        self,
        path: str,
        storage_account_name: str,
        container_name: str,
        write_options: Dict = None,
        auth_method: str = "default",
        chunk_size: int = 32768,
        flush_interval: float = 60.0,
        **kwargs,
    ) -> None:
        """
        Initializes the AzureParquet service.

        Args:
            path: The path to the Parquet file within the Azure Blob Storage container.
            storage_account_name: The name of the Azure Storage Account.
            container_name: The name of the Azure Blob Storage container.
            write_options: Additional write options to pass to pyarrow.parquet.write_table.
            auth_method: The authentication method to use for Azure Blob Storage ('default' for DefaultAzureCredential).
            chunk_size: The maximum number of rows to buffer before writing to Azure Blob Storage.
            flush_interval: The maximum time (in seconds) to wait before flushing the buffer.
            **kwargs: Additional keyword arguments passed to AzureBlobFileSystem.
        """
        super().__init__(
            path=f"abfs://{container_name}@{storage_account_name}.blob.core.windows.net/{path}",
            chunk_size= chunk_size,
            flush_interval= flush_interval,
            write_options=write_options if write_options is not None else {},
            **kwargs
        )
        self.chunck_size = chunk_size
        abfs_kwargs = kwargs.copy()
        if auth_method == "default":
            abfs_kwargs["credential"] = DefaultAzureCredential(
                exclude_shared_token_cache_credential=True
                # This is to avoid using the cached token
                # https://stackoverflow.com/questions/67165101/azure-chainedtokencredential-fails-after-password-change
            )
        self.write_options["filesystem"] = AzureBlobFileSystem(
            account_name=storage_account_name, **abfs_kwargs
        )

        self.container_name = container_name
        self.account_name = storage_account_name
        self.blob_path = path

class AzurePartitionParquet(AzureParquet):
    """
    A service that asynchronously writes data to partitioned Parquet files on Azure Blob Storage
    based on the ingestion date, with options for file row limits.
    """
    def __init__(
        self,
        path: str,
        storage_account_name: str,
        container_name: str,
        write_options: Dict = None,
        auth_method: str = "default",
        chunk_size: int = 500,
        flush_interval: float = 60.0,
        rows_per_file: int = 100000,
        **kwargs,
    ) -> None:
        """
        Initializes the AzurePartitionParquet service.

        Args:
            path: The base path for the partitioned Parquet files within the Azure Blob Storage container.
            storage_account_name: The name of the Azure Storage Account.
            container_name: The name of the Azure Blob Storage container.
            write_options: Additional write options to pass to pyarrow.parquet.write_table.
            auth_method: The authentication method to use for Azure Blob Storage ('default' for DefaultAzureCredential).
            chunk_size: The maximum number of rows to buffer before writing.
            flush_interval: The maximum time (in seconds) to wait before flushing the buffer.
            rows_per_file: The maximum number of rows to write to each individual Parquet file.
            **kwargs: Additional keyword arguments passed to the base AzureParquet class.
        """
        # Construct the partition path.
        aest_timezone = timezone('Australia/Melbourne')
        today_aest = datetime.now(aest_timezone).date()
        today_str = today_aest.strftime("%Y-%m-%d")
        self.partition_path = f"{path}/ingestion_date={today_str}"
        super().__init__(
            path=self.partition_path,  # Use the constructed partition path
            storage_account_name=storage_account_name,
            container_name=container_name,
            write_options=write_options,
            auth_method=auth_method,
            chunk_size=chunk_size,
            flush_interval=flush_interval,
            **kwargs,
        )
        self.rows_per_file = rows_per_file
        self._file_counter = 1
        self._current_row_count = 0

    def _get_current_file_path(self) -> str:
        """
        Constructs the file path for the current Parquet file within the partition.

        Returns:
            The full file path for the Parquet file.
        """
        #  get the file path.
        base_path = self.partition_path
        if not base_path.endswith('/'):
            base_path += '/'
        return f"{base_path}part{self._file_counter:03d}.parquet"

    def _write_table(self, table: pyarrow.Table) -> None:
        """
        Writes a PyArrow Table to the current Parquet file in Azure Blob Storage.

        Args:
            table: The PyArrow Table to write.
        """
        # Use the file path.
        file_path = self._get_current_file_path()

        #local Test Only
        # pq.write_table(table, file_path)
        pq.write_table(table, file_path, **self.write_options)
    
    
    
    async def load_data(self, g_data: AsyncGenerator[Tuple, None]) -> None:
        """
        Asynchronously loads data from the provided generator, buffers it, and writes to partitioned
        Parquet files on Azure Blob Storage. New files are created based on the `rows_per_file` limit.

        Args:
            g_data: An asynchronous generator yielding tuples of data. The first
                    item in the generator is expected to be a tuple with the
                    string 'columns' as the first element and a list of column
                    names as the second element. Subsequent tuples represent
                    rows of data. Missing values in the rows are converted to pandas.NA.
        """
        columns: Optional[List[str]] = None
        async for row in g_data:
            if row[0] == 'columns':
                columns = row[1]
            else:
                if columns:
                    row_dict = dict(zip(columns, row))
                    self.buffer.append(
                        {k: v if v is not None else pd.NA for k, v in row_dict.items()}
                    )
                    self._current_row_count += 1

                    if len(self.buffer) >= self.chunk_size:
                        super()._flush_buffer()
                        if self._current_row_count >= self.rows_per_file:
                            self._current_row_count = 0
                            self._file_counter += 1
                            #  Update the path for the next file.
                            self.path = self._get_current_file_path() #set the self.path

                    # Time-based flush check
                    if self._last_flush_time is not None and (time.time() - self._last_flush_time) >= self.flush_interval:
                        super()._flush_buffer()
                        if self._current_row_count >= self.rows_per_file:
                            self._current_row_count = 0
                            self._file_counter += 1
                            # Update the path
                            self.path = self._get_current_file_path() #set the self.path
                    elif self._last_flush_time is None:
                        self._last_flush_time = time.time()  # Initialize on first row

        # Ensure any remaining data in the buffer is flushed at the end.
        if self.buffer:
            super()._flush_buffer()