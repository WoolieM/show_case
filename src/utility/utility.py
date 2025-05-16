import yaml
from typing import Dict, Any, Optional, List, AsyncGenerator, Tuple
import pandas as pd
import aioodbc
from sqlalchemy import create_engine, text
from dataclasses import dataclass
from src.utility.logger import get_logger


def load_config(filepath: str = "config.yaml") -> Optional[Dict[str, Any]]:
    """Loads configuration data from a YAML file.

    This function reads a YAML file and parses its contents into a Python dictionary.
    It uses `yaml.safe_load()` to prevent potential security vulnerabilities.

    Args:
        filepath (str): The path to the YAML configuration file. Defaults to "config.yaml".

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing the configuration data if the file
        is successfully loaded and parsed. Returns None if the file is not found or if there
        is an error during YAML parsing.

    Raises:  (It's good practice to document potential exceptions, but in this case,
              the try-except block handles them, so you might not need to list them
              explicitly in the docstring unless you want to call special attention
              to some of them).  If you did want to document them:
        FileNotFoundError: If the specified file does not exist.
        yaml.YAMLError: If there is an error in the YAML syntax of the file.
    """
    try:
        with open(filepath, 'r') as file:
            config = yaml.safe_load(file)
            return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{filepath}' not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file '{filepath}': {e}")
        return None

def read_sql_file(file_path: str) -> str:
    """
    Reads an SQL query from a file and returns it as a string.

    Args:
        file_path (str): The path to the SQL file.

    Returns:
        str: The SQL query read from the file.
    """
    with open(file_path, 'r') as file:
        sql_query = file.read()
    return sql_query

@dataclass
class SQLConnection:
    db_name: str
    server: str
    dsn: Optional[str] = None
    _engine: Optional[Any] = None

    def get_dsn(self) -> str:
        """
        Constructs the DSN for the database connection using Windows Authentication.

        Returns:
            str: The DSN string.
        """
        if self.dsn is None:
            self.dsn =  f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={self.server};DATABASE={self.db_name};Trusted_Connection=yes;TrustServerCertificate=yes;'
        return self.dsn

    def get_engine(self):
        """
        Creates and returns a SQLAlchemy engine for the database connection.
        """
        if self._engine is None:
            self._engine = create_engine(
                'mssql+pyodbc://@' + self.server + '/' + self.db_name + '?trusted_connection=yes&driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'
            )
        return self._engine
    

    async def row_generator(self, query: str) -> AsyncGenerator[Tuple, None]:
        """
        Asynchronously executes a SQL query and yields column names and then rows as tuples.

        Yields:
            Tuple[str, Tuple]: The first yield will be ('columns', (col1_name, col2_name, ...)).
            Subsequent yields will be (row_value1, row_value2, ...).
        """
        dsn = self.get_dsn()
        async with aioodbc.create_pool(dsn=dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query)
                    columns = [desc[0] for desc in cur.description] # Yield column names first
                    yield('columns', tuple(columns))
                    async for row in cur:
                        yield row
    
    async def run_query_aio(self, query: str) -> pd.DataFrame:
        """
         Asynchronously consumes the row generator and returns the result as a DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            pd.DataFrame: The result of the query as a DataFrame.
        """
        rows: List[list] = []
        columns: List[str] = []
        async for item in self.row_generator(query):
            if item[0] == 'columns':
                columns = list(item[1])

            else:
                rows.append(list(item))

        df = pd.DataFrame(rows, columns=columns)
        return df
    
    def upload(
        self,       
        df: pd.DataFrame,
        to_service_table_name: str
    ) -> None:
        """
        Uploads a DataFrame to a specified SQL Server table.

        Args:
            df (pd.DataFrame): The DataFrame to upload.

        Returns:
            None
        """
        engine = self.get_engine()
        
        #Workaound , SQLAlchemy cannot drop table for to_sql method#
        with engine.connect() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS dbo.{to_service_table_name}"))
            connection.commit()
        
        df.to_sql(to_service_table_name, con=engine, index=False, schema = 'dbo')

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Executes a SQL query against the specified SQL Server database and returns the result as a DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            pd.DataFrame: The result of the query as a DataFrame.
        """
        engine = self.get_engine()
        with engine.connect() as connection:
            result = pd.read_sql_query(query, connection)
            return result
        
    def execute_command(self, query: str) -> None:
        """
        Executes a SQL command that does not return data.

        Args:
            command (str): The SQL command to execute.
        """
        engine = self.get_engine()
        with engine.connect() as connection:
            connection.execute(text(query))
            connection.commit()



class SQLConnectionWithLogin(SQLConnection):
    def __init__(self, db_name: str, server: str, username: str, password: str):
        super().__init__(db_name, server)
        self.username = username
        self.password = password

    def get_dsn(self) -> str:
        """
        Constructs the DSN for the database connection using SQL Server Authentication.

        Returns:
            str: The DSN string.
        """
        return f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={self.server};DATABASE={self.db_name};UID={self.username};PWD={self.password};TrustServerCertificate=yes;'
    
    def get_engine(self):
        """
        Creates and returns a SQLAlchemy engine for the database connection using SQL Server Authentication.
        """
        if self._engine is None:
            self._engine = create_engine(
                f'mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.db_name}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'
            )
        return self._engine
