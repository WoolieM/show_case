import pandas as pd
from typing import List
import os
import sys
module_path = os.path.abspath(os.path.join('..','..'))
if module_path not in sys.path:
    sys.path.append(module_path)

from src.utility.utility import SQLConnection, load_config

config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = load_config(config_path)

async def get_procedure_list(DB_LIST: List[str] = config['db_list']) -> pd.DataFrame:
    """
    Generates and executes SQL queries to union all stored procedures and their definitions,
    
    Parameters:
    DB_LIST (List[str]): A list of database names to query.

    Returns:
    pd.DataFrame:the union of all stored procedures and their definitions
                                       
    Example:
    >>> DB_LIST = ['db1', 'db2', 'db3']
    >>> df = procedure_list(DB_LIST)
    >>> print(df.head())
    """
 
    union_query = ""
    exclude_procedure = config.get('exclude_procedure')
    # Loop through each database in the list
    for db in DB_LIST:
        query = f"""
        SELECT
            '{db}' AS db,
            c.name AS schema_name,
            a.name,
            b.definition
        FROM
            {db}.sys.procedures as a
        JOIN
            {db}.sys.sql_modules as b
        ON
            a.object_id = b.object_id
        JOIN
            {db}.sys.schemas as c
        ON
            a.schema_id = c.schema_id
        WHERE
            a.name NOT IN ({','.join([f"'{procedure}'" for procedure in exclude_procedure])})
        """
            # Append the queries to the union_query strings with UNION keyword
        if union_query:
            union_query += " UNION "
        union_query += query
    # Execute the union queries and return the results as DataFrames
    sql_conn = SQLConnection(db_name = config['databasename'], server = config['servername'])
    df = await sql_conn.run_query_aio(union_query)

    return df