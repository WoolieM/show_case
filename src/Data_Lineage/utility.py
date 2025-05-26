import xml.etree.ElementTree as ET
import re
from typing import Set, List, Dict, Union
import os
import sys
import pandas as pd
from src.utility.utility import load_config
import shutil

#For Notebook path
module_path = os.path.abspath(os.path.join('..','..'))
if module_path not in sys.path:
    sys.path.append(module_path)

config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = load_config(config_path)

UNVERSAL_TABLE_PATTERN = re.compile(
    r'\b(?:FROM|JOIN|UPDATE|INTO|MERGE|USING)\s+([\w\d\-]+(?:\.[\w\d\-]+){0,3})',
    re.IGNORECASE
)
def tree_root(file_path: str) -> ET.Element:
    """
    Parses an XML file and returns its root element.

    Args:
        file_path (str): The path to the XML file.

    Returns:
        ET.Element: The root element of the parsed XML tree.

    Raises:
        FileNotFoundError: If the specified file path does not exist.
        ET.ParseError: If the XML file cannot be parsed.
    """
    try:
        tree = ET.parse(file_path)
        return tree.getroot()
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except ET.ParseError:
        raise ET.ParseError(f"Error parsing XML in file: {file_path}")

class SQLScriptCleaner:
    """
    A class to clean SQL scripts by removing unnecessary commands and comments.

    Attributes:
    sql_script (str): The original SQL script.
    """

    def __init__(self, sql_script: str):
        """
        Initializes the SQLScriptCleaner with the provided SQL script.

        Parameters:
        sql_script (str): The original SQL script.
        """
        self.sql_script = sql_script

    def remove_comments(self) -> 'SQLScriptCleaner':
        """
        Removes comments from the SQL script.

        Returns:
        SQLScriptCleaner: The instance of the class.
        """
        self.sql_script = re.sub(r'/\*.*?\*/', '', self.sql_script, flags=re.DOTALL)
        self.sql_script = re.sub(r'--.*$', ' ', self.sql_script, flags=re.MULTILINE)
        return self

    def remove_extra_whitespace(self) -> 'SQLScriptCleaner':
        """
        Removes extra whitespace and newlines from the SQL script.

        Returns:
        SQLScriptCleaner: The instance of the class.
        """
        self.sql_script = re.sub(r'\r\n', ' ', self.sql_script)
        self.sql_script = re.sub(r'\t', ' ', self.sql_script)
        self.sql_script = re.sub(r'\n', ' ', self.sql_script).strip()
        return self

    def remove_brackets(self) -> 'SQLScriptCleaner':
        """
        Removes brackets from the SQL script.

        Returns:
        SQLScriptCleaner: The instance of the class.
        """
        self.sql_script = re.sub(r'\[([^\]]+)\]', r'\1', self.sql_script)
        return self

    def remove_patterns(self, patterns: list) -> 'SQLScriptCleaner':
        """
        Removes specific patterns from the SQL script.

        Parameters:
        patterns (list): A list of patterns to remove from the SQL script.

        Returns:
        SQLScriptCleaner: The instance of the class.
        """
        for pattern in patterns:
            self.sql_script = re.sub(pattern, ' ', self.sql_script, flags=re.IGNORECASE)
        return self

    def clean(self) -> str:
        """
        Cleans the SQL script by removing comments, extra whitespace, brackets, server names, 'dbo.', and specific patterns.

        Returns:
        str: The cleaned SQL script.
        """
        patterns = [
            r'\bSET\s+TRANSACTION\s+ISOLATION\s+LEVEL\s+SNAPSHOT\s+BEGIN\s+TRAN\b',
            r'\bOPEN\s+SYMMETRIC\s+KEY\s+SymmetricKey01\b',
            r'DECRYPTION BY CERTIFICATE CertificatetoPrtotectSensitiveData;'
        ]

        return self.remove_comments()\
                   .remove_extra_whitespace()\
                   .remove_brackets()\
                   .remove_patterns(patterns)\
                   .sql_script

# Function to extract stored procedure names from a SQL definition
def extract_sp_names(sp: str)-> str:
    # Define a regular expression pattern to match stored procedure names
    sp_pattern = re.compile(
        r'\b(?:CREATE\s+PROCEDURE|EXEC||EXECUTE|)\s+'
        r'(@?\w*\s*=\s*([\[\]a-zA-Z0-9_\.]+)|'
        r'(\[?[a-zA-Z0-9_]+\]?\.\[?[a-zA-Z0-9_]+\]?\.\[?[a-zA-Z0-9_]+\]?|\[?[a-zA-Z0-9_]+\]?\.\[?[a-zA-Z0-9_]+\]?|\[?[a-zA-Z0-9_]+\]?))',
        re.IGNORECASE
    )
    return sp_pattern.findall(sp)


def table_lineage_from_sql(sql: str) -> tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """
    Analyzes an SQL query to identify and categorize target and source tables, including handling table aliases and cursors.

    Parameters:
    sql (str): The SQL query to be analyzed.

    Returns:
    Dict[str, List[str]]: A dictionary with two keys:
        - 'target_tables': A list of target tables identified in the SQL query.
        - 'source_tables': A list of source tables identified in the SQL query.

    Example:
    >>> sql_query = "SELECT * FROM mydb.mytable JOIN anotherdb.anothertable ON mytable.id = anothertable.id"
    >>> result = table_lineage_from_sql(sql_query)
    >>> print(result['target_tables'])
    >>> print(result['source_tables'])
    """
    target_tables: Set[str] = set()
    source_tables: Set[str] = set()
    alias_tables: Set[str] = set()
    cursors: Set[str] = set()

    match_pattern_list: List[str] = []

    # Regular expressions for table identification
    table_pattern_with_alias = re.compile(
        r"\b(?:FROM|JOIN|UPDATE|INTO|MERGE|USING)\s+([\w\d\-]+(?:\.[\w\d\-]+){0,3})(?:\s+AS)?\s*([\w\d]+)?\b",
        re.IGNORECASE,
    )
    table_pattern = UNVERSAL_TABLE_PATTERN
    cursor_pattern = re.compile(r'\bDECLARE\s+(\w+)\s+CURSOR\s+FOR\b', re.IGNORECASE)

    for match in table_pattern_with_alias.finditer(sql):
        alias = match.group(2)
        if alias and alias.upper() not in config['sql_reserved_words']:
            alias_tables.add(alias.lower())

        match_pattern_list.append(match.group())

    # Need to loop again in case the first loop capture table names between two key words e.g. from table_name join
    for match in table_pattern.finditer(sql):
        match_pattern_list.append(match.group())

    # Collect cursor names
    for match in cursor_pattern.finditer(sql):
        cursor_name = match.group(1).lower() if match.lastindex == 1 else None
        cursors.add(cursor_name)

    for index, match_ in enumerate(match_pattern_list):
        keyword = match_.split()[0].upper()
        table_name = match_.split()[1].lower()
        if keyword in {"INTO", "MERGE", "UPDATE"} and table_name not in alias_tables and table_name.upper() not in config['sql_reserved_words']:
            target_tables.add(table_name)
        elif keyword == 'UPDATE' and table_name in alias_tables:
            if index + 1 < len(match_pattern_list):  # Check bounds!
                next_match = match_pattern_list[index + 1]
                table_name = next_match.split()[1].lower()
                target_tables.add(table_name)
            else:
                print("Warning: UPDATE with alias at end of list.  Missing table name.")  # Or raise an exception
        elif keyword in {"FROM", "JOIN", "USING"} and table_name not in alias_tables and table_name.upper() not in config['sql_reserved_words'] and table_name not in cursors:
            source_tables.add(table_name)

    # match_pattern_list is for inspection purpose only
    return target_tables, source_tables, match_pattern_list


def extract_table_lineage(sql: str, simple_return = False) -> Dict[str, List[str]]:
    """
    Extracts table lineage from a SQL script.

    Args:
        sql: The SQL script.

    Returns:
        A dictionary with 'target_tables' and 'source_tables' as keys
        and lists of unique table names as values.
    """
    
    def match_target_source(sql: str) -> Dict[str, List[str]]:

        source_target = table_lineage_from_sql(sql)
        # Extracting target and source tables from the returned tuple
        return {
            'target_tables': list(source_target[0]),
            'source_tables': list(source_target[1])
        }

    first_parse: Dict[str, List[str]] = match_target_source(sql)

    if simple_return:
        return first_parse

    target_tables = first_parse['target_tables']
    source_tables = first_parse['source_tables']
    target_number = len(target_tables)
    if target_number <= 1:

        return [{
            'target_tables': target_tables[0] if target_number == 1 else target_tables,
            'source_tables': source_tables
        }]
    else:
        statement_list = None
        data_lineages:List[Dict[str, List[str]]] = []
        consolidated_lineages = {}
        def sql_split(sql: str) -> List[Dict[str, Union[str, Set[str]]]]:
            # DDL_KEYWORDS = {'update', 'merge', 'into'}
            statements = []
            for keyword in config['ddl_keywords']:
                for match in re.finditer(rf"\s+\b{keyword}\b\s+", sql.lower()):
                    statement_start = match.start()
                    statement_end_match = re.search(r";|\Z", sql[statement_start:])
                    statement_end = statement_start + statement_end_match.start() if statement_end_match else len(sql)
                    statement = sql[statement_start:statement_end].strip()
                    statements.append(statement)
        
            return statements
        
        statement_list = sql_split(sql)
        for sql in statement_list:
            data_lineages.append(
                {
                    'target_tables': target_tables,
                    'source_tables': source_tables
                }
            )

        for lineage in data_lineages:
            targets = lineage['target_tables']
            sources = lineage['source_tables']
            for target in targets:
                if target:  #ignore if target table is null
                    if target in consolidated_lineages:
                        consolidated_lineages[target]['source_tables'].update(sources) #use update to add to the set
                    else:
                        consolidated_lineages[target] = {'source_tables': set(sources)}

        # Convert the dictionary back to a list of dictionaries
        result = []
        for target, data in consolidated_lineages.items():
            result.append({'target_tables': target, 'source_tables': data['source_tables']})
        return result
    
def checking_db_name(
    db_name: str,
    schema_name: str,
    table_list: List[str]
) -> List[str]:
    """Removes server name prefix and deduplicates (optimized).

    Handles table names in 'dbo.table_name' or 'db_name.dbo.table_name'.
    Deduplicates the list only if it has 2 or more elements.

    Args:
        db_name: The db name.
        schema_name: The schema name.
        table_list: The list of table names.

    Returns:
        A new list with the db name prefix removed and duplicates removed.
        Returns the original list if it has less than 2 elements.
    """

    if len(table_list) < 2:  # Optimized: Check length first
        return table_list  # Return original if short

    db_prefix  = (db_name + ".")
    db_schema_prefix = (db_prefix + schema_name + ".")
    seen: Set[str] = set()
    result: List[str] = []

    for table in table_list:
        if table.startswith(db_schema_prefix):
            new_table_name = table[len(db_prefix):]
            if new_table_name not in seen:
                result.append(new_table_name)
                seen.add(new_table_name)
        else:
            if table not in seen:
                result.append(table)
                seen.add(table)

    return result

def remove_sp_bracket(sp_name: Union[List, str]) -> Union[List, str]:
    """
    Extracts the last part of a stored procedure name from a given string.

    This function removes any square brackets from the input string and splits it by periods.
    

    Parameters:
    sp_name (str): The input string containing the stored procedure name, possibly with schema and database information.

    Returns:
    str: The extracted stored procedure name.
    """
    if isinstance(sp_name, list):
        return [remove_sp_bracket(item) for item in sp_name]
    elif isinstance(sp_name, str):
        cleaner = SQLScriptCleaner(sp_name)
        return cleaner.remove_brackets().sql_script.lower()
    
def split_table(table_name: Union[List, str], db: str = None, server: str = 'cso-sql01') -> pd.Series:
    """
    Splits a table name string into its component parts (server, database, schema, table).

    Handles different formats of table names separated by periods and assigns them
    to the corresponding columns. It also handles cases where the input is a list
    (taking the first element if the list is not empty) or None/NaN values.

    Args:
        table_name (Union[List, str]): The table name to split. It can be a string
            in the format 'server.database.schema.table', 'database.schema.table',
            'schema.table', or just 'table'. It can also be a list containing such a string
            as its first element.
        db (str, optional): The database name. Used when the table_name string
            does not explicitly include it. Defaults to None.
        server (str, optional): The server name. Used as the default server or
            updated based on the table name. Defaults to 'cso-sql01'.

    Returns:
        pd.Series: A pandas Series containing the extracted parts in the order:
            'server', 'db', 'parent_schema', 'parent_table'. Returns a Series of
            None values if the table_name is None, NaN, or an empty list, or if
            the table name format is not recognized.
    """
    default_schema = 'dbo'  # Default schema name

    if isinstance(table_name, list):
        if not table_name:
            return pd.Series([None] * 4)    
        table_name = table_name[0]
    
    if table_name is None or pd.isna(table_name):
        return pd.Series([None] * 4)

    parts = table_name.split('.')
    num_parts = len(parts)
    
    if num_parts == 4:
        return pd.Series(parts)
    elif num_parts == 3:
        if parts[0] == 'microsoftdynamicsax':
            server = 'cso-sql11'
        return pd.Series([server] + parts)
    elif num_parts == 2:
        return pd.Series([server, db, parts[0], parts[1]])
    else:
        return pd.Series([server, db, default_schema, parts[0]])


def recreate_directory(directory: str) -> None:
    """
    Deletes the specified directory if it exists and then recreates it.

    Args:
        directory (str): The path of the directory to recreate.
    """
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory) 