import sys
import os
import asyncio
import pandas as pd
from typing import Dict, Union, List, Optional
import re
from src.utility.utility import (
    SQLConnection,
    read_sql_file,
    load_config
)
from src.utility.logger import get_logger
from src.data_lineage.utility import recreate_directory, UNVERSAL_TABLE_PATTERN, SQLScriptCleaner, table_lineage_from_sql
import subprocess
import json
import requests
from requests_ntlm  import HttpNtlmAuth
import time
#For Notebooke Path
module_path = os.path.abspath(os.path.join('..','..'))
if module_path not in sys.path:
    sys.path.append(module_path)

info_logger = get_logger(name = 'info_messages', level = 20)


CONFIG_PATH: str = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = load_config(CONFIG_PATH)


#Check ENV
if os.name == 'nt':  # 'nt' indicates Windows
    username = os.getlogin()
    DIRECTORY: str = config.get('pbix_export')
    DIRECTORY: str = DIRECTORY.format(username = username)
else:
    DIRECTORY: str = '/opt/pbix_export/'

SQL_PATH: str = os.path.join(os.path.dirname(__file__), 'pbix_content.sql')
sql_script: str = read_sql_file(SQL_PATH)
info_logger.info(f"SQL script loaded from {SQL_PATH}")
SAVE_PATH = config.get('excel_save_path').format(username = username)

async def extract_pbix_file(df: pd.DataFrame, concurrency_limit: int = 5) -> tuple[list[str], list[str]]: 
    """
    Extracts .pbix files from a DataFrame, downloads them, and asynchronously extracts their contents.

    This function iterates over rows in a DataFrame, downloads Power BI report files (.pbix)
    from a specified URL, and extracts the contents of each file into a separate directory.
    It handles duplicate report names (where reports have the same name but different paths)
    by creating subdirectories to avoid overwriting files.


    Args:
        df (pd.DataFrame): A DataFrame containing report information.
            The DataFrame must include the following columns:
            - 'name' (str): The name of the report.
            - 'itemid' (str): The item ID used in the download URL.
            - 'path' (str): The path of the report, used for handling duplicates.
            - 'isduplicate' (int): A flag indicating if the report name is a duplicate
              but has a different path (1 for duplicate, 0 for not).

    Returns:
        tuple[list[str], list[str]]: A tuple containing two lists:
            - extracted_folder_list (list[str]): A list of paths to the directories where the .pbix
              files were extracted.
            - root_path_list (list[str]): A list of the original 'path' values from the DataFrame,
              corresponding to each extracted folder.

    Data Hint:
    The input DataFrame `df` should have the following structure:

    | name       | itemid                                 | path                                     | isduplicate |
    |------------|----------------------------------------|------------------------------------------|-------------|
    | Report1    | 'xxxxxxxx-xxxx-xxxx-xxxxxxxxxxxxxxxxx' | '/Folder1/Report1'                       | 0           |
    | Report2    | 'xxxxxxxx-xxxx-xxxx-xxxxxxxxxxxxxxxxx' | '/Folder2/Report2'                       | 0           |
    | Report1    | 'xxxxxxxx-xxxx-xxxx-xxxxxxxxxxxxxxxxx' | '/Folder1/Report1_Duplicate'             | 1           |
    | Report3    | 'xxxxxxxx-xxxx-xxxx-xxxxxxxxxxxxxxxxx' | '/Folder3/Report3'                       | 0           |

    Where:
        - `name` is the name of the Power BI report.
        - `itemid` is a unique identifier for the report.
        - `path` is the location of the report.
        - `isduplicate` is 1 if the report name is a duplicate but has a different path, 0 otherwise.

    """
    extracted_folder_list: list[str] = []
    root_path_list: list[str] = []
    tasks: list[asyncio.Task] = []
    semaphore = asyncio.Semaphore(concurrency_limit)
    for _, row in df.iterrows():
        report_name: str = row['name']
        report_directory: str = DIRECTORY
        url: str = config.get('ssrs_server').format(itemid = row['itemid'])
        if row['isduplicate'] == 1:
            report_path: str = row['path'].replace('/', '_')
            report_directory: str = os.path.join(DIRECTORY, report_path)
            #Test Temp Remove
            recreate_directory(report_directory)
            info_logger.info(f"Directory recreated at {report_directory}")
        file_path: str = os.path.join(report_directory, f"{report_name}.pbix")
        extracted_folder_path: str = os.path.join(report_directory, report_name)
        response_status = download_file(
            url = url,
            file_path = file_path,
            windows_login_password = os.environ.get('WINDOWS_PASSWORD'),
            report_name = report_name,
            itemid = row['itemid']
        )

        if response_status == 200:
            ## Test Temp Remove
            task = asyncio.create_task(extract_pbix_async(file_path, semaphore, itemid = row['itemid']))
            tasks.append(task)
            ##############
            extracted_folder_list.append(extracted_folder_path)
            root_path_list.append(row['path'])

    pbi_start_process_time = time.time()
    ##Test Temp Remove
    await asyncio.gather(*tasks)  # wait for all tasks to complete
    info_logger.info("All PBIX files extracted successfully.")
    pbi_end_process_time = time.time() - pbi_start_process_time
    info_logger.info(f"PBIX extraction time: {pbi_end_process_time:.2f} seconds")
    return extracted_folder_list, root_path_list

async def extract_pbix_async(pbix_file_path: str, semaphore: asyncio.Semaphore, itemid) -> None:
    """
    Asynchronously extracts the contents of a Power BI Desktop (.pbix) file using pbi-tools via PowerShell.

    Args:
        pbix_file_path (str): The full path to the .pbix file to extract.
        logger (Optional[Logger], optional): An optional logger instance to record messages.
                                            Defaults to None, in which case messages are printed to stdout.

    Raises:
        subprocess.CalledProcessError: If the pbi-tools extract command returns a non-zero exit code.
        FileNotFoundError: If PowerShell is not found on the system.
        Exception: For any other unexpected errors during the process.

    Returns:
        None
    """
    
    try:
        async with semaphore:
            command = f"pbi-tools extract '{pbix_file_path}'"
            info_logger.info(f"Executing command: {command}")
            process = await asyncio.create_subprocess_exec(
                'powershell', '-Command', command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_output = stderr.decode()
                info_logger.error(f"Error executing command: {command} for itemid {itemid}")
                info_logger.error(f"Stderr: {error_output}")
                
            else:
                info_logger.info(f"Command executed successfully: {command}")
                info_logger.info(stdout.decode())

    except FileNotFoundError:
        info_logger.error("PowerShell not found. Please ensure it is installed and available in your PATH.")
        raise
    except subprocess.CalledProcessError as e:
        info_logger.error(f"Error executing command: {e.cmd}")
        raise
    except Exception as e:
        info_logger.error(f"An unexpected error occurred: {e}")
        raise

def check_for_connections_file(folder_path: str) -> bool:
    """
    Checks if a 'Connections.json' file exists within the specified folder.

    This file is often present in the extracted contents of a Power BI
    .pbix file and can provide hints about the data connectivity mode.

    Args:
        folder_path (str): The path to the folder to check. This is typically
                           the root directory of an extracted .pbix file.

    Returns:
        bool: True if 'Connections.json' exists in the folder, indicating
              potential connection information; False otherwise.
    """
    connection_file = os.path.join(folder_path, 'Connections.json')
    exists = os.path.exists(connection_file)
    info_logger.info(f"Connections Exists in {folder_path}: {exists}")
    return exists

def check_for_m_language_file(folder_path: str) -> bool:
    """
    Checks if a 'M Language' file exists within the specified folder.
    
    This file is often present in the extracted contents of a Power BI
    .pbix file and can provide hints about the data connectivity mode.

    Args:
        folder_path (str): The path to the folder to check. This is typically
                           the root directory of an extracted .pbix file.

    Returns:
        bool: True if 'M Language' file exists in the folder, indicating
              potential connection information; False otherwise.
    Where:
        Objective-C Source File Extension:
            - .m: Objective-C source code files are typically saved with the .m extension.
    """
    m_language_file = os.path.join(folder_path, 'Mashup', 'Package', 'Formulas', 'Section1.m')
    exists = os.path.exists(m_language_file)
    info_logger.info(f"M Language Exists in {folder_path}: {exists}")
    return exists

def check_for_model_file(folder_path: str) -> bool:
    """
    Checks if the 'model.tmdl' file exists within the 'Model' subdirectory
    of the specified folder path.

    Args:
        folder_path: The path to the main folder to check within.

    Returns:
        True if the 'model.tmdl' file exists in the expected location, False otherwise.
    """
    model_file = os.path.join(folder_path, 'Model', 'model.tmdl')
    exists = os.path.exists(model_file)
    info_logger.info(f'model file Exists in {folder_path}: {exists}')
    return exists

def extract_tables_from_model_tmdl(tmdl_file: str) -> list[str]:
    """
    Extracts the list of table names from the PBI_QueryOrder annotation
    in a Tabular Model Definition Language (.tmdl) file, handling potential BOM.

    Args:
        file_name: The path to the .tmdl file.

    Returns:
        A list of table names extracted from the PBI_QueryOrder annotation,
        or an empty list if the annotation is not found or the file cannot be read.
    """
    
    with open(tmdl_file, 'r', encoding='utf-8-sig') as f:
        content = f.read()
    
    match = re.search(r'annotation PBI_QueryOrder = \[(.*?)\]', content)
    if match:
        table_string = match.group(1)
        all_tables = [
            table.strip().strip('"') for table in table_string.split(',')
        ]
        ## Exclude Error Load Table
        tables = [table for table in all_tables if not table.startswith('Errors in')]

    info_logger.info(f'Extract Table List from {tmdl_file}: {tables}')
    return tables

def extract_partition_info(tmdl_content: str) -> Optional[Dict[str, str]]:
    """
    Extracts server name, database name, schema name, object name, and query
    from a partition definition within TMDL content, handling two M language source patterns.

    Args:
        tmdl_content: A string containing the content of a TMDL file
                      or a relevant section containing a partition definition.

    Returns:
        A dictionary containing the extracted information.  The dictionary
        will contain different keys depending on the M language pattern:

        - Pattern 1: 'server_name', 'db_name', 'schema_name', 'object_name'
        - Pattern 2: 'server_name', 'db_name', 'query', 'table_match'
        Returns None if the necessary information cannot be found.
    """
    navigation_partition_pattern = re.search(
        r'mode:\s+import\s+source\s*=\s*let\s+'
        r'Source\s*=\s*Sql\.Databases\("([^"]*)"\),\s+'
        r'([^ ]+)\s*=\s*Source\{\[Name="([^"]*)"\]\}\[Data\],\s+'
        r'([^ ]+)\s*=\s*\2\{\[Schema="([^"]*?)",Item="([^"]*?)"\]\}\[Data\]',
        tmdl_content,
        re.DOTALL
    )

    select_partition_pattern = re.search(
        r'mode:\s+import\s+source\s*=\s*let\s+'
        r'Source\s*=\s*Sql\.Database\("([^"]*)",\s*"([^"]*)",\s*\[Query="([^"]*)"\]\)',
        tmdl_content,
        re.DOTALL
    )

    if navigation_partition_pattern:
        info_logger.info(f'M Language Content: {navigation_partition_pattern.groups()}')
        server_name = navigation_partition_pattern.group(1)
        db_variable = navigation_partition_pattern.group(2)
        db_name = navigation_partition_pattern.group(3)
        schema_variable = navigation_partition_pattern.group(4)
        schema_name = navigation_partition_pattern.group(5)
        object_name = navigation_partition_pattern.group(6)

        result = {
            "server": server_name.lower(),
            "db": db_name.lower(),
            "parent_schema": schema_name.lower(),
            "object_name": object_name.lower(),
        }
        info_logger.info(f'tmdl: {result}')
        return [result]

    elif select_partition_pattern:
        info_logger.info(f'M Language Content: {select_partition_pattern.groups()}')
        server_name = select_partition_pattern.group(1)
        db_name_from_call = select_partition_pattern.group(2)
        m_query = select_partition_pattern.group(3)
        m_query = clean_m_language_query(m_query)
        query = SQLScriptCleaner(m_query).clean()
        table_matches = UNVERSAL_TABLE_PATTERN.findall(query)
        results: list = []
        if table_matches:
            for match in table_matches:
                parts = match.split(',')
                num_parts = len(parts)
                extracted_table_name = parts[-1].lower()
                table_parts = extracted_table_name.split('.')
                table_parts_len = len(table_parts)
                extracted_db_name = table_parts[0] if table_parts_len == 3 else db_name_from_call.lower()
                extracted_schema_name = table_parts[0] if table_parts_len == 2 else (parts[-2] if num_parts == 2 else 'dbo')
                result = {
                    'server': server_name.lower(),
                    'db': extracted_db_name.lower(),
                    'parent_schema': extracted_schema_name.lower(),
                    'object_name': table_parts[-1].lower()
                }
                info_logger.info(f'SELECT Pattern Query {query}: result: {result}')
                results.append(result)
            
            info_logger.info(f'Comprehesive result: {results}')
        return results

    else:
        return None

def clean_m_language_query(m_query: str) -> str:
    """
    Cleans up an M language SQL query string to be more readable
    by removing M-specific formatting like '#(lf)' and extra spaces.

    Args:
        m_query: The M language SQL query string.

    Returns:
        A cleaned-up SQL query string.
    """

    cleaned_query = m_query.replace('#(lf)', '\n').strip()
    info_logger.info(f'Cleaned Query: {cleaned_query}')
    return cleaned_query  

def extract_model_detail(folder_path) -> list[Union[str, Dict[str, str]]]:

    model_tmdl = os.path.join(folder_path, 'Model', 'model.tmdl')
    table_list = extract_tables_from_model_tmdl(model_tmdl)
    result = []
    table_dict = None
    for table in table_list:
        table_tmdl = os.path.join(folder_path, 'Model', 'tables',f'{table}.tmdl')
        info_logger.info(f'table.tmdl path: {table_tmdl}')
        try:
            with open(table_tmdl, 'r', encoding= 'utf-8-sig') as f:
                tmdl_content = f.read()
        except FileNotFoundError:
            tmdl_content = None
            info_logger.info(f'Not Found: {table_tmdl}')

        if tmdl_content:
            table_dict = extract_partition_info(tmdl_content)

        if table_dict:
            info_logger.info(f'{table} has Dict: {table_dict}')
        else:
            info_logger.info(f'{table} has no Dict')

        if isinstance(table_dict, list):
            for _ in table_dict:
                result.append(_)
        else:
            result.append(table_dict)

    return result

def extract_connection_details(json_file: str) -> Dict[str, Union[str, None]]:
    """
    Extracts server, database name, and object name from a Connections.json file.

    Args:
        json_file (str): The path to the Connections.json file.

    Returns:
        dict: A dictionary containing the extracted 'server', 'db_name', and
              'object_name'. Returns an empty dictionary if the expected
              information cannot be found.
    """
    result: Dict[str, Optional[str]] = {}
    
    with open(json_file, 'r') as f:
        connections_data = json.load(f)
        if 'Connections' in connections_data and connections_data['Connections']:
            connection = connections_data['Connections'][0]
            if 'ConnectionString' in connection:
                connection_string = connection['ConnectionString']
                parts = connection_string.split(';')
                for part in parts:
                    if part.lower().startswith('data source='):
                        result['server'] = part.split('=')[1]
                    elif part.lower().startswith('initial catalog='):
                        result['db_name'] = part.split('=')[1].strip('"')
                    elif part.lower().startswith('cube='):
                        result['object_name'] = part.split('=')[1].strip('"')

    return result


def download_file(
        url: str,
        file_path: str,
        windows_login_password: str,
        report_name: str,
        itemid: str
    ) -> None:
    """Downloads a Power BI Desktop (.pbix) file from an SSRS server using
    Windows NTLM authentication.

    Args:
        ssrs_report_url: The specific URL on the SSRS server to download the
                         .pbix file content (str). This typically looks like:
                         'http://<your_ssrs_server>/reports/api/v2.0/catalogitems(item_id)/Content/$value'.
        local_file_path: The full local path where the downloaded .pbix file
                         will be saved (str), including the filename with the
                         '.pbix' extension.
        windows_login_password: The password for the current Windows user to
                               authenticate with the SSRS server (str).
    """
    username = os.getlogin()
    auth = HttpNtlmAuth(username, windows_login_password)
    response = requests.get(url, auth = auth, stream = True)

    ## Test Temp Remove
    if response.status_code == 200:
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size = 8192):
                file.write(chunk)
        info_logger.info(f"{report_name} downloaded successfully: {file_path}")
    elif response.status_code == 401:
        info_logger.error(f"Authentication failed for {report_name} - check your credentials.")
    elif response.status_code == 404:
        info_logger.error(f"File not found: {url}")
    elif response.status_code == 403:
        info_logger.error(f'Insufficent Permission: {report_name}')
    else:
        info_logger.error(f"Failed to download file: {itemid} - Status code: {response.status_code}")
    #######
    return response.status_code


def metadata_extraction(folder_info: tuple[list[str], list[str]]) -> pd.DataFrame:
    """
    Extracts metadata from a list of folders and returns it as a Pandas DataFrame.

    Args:
        folder_info (tuple[list[str], list[str]]): A list of folder paths to process.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted 'folder_path',
                      'report_name', 'server_name', 'db_name', and 'object_name'.
    """
    
    def extract_data_lineage_from_model_tmdl() -> None:
        extract_model_list = extract_model_detail(folder)

        if all(item is None for item in extract_model_list):
            info_logger.info(f'One Time Report: {report_name} : {root}')
        else:
            info_logger.info(f'Extracting.. {report_name} from {root}, List of Dict: {extract_model_list}')
            for source_info in extract_model_list:
                if source_info:
                    info_logger.info(f'Souce Info: {source_info}')
                    metadata = {
                        'execute_path': root,
                        'name': report_name,
                        'server': source_info.get('server'),
                        'db': source_info.get('db'),
                        'object_name': source_info.get('object_name'),
                        'object_type': 'table',
                        'parent_schema': source_info.get('parent_schema')
                    }
                    info_logger.info(f'Read Sucessfully model tmdl {metadata}')
                    all_metadata.append(metadata)
                    
    all_metadata = []
    folder_list, root_path_list = folder_info
    for folder, root in zip(folder_list, root_path_list):
        report_name = os.path.basename(folder)

        if check_for_connections_file(folder) == True:
            connection_file = os.path.join(folder, 'Connections.json')
            connection_details = extract_connection_details(connection_file)
            metadata = {
                'execute_path': root,
                'name': report_name,
                'server': connection_details.get('server'),
                'db': connection_details.get('db_name'),
                'object_name': connection_details.get('object_name'),
                'object_type': 'cube',
                'parent_schema': None
            }
            all_metadata.append(metadata)
            if connection_details:
                info_logger.info(f"Connection details found: {connection_details}  in {folder}")
            elif check_for_model_file(folder) == True:
                extract_data_lineage_from_model_tmdl()
            else:
                info_logger.info(f"No connection details found in {connection_file}.")
        elif check_for_m_language_file(folder) == True:
            m_language_file = os.path.join(folder, 'Mashup', 'Package', 'Formulas', 'Section1.m')
            info_logger.info(f"M Language file found: {m_language_file} in {folder}")
            with open(m_language_file, 'r') as f:
                m_query = f.read()
                info_logger.info(f'Read Successfully: {m_language_file}')
                extracted_info_list = extract_m_laungaue_source(m_query)
                info_logger.info(f"Extracted M language source: {extracted_info_list}")
                for source_info in extracted_info_list:
                    metadata = {
                        'execute_path': root,
                        'name': report_name,
                        'server': source_info.get('server'),
                        'db': source_info.get('db'),
                        'object_name': source_info.get('object_name'),
                        'object_type': source_info.get('object_type'),
                        'parent_schema': source_info.get('parent_schema')
                    }
                    all_metadata.append(metadata)

        elif check_for_model_file(folder) == True:
            extract_data_lineage_from_model_tmdl()

        else:
            info_logger.info(f"Need further def to extract metadata from {report_name} in {folder}")
        
    df = pd.DataFrame(all_metadata)
    df.drop_duplicates(inplace= True)
    # df.to_excel(SAVE_PATH, index = False)
    info_logger.info(f"Metadata extracted and saved to {SAVE_PATH}")
    return df

def extract_m_laungaue_source(m_code: str) -> List[Dict[str, Optional[str]]]:
    """
    Extracts server, database, schema, object name, and object type
    from pbix calls within M language code.

    Args:
        m_code: A string containing M language code.

    Returns:
        A list of dictionaries, where each dictionary contains the extracted
        information for a single 'Source = Sql.Database(...)' call.
        Each dictionary has the keys:
        'server': Server name (lowercased),
        'db': Database name (lowercased),
        'parent_schema': Schema name (default 'dbo'),
        'object_name': Object name,
        'object_type': 'table' or 'sp',
        'query': the cleaned query.
        Values are strings or None if not found.
    """
    extracted_info_list: List[Dict[str, Optional[str]]] = []
    source_matches = re.findall(r'Source\s+=\s+Sql\.Database\("([^"]+)",\s+"([^"]+)",\s+\[Query="([^"]+)"\]\),', m_code, re.DOTALL)
    for source_match in source_matches:
        server_name = source_match[0]
        db_name = source_match[1]
        raw_m_query = source_match[2]
        info: Dict[str, Optional[str]] = {
            'server': None,
            'db': None,
            'parent_schema': 'dbo',
            'object_name': None,
            'object_type': None,
            'query': None
        }
        if server_name and db_name:
            info['server'] = server_name.lower()
            info['db'] = db_name.lower()
            m_cleaned_query = clean_m_language_query(raw_m_query) if raw_m_query else None
            cleaned_query = SQLScriptCleaner(m_cleaned_query).clean().lower()
            info['query'] = cleaned_query
            if cleaned_query:
                sp_match = re.search(
                    r'(?:EXECUTE)\s+([\w\d\-]+(?:.[\w\d\-]+)?)?(?:\.([\w\d\-]+))?',
                    cleaned_query,
                    re.IGNORECASE
                )
                if sp_match:
                    info_logger.info(f'sp_match: {sp_match.groups()}')
                    parts = sp_match.group(1).split('.')
                    schema = parts[0] if len(parts) == 2 else 'dbo'
                    object_name = parts[-1]
                    info['parent_schema'] = schema
                    info['object_name'] = object_name
                    info['object_type'] = 'sp'
                else:
                    info['object_type'] = 'table'
                    table_list = table_lineage_from_sql(cleaned_query)[1]
                    for table in table_list:
                        parts = table.split('.')
                        len_part = len(parts)
                        if len_part == 3:
                            info['db'] = parts[0]
                        info['parent_schema'] = parts[0] if len_part == 2 else 'dbo'
                        info['object_name'] = parts[-1]
                        extracted_info_list.append(info)

        extracted_info_list.append(info)

    return extracted_info_list


async def main() -> pd.DataFrame:
    recreate_directory(DIRECTORY)
    info_logger.info(f"Directory recreated at {DIRECTORY}")
    sql_conn = SQLConnection(
        db_name=config['report_databasename'],
        server=config['report_servername']
    )
    df = await sql_conn.run_query_aio(sql_script)
    info_logger.info("SQL query executed successfully.")
    pbix_folder_list: tuple[list[str], list[str]] = await extract_pbix_file(df)
    info_logger.info(f"PBIX files extracted successfully, pbix_folder_list: {pbix_folder_list}")
    f_df = metadata_extraction(pbix_folder_list)
    info_logger.info("Metadata extraction completed.")
    print('PBIX Done')
    return f_df

if __name__ == "__main__":
    asyncio.run(main())