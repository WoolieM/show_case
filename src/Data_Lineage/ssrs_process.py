import xml.etree.ElementTree as ET
from typing import Dict, Optional, List
import re
import sys
import os
import asyncio
import pandas as pd
import time
#For Notebooke Path
module_path = os.path.abspath(os.path.join('..','..'))
if module_path not in sys.path:
    sys.path.append(module_path)

from src.utility.utility import (
    SQLConnection,
    read_sql_file,
    load_config
)
from src.data_lineage.utility import  SQLScriptCleaner, split_table
from src.utility.logger import get_logger
from src.data_lineage.pbix_process import main as pbix_main
debug_logger = get_logger(name='debug_messages', level = 10 )
info_logger = get_logger(name = 'info_messages', level = 20)



username = os.getlogin()


CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = load_config(CONFIG_PATH)
SQL_PATH = os.path.join(os.path.dirname(__file__), 'ssrs.sql')
sql_script = read_sql_file(SQL_PATH)
SAVE_PATH = config.get('excel_save_path').format(username = username)




def _extract_server_db(connect_string: str) -> Dict[str, Optional[str]]:
    """Extracts server and database from a connection string."""
    if not connect_string:
        return {'server': None, 'database': None}
    server_match = re.search(r'Data Source=([^;]+)', connect_string)
    database_match = re.search(r'Initial Catalog="?([^;"]+)"?', connect_string)
    return {
        'server': server_match.group(1) if server_match else None,
        'database': database_match.group(1) if database_match else None,
    }


def find_data_source(root: ET.Element, namespaces: Dict[str, str]) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Extracts data source information from an XML element tree.

    Args:
        root: The root element of the XML tree.
        namespaces: A dictionary containing namespace prefixes and their corresponding URLs.

    Returns:
        A dictionary where keys are data source names and values are dictionaries containing server and database info.
    """
    data_sources: Dict[str, Dict[str, Optional[str]]] = {}
    for ds_elem in root.findall('.//def:DataSource', namespaces):
        name = ds_elem.get('Name')
        connect_elem = ds_elem.find('.//def:ConnectString', namespaces)
        ref_elem = ds_elem.find('.//def:DataSourceReference', namespaces)

        if connect_elem is not None:
            data_sources[name] = _extract_server_db(connect_elem.text)
        elif ref_elem is not None:
            data_sources[name] = {'server': 'cso-sql01', 'database': ref_elem.text.split('/')[-1]}
        else:
            data_sources[name] = {}
    return data_sources

def parse_datasets(xml_data: str, report_type: str) -> List[Dict[str, Optional[str]]]:
    """
    Parses XML and extracts dataset information.

    Args:
        xml_data: The XML data as a string.
        report_type: The type of report ('report' or 'dataset').

    Returns:
        A list of dictionaries, where each dictionary represents a dataset.
    """
    root = ET.fromstring(xml_data)
    ns: Dict[str, str] = {'def': root.tag.split('}')[0].strip('{')}
    data_sources = find_data_source(root, ns) if report_type == 'report' else {}

    datasets: List[Dict[str, Optional[str]]] = []
    for ds_elem in root.findall('.//def:DataSet', ns):
        name = ds_elem.get('Name')
        ds_name_elem = ds_elem.find('.//def:DataSourceName', ns)
        cmd_text_elem = ds_elem.find('.//def:CommandText', ns)
        cmd_type_elem = ds_elem.find('.//def:CommandType', ns)
        shared_ds_elem = ds_elem.find('.//def:SharedDataSetReference', ns)
        ds_ref_elem = ds_elem.find('.//def:DataSourceReference', ns)

        server_db = data_sources.get(ds_name_elem.text) if report_type == 'report' and ds_name_elem is not None else (
            {'database': ds_ref_elem.text.split('/')[-1].lower()} if report_type != 'report' and ds_ref_elem is not None else None
        )

        # sql:str = ' '.join(cmd_text_elem.text.split()) + ' ' if cmd_text_elem is not None else None
        sql:str = cmd_text_elem.text if cmd_text_elem is not None else None
        clean_sql:str = SQLScriptCleaner(sql).remove_brackets().remove_comments().sql_script.lower() if sql else ''

        datasets.append({
            'dataset_name': name,
            'server': server_db.get('server').lower() if server_db and server_db.get('server') else 'cso-sql01',
            'db': server_db.get('database').lower() if server_db and server_db.get('database') else (server_db.get('database') if server_db else None),
            'sql': clean_sql,
            'shared_data_set': shared_ds_elem.text[1:].replace('/', ' -> ') if shared_ds_elem is not None else None,
            'command_type': 'sp' if cmd_type_elem is not None or clean_sql.startswith('exec') else ('SSAS' if re.search(r'select non empty', clean_sql, re.IGNORECASE) else 'sql')
        })

    return datasets


def extract_tablenames(sql_query: str) -> List[str]:
    """
    Extracts table names from an SQL query.

    This function searches for table names following SQL keywords like FROM, JOIN,
    UPDATE, INTO, MERGE, and USING. It handles table names enclosed in parentheses
    and returns a set of unique table names.

    Args:
        sql_query: The SQL query string from which to extract table names.

    Returns:
        A set of unique table names found in the SQL query.
    """
    pattern = r"\b(?:FROM|JOIN|UPDATE|INTO|MERGE|USING|EXEC)\s+([\w\d\-\.]+)"
    matches = re.findall(pattern, sql_query, re.IGNORECASE)
    table_list = set()

    if matches:
        for match in matches:
            table = match.strip()
            ## Handle Subquery e.g. FROM( or table_name)
            table = table.replace('(', '').replace(')', '')
            table_list.add(table)
        
    else:
        return []
    return list(table_list)

def get_object_list(row) -> List:
    """
    Extracts table names from SQL queries or stored procedure names.

    Args:
        row (pd.Series): A row from a DataFrame containing 'sql' and 'command_type' columns.

    Returns:
        list:   A list of table names if 'command_type' is 'sql', 
                a list containing the stored procedure name if 'command_type' is 'sp',
                or an empty list otherwise.
    """
    if row['shared_data_set']:
        return [row['shared_data_set']]
    elif row['command_type'] == 'sql':
        return extract_tablenames(row['sql'])  
    elif row['command_type'] == 'sp':
        sql = row['sql']
        if sql.startswith('exec'):
            return [sql.replace('exec ', '')]
        else:
            return [sql]
    else:
        return []
    


def get_shared_data_set(main_df: pd.DataFrame) -> pd.DataFrame:
    """"
    Extracts shared data set information from the main DataFrame.
    This function filters the main DataFrame to include only rows with shared data sets,
    Arguments:
        main_df (pd.DataFrame): The main DataFrame containing report data.
    Returns:
        pd.DataFrame: A DataFrame containing shared data set information.
    
    """
    
    df = main_df[main_df['shared_data_set'].notna()]
    df = df.copy()
    df['server'] = 'cso-biapp03'
    df['db'] = 'reportserver'
    df['object_list'] = df['shared_data_set']
    df['object_type'] = 'shared_dataset'
    df['parent_schema'] = 'dbo'

    return df

def get_sp_data(main_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts stored procedure information from the main DataFrame.
    This function filters the main DataFrame to include only rows with stored procedures,
    Arguments:
        main_df (pd.DataFrame): The main DataFrame containing report data.
    Returns:
        pd.DataFrame: A DataFrame containing stored procedure information.
    
    """
    
    df = main_df[main_df['command_type'] == 'sp']
    df = df.copy()
    df['object_type'] = 'sp'
    df[
        ['parent_schema', 'object_list']
    ] = df['object_list'].apply(
            lambda x: pd.Series(x.split('.')) if len(x.split('.')) == 2 else pd.Series(['dbo', x])
            )

    return df

def get_sql_df(main_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts SQL query information from the main DataFrame.
    This function filters the main DataFrame to include only rows with SQL queries,
    Arguments:
        main_df (pd.DataFrame): The main DataFrame containing report data.
    Returns:
        pd.DataFrame: A DataFrame containing SQL query information.
    
    """
    
    df = main_df[(main_df['command_type'] == 'sql') & main_df['shared_data_set'].isna()]
    df = df.copy()
    df['object_type'] = 'table'
    df[['server', 'db','parent_schema', 'object_list']] = df.apply(
        lambda row: split_table(row['object_list'], row['db']), axis=1
    )

    return df

def get_ssas_df(main_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts SSAS query information from the main DataFrame.
    This function filters the main DataFrame to include only rows with SSAS queries,
    Arguments:
        main_df (pd.DataFrame): The main DataFrame containing report data.
    Returns:
        pd.DataFrame: A DataFrame containing SSAS query information.
    
    """
    
    df = main_df[main_df['command_type'] == 'SSAS']
    df = df.copy()
    #temp solution for ssas

    df['object_list'] = df['dataset_name']
    df['object_type'] = 'cube'
    df['parent_schema'] = 'dbo'

    return df

def main() -> None:
    info_logger.info('Start')
    sql_conn= SQLConnection(db_name = config['report_databasename'], server= config['report_servername'])
    upload_conn = SQLConnection(db_name = config['to_databasename'], server= config['to_servername'])
    
    COLUMNS_TO_KEEP = [
        'name',
        'server',
        'db',
        'parent_schema',
        'object_list',
        'execute_path',
        'object_type'
    ]
    
    df = asyncio.run(sql_conn.run_query_aio(sql_script))
    # Exclude BI WIP and Z Archive paths
    df = df[
        ~df['execute_path'].str.startswith('BI WIP') &
        ~df['execute_path'].str.startswith('Z Archive')
    ]
    
    df['content'] = df.apply(lambda row: parse_datasets(row['reportdefinition'], row['type']), axis=1)
    df = df.explode('content', ignore_index = True)

    # Temp Remove Null Contnet
    df = df[df['content'].notna()]

    for col in ['dataset_name', 'server', 'db', 'sql', 'command_type', 'shared_data_set']:
        df[col] =df['content'].apply(lambda x: x.get(col))


    df['object_list'] = df.apply(get_object_list, axis=1)

    # Create a boolean mask for the rows to be removed
    mask = (df['command_type'] == 'sql') & (df['object_list'].apply(len) == 0)

    df.drop(df[mask].index, inplace = True)

    df=df.explode('object_list', ignore_index = True)

    shared_dataset_df = get_shared_data_set(df)
    info_logger.info('shared_dataset Complete')
    sp_df = get_sp_data(df)
    info_logger.info('sp dataset Complete')
    sql_df = get_sql_df(df)
    info_logger.info('sql dataset Complete')
    merged_df = pd.concat([shared_dataset_df[COLUMNS_TO_KEEP], sp_df[COLUMNS_TO_KEEP], sql_df[COLUMNS_TO_KEEP]], ignore_index=True)
    info_logger.info('Merge Three Datasets')
    merged_df.rename( 
        columns = {
            'object_list': 'object_name'
        },
        inplace = True
    )
    #give report type name
    merged_df['report_type'] = 'ssrs'

    info_logger.info('Change Column Name')
    pbix_df = asyncio.run(pbix_main())
    pbix_df['report_type'] = 'powerbi'
    info_logger.info('pbix dataset Complete')
    merged_df = pd.concat(
        [
            merged_df[merged_df.columns.to_list()],
            pbix_df[merged_df.columns.to_list()]
        ],
        ignore_index=True
    )
    info_logger.info('Merge pbix dataset')

    # merged_df.to_excel(SAVE_PATH, index=False, engine='openpyxl')
    # info_logger.info('Save to Excel')
    
    upload_conn.upload(merged_df, to_service_table_name= 'ssrs_data_lineage')
    info_logger.info('upload Complete')

if __name__ == "__main__":
    main()