import pandas as pd
import os
import sys
from typing import Dict, Optional, List, Callable, Tuple, Any, Generator
from dataclasses import dataclass
import xml.etree.ElementTree as ET
import re

#For Notebooke Path
module_path = os.path.abspath(os.path.join('..','..'))
if module_path not in sys.path:
    sys.path.append(module_path)
from src.data_lineage.utility import tree_root, SQLScriptCleaner
from src.utility.utility import load_config, SQLConnection, read_sql_file

config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = load_config(config_path)

username:str = os.getlogin()
SSIS_FILE_DIRECTORY: str = config.get('ssis_export')

@dataclass
class Utility:
    """
    A utility class for handling SSIS file directory operations and executing SQL queries.

    Attributes:
        directory (str): The directory where SSIS files are stored.
        dtsx_folder_path (pd.DataFrame): DataFrame containing DTSX folder paths.

    Methods:
        run_query(query: str, dbname: str, server: str) -> pd.DataFrame:
            Executes a SQL query asynchronously and returns the result as a DataFrame.

        get_dtsx_folder() -> pd.DataFrame:
            Retrieves DTSX folder information from the database and constructs file paths.

        ensure_dtsx_folder_path() -> pd.DataFrame:
            Ensures the DTSX folder path DataFrame is populated, retrieving it if necessary.
    """

    directory: str = SSIS_FILE_DIRECTORY.format(username = username)
    dtsx_folder_path: Optional[pd.DataFrame] = None
    all_connection_dict: Optional[Dict[str, str]]= None


    def get_folder_list(self) -> List[str]:
        """
        Returns a list of subdirectories in the specified directory.

        Args:
            directory (str): The path of the directory to list subdirectories from.

        Returns:
            List[str]: A list of subdirectory paths.
        """
        return [os.path.join(self.directory, f) for f in os.listdir(self.directory) if os.path.isdir(os.path.join(self.directory, f))]

    def get_connet_manager_dict(self) -> Dict[str, str]:
        """
        Creates a dictionary mapping connection manager DTSIDs to ObjectNames from .conmgr files.

        This function iterates through the folders in the specified SSIS file directory,
        parses .conmgr files, and extracts the DTSID and ObjectName attributes to build
        a dictionary where DTSIDs are keys and ObjectNames are values.

        Args:
            ssis_file_directory (str): The directory containing SSIS package folders.

        Returns:
            Dict[str, str]: A dictionary mapping DTSIDs to ObjectNames.
        """

        def get_manage_connection(folder: str) -> Dict[str, str]:
            """
            Extracts connection manager DTSIDs and ObjectNames from .conmgr files in a folder.

            Args:
                folder (str): The path of the folder to process .conmgr files in.

            Returns:
                Dict[str, str]: A dictionary mapping DTSIDs to ObjectNames.
            """
            connect_dict: Dict[str, str] = {}
            for root, dirs, files in os.walk(folder):
                for file in files:
                    if file.endswith('.conmgr'):
                        file_path = os.path.join(root, file)
                        try:
                            tree_root_ = tree_root(file_path)
                            object_name = tree_root_.get('{www.microsoft.com/SqlServer/Dts}ObjectName')
                            connect_id = tree_root_.get('{www.microsoft.com/SqlServer/Dts}DTSID')

                            if connect_id and object_name:
                                if connect_id not in connect_dict: #check if connect_id is already in the dictionary
                                    connect_dict[connect_id] = object_name
                        except (FileNotFoundError, ET.ParseError) as e:
                            print(f"Error processing {file_path}: {e}")
                        except Exception as e:
                            print(f"An unexpected error occurred processing {file_path}: {e}")
            return connect_dict
        
        if self.all_connection_dict is None:
            self.all_connect_dict: Dict[str, str] = {}
            for folder in self.get_folder_list():
                self.all_connect_dict.update(get_manage_connection(folder))
        return self.all_connect_dict 

    async def get_dtsx_folder(self) -> pd.DataFrame:
        """
        Retrieves DTSX folder information from the database and constructs file paths.

        Returns:
            pd.DataFrame: DataFrame containing DTSX folder paths.
        """
        if self.dtsx_folder_path is None:
            sql_conn = SQLConnection(db_name = config['databasename'], server = config['servername'])
            SQL_PATH = os.path.join(os.path.dirname(__file__), 'ssis_execution.sql')
            base_query = read_sql_file(SQL_PATH)
            ignore_packages = config.get('ignore_package_list', [])
            formatted_packages = ', '.join([f"'{package}'" for package in ignore_packages])
            final_query = f'{base_query} AND a.package_name NOT IN ({formatted_packages})'
            df: pd.DataFrame = await sql_conn.run_query_aio(query = final_query)

            df['file_path'] = df.apply(
                lambda row: os.path.join(self.directory, row['project'], row['package_name']), axis = 1
                )

            self.dtsx_folder_path = df

        return self.dtsx_folder_path

    async def ensure_dtsx_folder_path(self) -> pd.DataFrame:
        """
        Ensures the DTSX folder path DataFrame is populated, retrieving it if necessary.

        Returns:
            pd.DataFrame: DataFrame containing DTSX folder paths.
        """
        if self.dtsx_folder_path is None:
            await self.get_dtsx_folder()
        return self.dtsx_folder_path
    

def get_root(
    df: pd.DataFrame,
    master_dtsx: str,
    dtsx_file: str
) -> ET.Element:
    """
    Retrieve the root element of an XML file for a given DTSX package.

    This function filters the provided DataFrame to find the file path of the specified DTSX package,
    parses the XML file at that path, and returns the root element of the XML tree.

    Args:
        df (pd.DataFrame): DataFrame containing DTSX package information, including file paths.
        master_dtsx (str): The name of the master DTSX package.
        dtsx_file (str): The name of the DTSX file to retrieve the root element for.

    Returns:
        ET.Element: The root element of the parsed XML tree.
    """
    filter_df = df.loc[
        (df['master_dtsx'] == master_dtsx) & (df['package_name']== dtsx_file),
        'file_path'
    ]
    file_path = filter_df.iloc[0]

    return tree_root(file_path)

def get_executables(root: ET.Element) -> List[ET.Element]:
    """
    Retrieves enabled DTS:Executable elements from an XML root element.

    Args:
        root (ET.Element): The root element of the XML document.

    Returns:
        List[ET.Element]: A list of enabled DTS:Executable elements.
    """
    enabled_executables = []
    
     # Iterate over all 'DTS:Executable' elements using the namespace
    for executable in root.findall('.//DTS:Executable', config['namespaces']):
        # Construct the fully qualified name for the 'Disabled' attribute
        disabled_attr_name = f'{{{config['namespaces']["dts"]}}}Disabled'
        # Check if the 'Disabled' attribute exists and is set to 'True'
        disabled_attr = executable.attrib.get(disabled_attr_name)
        if disabled_attr != 'True':
            enabled_executables.append(executable)
    return enabled_executables

async def parse_precedence_constraints(
        utility_instance: Utility,
        master_dtsx: str,
        dtsx_file: str
        ) -> Dict[str, str]:
    """
       Arg: dtsx File name, SQL Server Job: step related dtsx
       Return: Package Lineage
    """
    # Parse the XML file
    df = await utility_instance.ensure_dtsx_folder_path()
    root = get_root(df, master_dtsx, dtsx_file)
    
    # Find all PrecedenceConstraint elements
    constraints = root.findall('.//DTS:PrecedenceConstraint', config['namespaces'])
    
    # Extract and print information about each PrecedenceConstraint
    lineage = {}
    for constraint in constraints:
        from_task = constraint.get('{www.microsoft.com/SqlServer/Dts}From')
        to_task = constraint.get('{www.microsoft.com/SqlServer/Dts}To')
        if dtsx_file not in lineage:
            lineage[dtsx_file] = []
        lineage[dtsx_file].append( {'From': from_task, 'To': to_task} )
    return lineage

async def parse_dtsx_packages(
        utility_instance: Utility,
        master_dtsx: str,
        parent_dtsx_file: str
        ) -> Dict[str, Dict[str, str]]:
    df = await utility_instance.ensure_dtsx_folder_path()
    root = get_root(df, master_dtsx, parent_dtsx_file)
    # Find all Executable elements
    executables = get_executables(root)
    packages ={}
    for executable in executables:
        package_name = executable.get('{www.microsoft.com/SqlServer/Dts}ObjectName')

        ## Not all the package required. only executable type = Microsoft.ExecutePackageTask need
        executable_type = executable.get('{www.microsoft.com/SqlServer/Dts}ExecutableType')
        
        
        # Find the related DTSX file
        dtsx_file = None
        object_data = executable.find('.//DTS:ObjectData/ExecutePackageTask', config['namespaces'])
        if object_data is not None and (
            executable_type == 'Microsoft.ExecutePackageTask'):
            dtsx_file = object_data.find('PackageName').text

            if packages.get(parent_dtsx_file) is None:
                packages[parent_dtsx_file] = {package_name: dtsx_file}
            else:
                packages[parent_dtsx_file][package_name] = dtsx_file 
    return packages



async def find_source(utility_instance: Utility, master_dtsx: str ,dtsx_file: str) -> Dict:
    """
    Asynchronously analyzes a DTSX package file to extract source and destination information.

    This function parses the XML structure of a DTSX file, identifying data flow components
    (OLEDB Sources and Destinations, Execute SQL Tasks, and SSAS Processing Tasks) and extracting
    relevant details like database connections, table names, SQL statements, and SSAS target objects.

    Args:
        utility_instance: An instance of the Utility class providing access to DTSX file operations.
        master_dtsx: The path to the master DTSX file (used for context, not directly parsed).
        dtsx_file: The path to the DTSX file to be analyzed.

    Returns:
        A dictionary where keys are DTSX file paths and values are lists of dictionaries. Each
        inner dictionary represents a data flow operation and contains the following keys:
            'parent_db': The source database name (lowercase, brackets and 'dbo' removed).
            'child_db': The destination database name (lowercase, brackets and 'dbo' removed).
            'sql_statement': The SQL statement (cleaned) if applicable.
            'target_tables': The target table name(s) (lowercase, brackets and 'dbo' removed).
            'source_tables': The source table name(s) (lowercase, brackets and 'dbo' removed).
            For SSAS Processing Task:
            'child_db': Connection Name
            'target_tables': DimensionID or MeasureGroupID
    """

    df = await utility_instance.ensure_dtsx_folder_path()
    root = get_root(df, master_dtsx, dtsx_file)
    executables = get_executables(root)
    source_ = {}
    remove_string = 'Project.ConnectionManagers'

    def add_entry(source_: Dict, entry: Dict) -> None:
        if dtsx_file not in source_:
            source_[dtsx_file] = [entry]
        else:
            if entry not in source_[dtsx_file]:
                source_[dtsx_file].append(entry)
    
    for executable in executables:
        object_data = executable.find('.//DTS:ObjectData', config['namespaces'])
        executable_type = executable.attrib.get('{www.microsoft.com/SqlServer/Dts}ExecutableType')
        description = executable.attrib.get('{www.microsoft.com/SqlServer/Dts}Description')
        object_name = executable.attrib.get('{www.microsoft.com/SqlServer/Dts}ObjectName')
        #Initialise variables for SSAS Procssing Taks
        ssas_target = None
        connection_name = None
        # for executable in executables:
    
        #only real executable has copmonents
  
        if object_data is not None:
            components = executable.findall('.//component')
            ref_id = executable.attrib.get('{www.microsoft.com/SqlServer/Dts}refId')

        if description == 'Analysis Services Processing Task':
            
            for oj in object_data:
                processing_commands = oj.attrib.get('ProcessingCommands')
                if processing_commands:
                    processing_commands_root = ET.fromstring(processing_commands)
                    dimension_id_element = processing_commands_root.find('.//{http://schemas.microsoft.com/analysisservices/2003/engine}DimensionID')
                    measure_group_id_element = processing_commands_root.find('.//{http://schemas.microsoft.com/analysisservices/2003/engine}MeasureGroupID')
                    
                    ssas_target = (dimension_id_element.text if dimension_id_element is not None else None) or \
                                  (measure_group_id_element.text if measure_group_id_element is not None else None)
                    
                    connection_name = oj.attrib.get('ConnectionName')

                entry = {
                    'parent_db': None,
                    'child_db': connection_name,
                    'sql_statement': None,
                    'target_tables': ssas_target,
                    'source_tables': None,
                    'flag': 'SSAS Processing Task',
                    'object_name': object_name
                }
            add_entry(source_, entry)

        if executable_type == 'Microsoft.ExecuteSQLTask':
            # To Do: Need connection manager to get the database name
            sql_statement = object_data.find('.//SQLTask:SqlTaskData', config['namespaces']).attrib.get(
                '{www.microsoft.com/sqlserver/dts/tasks/sqltask}SqlStatementSource'
            )
            connectionid = object_data.find('.//SQLTask:SqlTaskData', config['namespaces']).attrib.get(
                '{www.microsoft.com/sqlserver/dts/tasks/sqltask}Connection'
            )
            clean_sql_statement = SQLScriptCleaner(sql_statement).clean().lower()
            if not clean_sql_statement.startswith('truncate table'):
                db = utility_instance.get_connet_manager_dict().get(connectionid)
                db = db.lower() if db else None

                entry = {
                    'parent_db': db,
                    'child_db': db,
                    'sql_statement': clean_sql_statement,
                    'target_tables': None,
                    'source_tables': None,
                    'flag': 'Execute SQL Task',
                    'object_name': object_name
                }
                add_entry(source_, entry)

        if components and description != 'Sequence Container':
        # if components:
            # Initialise variables to ensure they are always defined
            source_db = None
            destination_db = None
            sql_statement = None
            target_table = None
            connection_manager_ref_id = None
            source_table = None
            component_class_id = None
            
            for component in components:
                component_class_id = component.get('componentClassID')
                connection = component.find('.//connection')
                open_rowset = component.find(".//property[@name ='OpenRowset']")
                access_mode = component.find(".//property[@name='AccessMode']")
                
                if connection is not None:
                    connection_manager_ref_id = connection.get('connectionManagerRefId')
    
                if connection_manager_ref_id is not None and 'invalid' not in connection_manager_ref_id:
                    if component_class_id == 'Microsoft.OLEDBSource':
                        sql_command = component.find(".//property[@name='SqlCommand']")
                        # Definition: Access Mode: 0 = Table or View, 1 = Table or View name variable, 2 = SQL Command, 3 = Table name variable
                        sql_statement = sql_command.text if access_mode.text == '2' else None
                        source_db = connection_manager_ref_id.replace(remove_string, '')
                        source_db =  SQLScriptCleaner(source_db).remove_brackets().sql_script.lower()
                        if 'package.connectionmanagerslistofservers' in source_db:
                            source_table = 'each_pos_machine'
                        else:
                            source_table = open_rowset.text
                    elif component_class_id  == 'Microsoft.OLEDBDestination':
                        target_table = open_rowset.text
                        destination_db = connection_manager_ref_id.replace(remove_string, '')
                        destination_db = SQLScriptCleaner(destination_db).remove_brackets().sql_script.lower()
                    elif component_class_id == 'Microsoft.FlatFileSource':
                        #temp Solutino to define source_db and source_table
                        source_db = 'file_source'
                        source_table = 'file_system_table'
                    elif component_class_id == 'Microsoft.FlatFileDestination':
                        #temp Solutino to define destination_db and target_table
                        destination_db = 'file_destination'
                        target_table = 'file_system_table'
                    elif component_class_id == 'Microsoft.ExcelSource':
                        #temp Solutino to define source_db and source_table
                        source_db = 'excel_source'
                        source_table = 'excel_worksheet'

            #Name Conversion
            source_db = config['naming_convertion'].get(source_db, source_db)
            destination_db = config['naming_convertion'].get(destination_db, destination_db)
            new_object_name = ref_id[8:].replace('\\', ' -> ')
            entry = {
                # 'SourceDB': SQLScriptCleaner(source_db).remove_brackets().sql_script.lower(),
                # 'DestinationDB': SQLScriptCleaner(destination_db).remove_brackets().sql_script.lower(),
                'parent_db': source_db,
                'child_db': destination_db,
                #'SQL Statement': sql_statement,
                'sql_statement': SQLScriptCleaner(sql_statement).clean() if sql_statement else None,
                # 'Target Table': target_table,
                # 'Source Table': source_table,
                'target_tables': SQLScriptCleaner(target_table).remove_brackets().sql_script.lower(),
                'source_tables': SQLScriptCleaner(source_table).remove_brackets().sql_script.lower() if source_table else None,
                'flag': 'Data Flow Task',
                'object_name': new_object_name
            }
            add_entry(source_, entry)
            
    return source_


async def process_dtsx_list(
        utility_instance: Utility,
        dtsx_list: List,
        process_func: Callable[[Utility, str, str], Dict[str, str]]
        ) -> Dict[str, Dict[str, str]]:
    _dict_ = {}
    errors = {}  # Dictionary to store errors
    for k, v in dtsx_list.items():
        for dtsx in v:
            try:
                #Await the coroutine
                dict_ = await process_func(utility_instance, k, dtsx)
                if dict_ is not None:
                    _dict_ = dict_ | _dict_
            except Exception as e:
                error_key = f"{k}:{dtsx}"  # Create a unique key for the error
                errors[error_key] = str(e)  # Store the error message
                print(f"Error processing {error_key}: {e}") # Print the error immediately
    return _dict_


async def combined_together(utility_instance: Utility = Utility()) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Combines results from processing DTSX packages, grouped by folder and master DTSX.

    Args:
        utility_instance: An instance of the Utility class.

    Returns:
        A tuple of three nested dictionaries, each containing results from different
        processing functions (parse_precedence_constraints, parse_dtsx_packages, find_source).
        The outer keys represent folder names, and the inner keys represent master DTSX names.
    """
    a: Dict[str, Dict[str, str]] = {}
    b: Dict[str, Dict[str, str]] = {}
    c: Dict[str, Dict[str, str]] = {}

    dtsx_folder_df: pd.DataFrame = await utility_instance.ensure_dtsx_folder_path()

    # Two-level grouping
    # grouped = {}
    grouped = {
        folder: folder_group.groupby('master_dtsx')['package_name'].apply(list).to_dict()
            for folder, folder_group in dtsx_folder_df.groupby('project')
    }

    for folder, master_dtsx_packages in grouped.items():
        a[folder] = {}
        b[folder] = {}
        c[folder] = {}
        for master_dtsx, package_names in master_dtsx_packages.items():
            a[folder][master_dtsx] = await process_dtsx_list(
                utility_instance, {master_dtsx: package_names}, parse_precedence_constraints
            )
            b[folder][master_dtsx] = await process_dtsx_list(
                utility_instance, {master_dtsx: package_names}, parse_dtsx_packages
            )
            c[folder][master_dtsx] = await process_dtsx_list(
                utility_instance, {master_dtsx: package_names}, find_source
            )

    return a, b, c



def lineage(source_dict: Dict) -> pd.DataFrame:
    required_columns = [
        'project',
        'entry',
        'method_name',
        'parent_db',
        'child_db',
        # 'SQL Statement',
        'sql_statement',
        # 'Target Table',
        # 'Source Table',
        'target_tables',
        'source_tables',
        'flag',
        'object_name'
    ]
    sorting_columns = [
        'project',
        'entry',
        'method_name',
        'parent_db',
        # 'Source Table',
        'child_db',
        # 'Target Table',
        # 'SQL Statement',
        'sql_statement',
        'target_tables',
        'source_tables',
        'flag',
        'object_name'
    ]



    def generate_dataframe_rows(source_dict: Dict[str, Dict[str, Dict[str, List[Dict[str, Any]]]]]) -> Generator[List[Any], None, None]:
        """
        Generates rows for a pandas DataFrame from a deeply nested dictionary.

        Args:
            source_dict: A dictionary with three levels of nesting, where the innermost level
                        contains lists of dictionaries representing data for the DataFrame.

        Yields:
            A list representing a row of the DataFrame.
        """
        for k, v in source_dict.items():
            for subk, subv in v.items():
                for subsubk, subsubv in subv.items():
                    for d in subsubv:
                        yield [k, subk, subsubk, *d.values()]



 
    df = pd.DataFrame(generate_dataframe_rows(source_dict), columns = required_columns)
    df = df[sorting_columns]
    ## 2025-01-08 Temp Solution if SQL statement is not none then use table from sql statement instead of tables from openrowset
    df['source_tables'] = df.apply(lambda row: None if row['sql_statement'] is not None else row['source_tables'], axis = 1)
    df.drop_duplicates(inplace=True)
    
    return df

def extract_dtsx_sp(sql_statement: str) -> Optional[Tuple[str, str]]:
    """
    Extracts the stored procedure name and schema from a SQL EXEC statement.

    Args:
        sql_statement (str): The SQL statement to search.

    Returns:
        Optional[Tuple[str, str]]: A tuple containing the schema name and stored procedure name, or None if not found.
    """
    #pattern = r'(?:EXEC(?:UTE)?)\s+([\w\.]+)(?=\s+@|\s+|$)' #added @ to the lookahead
    pattern = r'(?:EXEC(?:UTE)?)\s+(?:@\w+\s?=\s?)?([\w\.]+)(?=\s+@|\s+|$)' #added @ to the lookahead
    result = re.findall(pattern, sql_statement, re.IGNORECASE)

    if not result:
        return None

    sp_full_name = result[0]
    parts = sp_full_name.split('.')

    if len(parts) == 2:
        if parts[0] in config['db_list']:
            schema_name = 'dbo'
        else:
            schema_name = parts[0]
        sp_name = parts[1]
    else:
        schema_name = 'dbo'
        sp_name = parts[0]

    return schema_name, sp_name

def find_dtsx_dependency_path(dependency_dict: Dict[str, Any], master_dtsx: str, child_dtsx: str) -> Optional[str]:
    """
    Finds the dependency path from a master DTSX to a child DTSX within a nested dictionary.

    Args:
        dependency_dict: The nested dictionary representing DTSX dependencies.
        master_dtsx: The starting DTSX.
        child_dtsx: The target DTSX.

    Returns:
        A string representing the dependency path, or None if not found.
    """

    def _find_path(current_dict: Dict[str, Any], current_path: List[Any], target_dtsx: str) -> Optional[List[str]]:
        """Recursive helper function to traverse the nested dictionary."""
        for key, value in current_dict.items():
            new_path = current_path + [key]
            if isinstance(value, dict):
                result = _find_path(value, new_path, target_dtsx)
                if result:
                    return result
            elif value == target_dtsx:
                return new_path + [value]
        return None

    if master_dtsx in dependency_dict:
        path = _find_path(dependency_dict[master_dtsx], [], child_dtsx)
        if path:
            parts = list(map(str, path))
            if parts[0] == master_dtsx:
                return " -> ".join(parts[1:])
            else:
                return " -> ".join(parts)
    return ''

def process_dtsx_dependency(dependency_dict, row) -> str:
    """
    Processes a single row of the DataFrame to find the dependency path.

    Args:
        dependency_dict: The nested dictionary representing DTSX dependencies.
        row: A row from the DataFrame with 'entry' and 'method_name' columns.

    Returns:
        A string representing the dependency path.
    """
    project_name = row['project']
    master_dtsx = row['entry']
    child_dtsx = row['method_name']

    return f"{project_name} -> {master_dtsx} -> {find_dtsx_dependency_path(dependency_dict[project_name], master_dtsx, child_dtsx)}"

def main():
    pass