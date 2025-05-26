import os

import zipfile
import asyncio
import sys
from typing import List, Any
from src.utility.utility import SQLConnection, load_config
import pandas as pd
from src.data_lineage.utility import recreate_directory
from src.utility.logger import get_logger

info_logger = get_logger(name='info_messages', level=20)
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = load_config(CONFIG_PATH)
DATABASE = 'SSISDB'

CONN = SQLConnection(db_name = DATABASE, server= config.get('default_server'))


#Check ENV
if os.name == 'nt':  # 'nt' indicates Windows
    username = os.getlogin()
    DIRECTORY: str = config.get('ssis_export').format(username = username)
else:
    DIRECTORY: str = '/opt/ssis_export/'

async def extract_package_full_permission() -> None:
    """
    Extracts SSIS project packages from the catalog and saves them to the specified directory.

    This function performs the following steps:
    1. Queries the catalog to get distinct folder and project names.
    2. Iterates over each folder and project name.
    3. Deletes the existing folder if it exists and recreates it.
    4. Executes a stored procedure to get the project binary data.
    5. Saves the binary data to a file in the specified directory.

    Args:
        None

    Returns:
        None
    """
    folder_df: pd.DataFrame = await CONN.run_query_aio(
        """
        SELECT DISTINCT
            folder_name,
            project_name
        FROM
            catalog.executions
        """
    )
    
    for _, row in folder_df.iterrows():
        # Define the folder path
        folder_path: str = os.path.join(DIRECTORY, row['folder_name'])
        
        # Recreate the folder
        os.makedirs(folder_path, exist_ok=True)
        
        #Construct the query
        query: str = f"EXEC ssisdb.catalog.get_project '{row['folder_name']}', '{row['project_name']}'"
        
        # Run the query and get the binary data
        df: pd.DataFrame = await CONN.run_query_aio(query)
        binary: Any = df.iat[0, 0]
        
        # Construct the file path and save the binary data to the file
        file_path: str = os.path.join(folder_path, f"{row['project_name']}.ispac")
        with open(file_path, 'wb') as file:
            file.write(binary)

def get_folder_list(directory: str) -> List[str]:
    """
    Returns a list of subdirectories in the specified directory.

    Args:
        directory (str): The path of the directory to list subdirectories from.

    Returns:
        List[str]: A list of subdirectory paths.
    """
    return [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isdir(os.path.join(directory, f))]

def extract_ispac_files(folder: str) -> None:
    """
    Extracts all .ispac files in the specified folder.

    Args:
        folder (str): The path of the folder to extract .ispac files from.
    """
    ispac_files = [f for f in os.listdir(folder) if f.endswith('.ispac')]
    for ispac_file in ispac_files:
        ispac_path = os.path.join(folder, ispac_file)
        with zipfile.ZipFile(ispac_path, 'r') as zip_ref:
            zip_ref.extractall(folder)
            info_logger.info(f"Extracted {ispac_file} to {folder}")

def replace_spaces_in_dtsx_files(folder: str) -> None:
    """
    Replaces '%20' with spaces in .dtsx filenames within the specified folder.

    Args:
        folder (str): The path of the folder to process .dtsx files in.
    """
    for root, dirs, files in os.walk(folder):
        for file in files:
            if file.endswith('.dtsx'):
                new_name = file.replace('%20', ' ')
                new_path = os.path.join(root, new_name)
                if not os.path.exists(new_path):
                    os.rename(os.path.join(root, file), new_path)

def main() -> None:
    """
    Main execution function to recreate directory, extract packages, and process folders.
    
    This function performs the following steps:
    1. Recreates the specified directory.
    2. Runs the asynchronous function to extract SSIS packages.
    3. Retrieves the list of folders in the directory.
    4. Processes each folder to extract .ispac files and replace spaces in .dtsx files.
    
    Args:
        None
    
    Returns:
        None
    """
    recreate_directory(DIRECTORY)
    print(f'recreate_directory {DIRECTORY}')
    info_logger.info(f'recreate_directory {DIRECTORY}')
    asyncio.run(extract_package_full_permission())
    folder_list: List[str] = get_folder_list(DIRECTORY)
    for folder in folder_list:
        extract_ispac_files(folder)
        replace_spaces_in_dtsx_files(folder)

if __name__ == "__main__":
    main()