import os
import shutil
import zipfile
import subprocess
from typing import List

DIRECTORY: str = 'C:/Users/wchen/Documents/ssis export/'
SCRIPT_DIR: str = 'C:/Users/wchen/OneDrive - Adairs Retail Group/Documents/engineering-code-collection/src/data_lineage/'

def recreate_directory(directory: str) -> None:
    """
    Deletes the specified directory if it exists and then recreates it.

    Args:
        directory (str): The path of the directory to recreate.
    """
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)

def execute_powershell_script(script_path: str) -> None:
    """
    Executes a PowerShell script.

    Args:
        script_path (str): The path of the PowerShell script to execute.
    """
    if os.path.exists(script_path):
        subprocess.run(['powershell', '-File', script_path], check=True)

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

def replace_spaces_in_dtsx_files(folder: str) -> None:
    """
    Replaces '%20' with spaces in .dtsx filenames within the specified folder.

    Args:
        folder (str): The path of the folder to process .dtsx files in.
    """
    for root, dirs,files in os.walk(folder):
        for file in files:
            if file.endswith('.dtsx'):
                new_name = file.replace('%20', ' ')
                new_path = os.path.join(root, new_name)
                if not os.path.exists(new_path):
                    os.rename(os.path.join(root, file), new_path)

# Main execution
recreate_directory(DIRECTORY)
print(f'recreate_directory {DIRECTORY}')
execute_powershell_script(os.path.join(SCRIPT_DIR, 'export_ssis.ps1'))
print(f'execute_powershell_script {os.path.join(SCRIPT_DIR, "export_ssis.ps1")}')
folder_list: List[str] = get_folder_list(DIRECTORY)
for folder in folder_list:
    extract_ispac_files(folder)
    replace_spaces_in_dtsx_files(folder)