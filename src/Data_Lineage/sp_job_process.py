from dataclasses import dataclass
import os
import sys
import pandas as pd
from typing import List
import re
#For Notebook path
module_path = os.path.abspath(os.path.join('..','..'))
if module_path not in sys.path:
    sys.path.append(module_path)

from src.utility.utility import SQLConnection, load_config, read_sql_file
from src.data_lineage.utility import extract_sp_names, extract_table_lineage, remove_sp_bracket
config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = load_config(config_path)
SQL_PATH = os.path.join(os.path.dirname(__file__), 'jobagent_sp_step.sql')

@dataclass
class JobAgentUtility:
    job_step: pd.DataFrame = None
    async def extract_job_step(self) -> pd.DataFrame:
        query = read_sql_file(SQL_PATH)
        sql_conn = SQLConnection(db_name = 'MSDB', server = config['servername'])
        self.job_step = await sql_conn.run_query_aio(query)
        return self.job_step
    
def identify_sp_tables(row) -> List:
    if 'EXEC' in row['Command'].upper() and row['subsystem'] == 'TSQL':
        return extract_sp_names(row['Command']), None
    elif row['subsystem'] == 'TSQL':
        return None, extract_table_lineage(sql = row['Command'], simple_return = True)
    else:
        return None, None
    
def process_dataframe(df) -> pd.DataFrame:
    df[['SP', 'Tables']] = df.apply(lambda row: pd.Series(identify_sp_tables(row)), axis=1)
    df['SP'] = df['SP'].apply(lambda x: clean_and_extract_distinct(x) if x else x)
    df['SP'] = df['SP'].apply(lambda x: remove_sp_bracket(x) if x else x)
    
    ##Unpack List
    df =  df.explode('SP')
    
    def split_sp(sp, method_db):
        parts = sp.split('.')
        if len(parts) == 3:
            method_db = parts[0]
            method_schema = parts[1]
            method_name = parts[2]
        elif len(parts) == 2:
            if parts[0] == method_db:
                method_db = method_db
                method_schema = 'dbo'
                method_name = parts[1]
            elif parts[0] not in config['db_list']:
                method_db = method_db
                method_schema = parts[0]
                method_name = parts[1]
            else:
                method_db = parts[0]
                method_schema = parts[1]
                method_name = None
        else:
            method_db = method_db
            method_schema = 'dbo'
            method_name = parts[0]
        
        return pd.Series([method_db, method_schema, method_name])
    
    df[['method_db', 'method_schema', 'method_name']] = df.apply(lambda row: split_sp(row['SP'], row['method_db']) if row['SP'] else pd.Series([None, None, None]), axis=1)

    return df

def clean_and_extract_distinct(sp: str) -> List:
    cleaned_objects = set()
    
    for obj in sp:
        for item in obj:
            # Remove '@??=' prefix if present
            cleaned_item = re.sub(r'@\w+\s*=\s*', '', item)
            if cleaned_item:
                cleaned_objects.add(cleaned_item)
    
    return list(cleaned_objects)