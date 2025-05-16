import pytest
import yaml
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.utility.utility import (
    load_config,
    read_sql_file,
    SQLConnection,
    SQLConnectionWithLogin
)
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = load_config(CONFIG_PATH).get('test_utility')

login_detail = config.get('sql_login')
def test_load_config_success():
    with open("test_config.yaml", "w") as f:
        yaml.dump({"test_key": "test_value"}, f)

    config = load_config("test_config.yaml")
    assert config == {"test_key": "test_value"}

    os.remove("test_config.yaml")

def test_load_config_file_not_found():
    config = load_config("nonexistent_config.yaml")
    assert config is None

def test_load_config_yaml_error():
    with open("invalid_config.yaml", "w") as f:
        f.write("invalid yaml: : :")

    config = load_config("invalid_config.yaml")
    assert config is None

    os.remove("invalid_config.yaml")

def test_read_sql_file_success():
    with open("test_query.sql", "w") as f:
        f.write("SELECT * FROM test_table;")

    query = read_sql_file("test_query.sql")
    assert query == "SELECT * FROM test_table;"

    os.remove("test_query.sql")

def test_sql_connection_get_dsn():
    conn = SQLConnection(db_name="test_db", server="test_server")
    expected_dsn = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=test_server;DATABASE=test_db;Trusted_Connection=yes;TrustServerCertificate=yes;"
    assert conn.get_dsn() == expected_dsn

def test_sql_connection_with_login_get_dsn():
    conn = SQLConnectionWithLogin(db_name="test_db", server="test_server", username="test_user", password="test_password")
    expected_dsn = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=test_server;DATABASE=test_db;UID=test_user;PWD=test_password;TrustServerCertificate=yes;"
    assert conn.get_dsn() == expected_dsn

def test_sql_connection_get_engine():
    conn = SQLConnection(db_name="test_db", server="test_server")
    engine = conn.get_engine()
    assert engine is not None

def test_sql_connection_run_query():
    
    conn = SQLConnectionWithLogin(**login_detail)
    query = "SELECT 1"
    result = conn.run_query(query)
    assert isinstance(result, pd.DataFrame)

def test_sql_connection_execute_command():
    conn = SQLConnectionWithLogin(**login_detail)

    command = config.get('command')
    conn.execute_command(command)
    

def test_sql_connection_upload():
    conn = SQLConnectionWithLogin(**login_detail)
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    upload_datetime = pd.Timestamp.now()
    df['upload_datetime'] = upload_datetime
    conn.upload(df, 'python_unit_woolie_test')

@pytest.mark.asyncio
async def test_sql_connection_run_query_aio():
    conn = SQLConnectionWithLogin(**login_detail)
    query = config.get('run_query_aio')
    result = await conn.run_query_aio(query)
    assert isinstance(result, pd.DataFrame)