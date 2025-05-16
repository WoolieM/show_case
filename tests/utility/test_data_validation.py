
import sys
import os
from typing import Dict, Any
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.utility.utility import (
    SQLConnectionWithLogin,
    load_config
)
CONFIG_PATH: str = os.path.join(os.path.dirname(__file__), 'config.yaml')
config: Dict[str, Any] = load_config(CONFIG_PATH)

login_detail: Dict[str, str] = config.get('test_utility').get('sql_login')
validation_config: Dict[str, str] = config.get('test_data_validation')

def test_date_validation() -> None:
    """Tests the Dim Date table for missing idDate or fullDate values."""
    conn = SQLConnectionWithLogin(**login_detail)
    query: str = validation_config.get('test_date_validation', '')
    df: pd.DataFrame = conn.run_query(query)
    assert df.iloc[0, 0] == 0, 'Missing Data in Dim Date'

def test_sales_order_validation() -> None:
    """Tests the Sales table for any missing Sales Order."""
    conn = SQLConnectionWithLogin(**login_detail)
    query:str = validation_config.get('test_sales_order_validation', '')
    df: pd.DataFrame = conn.run_query(query)
    assert df.iloc[0, 0] == 0, 'Missing Order ID in Sales'

def test_distinct_date_validation() -> None:
    """Tests for duplicated data in a date-related query (assumes a specific query structure)."""
    conn = SQLConnectionWithLogin(**login_detail)
    query: str = validation_config.get('test_distinct_date_validation')
    df: pd.DataFrame = conn.run_query(query)
    total_count: int = df.iloc[0, 0]
    distinct_count: int = df.iloc[0, 1]
    assert total_count == distinct_count, 'Duplicated Data Found'