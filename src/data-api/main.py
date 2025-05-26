from fastapi import FastAPI, Query, Body, HTTPException
from typing import Literal, Annotated
from enum import Enum
from pydantic import BaseModel, Field
from src.utility.utility import SQLConnectionWithLogin, load_config
import numpy as np
import os

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.yaml')
config = load_config(CONFIG_PATH)
app = FastAPI()


class TableConfig(BaseModel):
    schema: str| None = Field(
        default = "dbo",
        title = 'Schema',
        alias = 'Table Schema',
        description = 'Schema of the table to be queried'
    )
    table_name: Literal['dim_date', 'all_objects']

@app.get("/adairs/", tags=["DB Query"])
async def get_adairs(
    my_selected_table: Annotated[TableConfig, Query()]
):
    login_detail = config.get('sql_login')
    conn = SQLConnectionWithLogin(
        **login_detail
    )
    

    query = f'SELECT TOP 10 * FROM {my_selected_table.schema}.{my_selected_table.table_name}'
    
    # return {"message": "Hellow"}
    result = await conn.run_query_aio(query)
    result.replace({np.nan: None}, inplace = True)
    return result.to_dict(orient="records")