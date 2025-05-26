from pydantic import BaseModel
from pandas import Timestamp


class CommonModel(BaseModel):
    class Config:
        json_encoders = {Timestamp: lambda v: v.strftime("%Y-%m-%d %H:%M:%S")}
