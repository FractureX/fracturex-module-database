from pydantic import BaseModel

from fracturex_module_database.model.database_type import Database_Type

class Database_Config(BaseModel):
    type: Database_Type = None
    url: str
