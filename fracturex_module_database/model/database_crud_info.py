import psycopg2
from pydantic import BaseModel, ConfigDict
from pymongo import MongoClient

class PostgreSQL(BaseModel):
    
    class Insert(BaseModel):
        conn : psycopg2.extensions.connection
        query : str
        vars : tuple
        model_config = ConfigDict(arbitrary_types_allowed=True)
    
    class Select(BaseModel):
        conn : psycopg2.extensions.connection
        query : str
        vars : tuple = None
        model_config = ConfigDict(arbitrary_types_allowed=True)
    
    class Update(BaseModel):
        conn : psycopg2.extensions.connection
        query : str
        vars : tuple
        model_config = ConfigDict(arbitrary_types_allowed=True)
    
    class Delete(BaseModel):
        conn : psycopg2.extensions.connection
        query : str
        vars : tuple = None
        model_config = ConfigDict(arbitrary_types_allowed=True)
    
class MongoDB(BaseModel):
    
    class Insert(BaseModel):
        conn : MongoClient
        collection_name : str
        document : dict
        model_config = ConfigDict(arbitrary_types_allowed=True)
    
    class Select(BaseModel):
        conn : MongoClient
        collection_name : str
        query : dict = None
        aggregate_pipeline : list[dict] = None
        sort : list[dict[str, int]] = None
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
    class Update(BaseModel):
        conn : MongoClient
        collection_name : str
        query : dict = None
        update_values : dict
        model_config = ConfigDict(arbitrary_types_allowed=True)
        
    class Delete(BaseModel):
        conn : MongoClient
        collection_name : str
        query : dict = None
        model_config = ConfigDict(arbitrary_types_allowed=True)

class Insert(BaseModel):
    info : PostgreSQL.Insert | MongoDB.Insert

class Select(BaseModel):
    info : PostgreSQL.Select | MongoDB.Select

class Update(BaseModel):
    info : PostgreSQL.Update | MongoDB.Update

class Delete(BaseModel):
    info : PostgreSQL.Delete | MongoDB.Delete
