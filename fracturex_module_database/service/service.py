from bson import ObjectId
import psycopg2
from pymongo import MongoClient
from fastapi import status
from fastapi.responses import JSONResponse

from fracturex_module_database.config.environment import environment
from fracturex_module_database.infrastructure.database import (
    mongodb, 
    postgresql
)
from fracturex_module_database.model.database_crud_info import (
    PostgreSQL,
    MongoDB
)
from fracturex_module_database.model.database_type import Database_Type
from fracturex_module_database.model.database_config import Database_Config
from fracturex_module_database.model.dto.http_response import HTTP_Response

def get_database_connection(database_config_key : str = None) -> psycopg2.extensions.connection | MongoClient | JSONResponse:
    """
    Función para retornar la primera conexión a la base de datos (específicamente donde se inicia sesión), o la indicada con el parámetro "database_config_key"
    
    Parameters
    ----------
    database_config_key : str = None
        Nombre de la llave del registro de la base de datos
    
    Returns
    -------
    psycopg2.extensions.connection
        Conexión a una base de datos PostgreSQL
    MongoClient
        Conexión a una base de datos MongoDB
    JSONResponse
        Respuesta en formato JSON del error al conectar a una BD con los datos suministrados.
    """
    # Retornar None en caso de que no haya registros
    if environment.FRACTUREX_MODULE_DATABASE_CONFIG is None or len(environment.FRACTUREX_MODULE_DATABASE_CONFIG.keys()) == 0:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            content=HTTP_Response(success=False, message="Sin registro de base de datos").model_dump()
        )
    try:
        database_config : Database_Config = Database_Config(**(environment.FRACTUREX_MODULE_DATABASE_CONFIG.get(database_config_key if database_config_key is not None else list(environment.DATABASE_CONFIG.keys())[0])))
        # Asignar el type
        if "postgresql" in database_config.url.lower():
            database_config.type = Database_Type.POSTGRESQL
        elif "mongodb" in database_config.url.lower():
            database_config.type = Database_Type.MONGODB
        else:
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                content=HTTP_Response(success=False, message="Sin registro de base de datos").model_dump()
            )
        # Retornar la conexión
        if database_config.type == Database_Type.POSTGRESQL:
            return psycopg2.connect(dsn=database_config.url)
        elif database_config.type == Database_Type.MONGODB:
            return MongoClient(host=database_config.url)
    except Exception as e:
        # Retornar un error en caso de que no logre conectar
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            content=HTTP_Response(success=False, message=f"No se pudo conectar a la base de datos: {str(e)}", data={}).model_dump()
        )

def get_database_config(database_config_key : str) -> Database_Config | None:
    """
    Función para retornar un Database_Config
    
    Parameters
    ----------
    database_config_key : str
        Llave de la configuración de la base de datos en el archivo .env
    
    Returns
    -------
    Database_Config
        Objeto que contiene la información de la conexión a la base de datos
    None
        None en caso de que la llave no se encuentre
    """
    return Database_Config(**(environment.FRACTUREX_MODULE_DATABASE_CONFIG.get(database_config_key, None)))

def insert(crud_info : PostgreSQL.Insert | MongoDB.Insert, print_data : bool = False) -> list[dict] | ObjectId | JSONResponse:
    """
    Función para insertar en una base de datos
    
    Parameters
    ----------
    crud_info : database.model.database_crud_info.PostgreSQL.Insert | database.model.database_crud_info.MongoDB.Insert
        Información del CRUD a realizar
    
    print_data : bool = False
        Encargado de mostrar o no la información al momento de realizar el CRUD
    
    Returns
    -------
    list[dict]
        Lista de IDs registrados usando PostgreSQL
    
    ObjectId
        Id del documento creado usando MongoDB
        
    JSONResponse
        Respuesta en formato JSON en caso de haber error
    """
    if isinstance(crud_info, PostgreSQL.Insert):
        return postgresql.PostgreSQL.insert(conn=crud_info.conn, query=crud_info.query, vars=crud_info.vars, print_data=print_data)
    elif isinstance(crud_info, MongoDB.Insert):
        return mongodb.MongoDB.insert(conn=crud_info.conn, collection_name=crud_info.collection_name, document=crud_info.document, print_data=print_data)

def select(crud_info : PostgreSQL.Select | MongoDB.Select, print_data : bool = False) -> list | JSONResponse:
    """
    Función para retornar una consulta a una base de datos
    
    Parameters
    ----------
    crud_info : database.model.database_crud_info.PostgreSQL.Select | database.model.database_crud_info.MongoDB.Select
        Información del CRUD a realizar
    
    print_data : bool = False
        Encargado de mostrar o no la información al momento de realizar el CRUD
    
    Returns
    -------
    list
        Lista de registros de la consulta realizada
        
    JSONResponse
        Respuesta en formato JSON en caso de haber error
    """
    if isinstance(crud_info, PostgreSQL.Select):
        return postgresql.PostgreSQL.select(conn=crud_info.conn, query=crud_info.query, vars=crud_info.vars, print_data=print_data)
    if isinstance(crud_info, MongoDB.Select):
        return mongodb.MongoDB.select(conn=crud_info.conn, collection_name=crud_info.collection_name, query=crud_info.query, aggregate_pipeline=crud_info.aggregate_pipeline, sort=crud_info.sort, print_data=print_data)

def update(crud_info : PostgreSQL.Update | MongoDB.Update, print_data : bool = False) -> bool | JSONResponse:
    """
    Función para retornar una consulta a una base de datos
    
    Parameters
    ----------
    crud_info : database.model.database_crud_info.PostgreSQL.Update | database.model.database_crud_info.MongoDB.Update
        Información del CRUD a realizar
    
    print_data : bool = False
        Encargado de mostrar o no la información al momento de realizar el CRUD
    
    Returns
    -------
    bool
        Encargado de notificar si el registro fue modificado
        
    JSONResponse
        Respuesta en formato JSON en caso de haber error
    """
    if isinstance(crud_info, PostgreSQL.Update):
        return postgresql.PostgreSQL.update(conn=crud_info.conn, query=crud_info.query, vars=crud_info.vars, print_data=print_data)
    elif isinstance(crud_info, MongoDB.Update):
        return mongodb.MongoDB.update(conn=crud_info.conn, collection_name=crud_info.collection_name, query=crud_info.query, update_values=crud_info.update_values, print_data=print_data)

def delete(crud_info : PostgreSQL.Delete | MongoDB.Delete, print_data : bool = False) -> bool | JSONResponse:
    """
    Función para eliminar un(os) registro(s) de una tabla en una base de datos
    
    Parameters
    ----------
    crud_info : database.model.database_crud_info.PostgreSQL.Delete | database.model.database_crud_info.MongoDB.Update
        Información del CRUD a realizar
    
    print_data : bool = False
        Encargado de mostrar o no la información al momento de realizar el CRUD
    
    Returns
    -------
    bool
        Encargado de notificar si el/los registro(s) fue(ron) eliminado(s)
        
    JSONResponse
        Respuesta en formato JSON en caso de haber error
    """
    if isinstance(crud_info, PostgreSQL.Delete):
        return postgresql.PostgreSQL.delete(conn=crud_info.conn, query=crud_info.query, vars=crud_info.vars, print_data=print_data)
    elif isinstance(crud_info, MongoDB.Delete):
        return mongodb.MongoDB.delete(conn=crud_info.conn, collection_name=crud_info.collection_name, query=crud_info.query, print_data=print_data)
