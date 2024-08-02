from fastapi import status
from fastapi.responses import JSONResponse
from pymongo import MongoClient, errors
from bson import ObjectId

from fracturex_module_database.model.idatabase import IDatabase
from fracturex_module_database.model.dto.http_response import HTTP_Response

class MongoDB(IDatabase):
    
    @staticmethod
    def select(conn : MongoClient, collection_name : str, query : dict = None, aggregate_pipeline : list[dict] = None, sort : list[dict[str, int]] = None, print_data : bool = False) -> list[dict] | JSONResponse:
        print(f"---------- MongoDB.Select({conn.get_database().name}) ----------")
        if print_data:
            print(f"collection_name: {collection_name}")
            print(f"query: {query}")
            print(f"aggregate_pipeline: {aggregate_pipeline}")
        
        returnValue: list[dict] = []
        try:
            collection = conn.get_database()[collection_name]
            if aggregate_pipeline:
                # Ejecutar pipeline de agregación si está definido
                result = collection.aggregate(aggregate_pipeline)
            else:
                # Ejecutar búsqueda normal
                if query is not None:
                    result = collection.find(query).sort(sort) if sort else collection.find(query)
                else:
                    result = collection.find().sort(sort) if sort else collection.find()
            returnValue = list(result)
        except errors.PyMongoError as e:
            print("Exception")
            print(str(e))
            print("--------------------------------------------")
            returnValue = JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=HTTP_Response(success=False, message=f"There was an error: {str(e)}", data={}))
        finally:
            return returnValue

    @staticmethod
    def insert(conn : MongoClient, collection_name : str, document : dict, print_data : bool = False) -> ObjectId | JSONResponse:
        print(f"---------- MongoDB.Insert({conn.get_database().name}) ----------")
        if print_data:
            print(f"collection_name: {collection_name}")
            print(f"document: {str(document)}")
        
        try:
            collection = conn.get_database()[collection_name]
            result = collection.insert_one(document)
            returnValue = ObjectId(result.inserted_id)
        except errors.PyMongoError as e:
            print("Exception")
            print(str(e))
            print("--------------------------------------------")
            returnValue = JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=HTTP_Response(success=False, message=f"There was an error: {str(e)}", data={}))
        finally:
            return returnValue

    @staticmethod
    def update(conn : MongoClient, collection_name : str, query : dict, update_values : dict, print_data : bool = False) -> bool | JSONResponse:
        print(f"---------- MongoDB.Update({conn.get_database().name}) ----------")
        if print_data:
            print(f"collection_name: {collection_name}")
            print(f"query: {str(query)}")
            print(f"update_values: {str(update_values)}")
        
        try:
            collection = conn.get_database()[collection_name]
            result = collection.update_many(query, {'$set': update_values})
            returnValue = result.acknowledged
        except errors.PyMongoError as e:
            print("Exception")
            print(str(e))
            print("--------------------------------------------")
            returnValue = JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=HTTP_Response(success=False, message=f"There was an error: {str(e)}", data={}))
        finally:
            return returnValue

    @staticmethod
    def delete(conn : MongoClient, collection_name : str, query : dict, print_data : bool = False) -> bool | Exception:
        print(f"---------- MongoDB.Delete({conn.get_database().name}) ----------")
        if print_data:
            print(f"collection_name: {collection_name}")
            print(f"query: {str(query)}")
        
        try:
            collection = conn.get_database()[collection_name]
            result = collection.delete_many(query)
            returnValue = result.deleted_count > 0
        except errors.PyMongoError as e:
            print("Exception")
            print(str(e))
            print("--------------------------------------------")
            returnValue = JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=HTTP_Response(success=False, message=f"There was an error: {str(e)}", data={}))
        finally:
            return returnValue
