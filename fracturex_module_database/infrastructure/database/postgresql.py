import re
from fastapi.responses import JSONResponse
from psycopg2.extensions import (
    connection,
    cursor
)
from psycopg2.extras import NamedTupleCursor
from psycopg2 import errors
from typing import Any
from fastapi import status

from fracturex_module_database.model.dto.http_response import HTTP_Response
from fracturex_module_database.model.idatabase import IDatabase

class PostgreSQL(IDatabase):

    @staticmethod
    def select(conn : connection, query : str, vars : tuple | None = None, print_data : bool = False) -> list[dict] | JSONResponse:
        print(f"---------- PostgreSQL.Select({conn.info.dbname}) ----------")
        if print_data:
            print(f"query: {query}")
            print(f"vars: {str(vars)}")
        
        returnValue: list[dict] = []
        try:
            mycursor: cursor = conn.cursor(cursor_factory=NamedTupleCursor)
            if vars:
                mycursor.execute(query=query, vars=vars)
            else:
                mycursor.execute(query=query)
            for row in mycursor.fetchall():
                returnValue.append(row._asdict())
        except Exception as e:
            print("Exception")
            print(str(e.pgerror))
            print("--------------------------------------------")
            returnValue = JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=HTTP_Response(success=False, message=f"There was an error: {str(e)}", data={}))
        finally:
            if mycursor: mycursor.close()
            return returnValue
    
    @staticmethod
    def insert(conn : connection, query : str, vars : tuple, print_data : bool = False) -> list[dict] | JSONResponse:
        print(f"---------- PostgreSQL.Insert({conn.info.dbname}) ----------")
        returnValue: list[dict] = []
        if print_data:
            print(f"query: {query}")
            print(f"vars: {str(vars)}")
        
        try:
            mycursor: cursor = conn.cursor(cursor_factory=NamedTupleCursor)
            mycursor.execute(query=query, vars=vars)
            for row in mycursor.fetchall():
                returnValue.append(row._asdict())
        except Exception as e:
            print("Exception")
            print(str(e.pgerror))
            print("--------------------------------------------")
            returnValue = JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=HTTP_Response(success=False, message=f"There was an error: {str(e)}", data={}))
        finally:
            if mycursor: mycursor.close()
            return returnValue

    @staticmethod
    def update(conn : connection, query : str, vars : tuple | None = None, print_data : bool = False) -> bool | JSONResponse:
        print(f"---------- PostgreSQL.Update({conn.info.dbname}) ----------")
        if print_data:
            print(f"query: {query}")
            print(f"vars: {str(vars)}")
        
        returnValue: Any
        try:
            mycursor: cursor = conn.cursor(cursor_factory=NamedTupleCursor)
            if (vars is not None):
                mycursor.execute(query=query, vars=vars)
            else:
                mycursor.execute(query=query)
            returnValue = len(mycursor.fetchall()) > 0
        except Exception as e:
            print("Exception")
            print(str(e.pgerror))
            print("--------------------------------------------")
            returnValue = JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=HTTP_Response(success=False, message=f"There was an error: {str(e)}", data={}))
        finally:
            if mycursor: mycursor.close()
            return returnValue

    @staticmethod
    def delete(conn : connection, query : str, vars : tuple, print_data : bool = False) -> bool | JSONResponse:
        print(f"---------- PostgreSQL.Delete({conn.info.dbname}) ----------")
        if print_data:
            print(f"query: {query}")
            print(f"vars: {str(vars)}")
        
        returnValue: Any
        try:
            mycursor: cursor = conn.cursor(cursor_factory=NamedTupleCursor)
            mycursor.execute(query=query, vars=(vars,))
            conn.commit()
            returnValue = len(mycursor.fetchall()) > 0
        except Exception as e:
            print("Exception")
            print(str(e.pgerror))
            print("--------------------------------------------")
            returnValue = JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=HTTP_Response(success=False, message=f"There was an error: {str(e)}", data={}))
        finally:
            if mycursor: mycursor.close()
            return returnValue

    @staticmethod
    def notify(*, conn : connection, channel : str = 'notification', payload : dict, print_data : bool = False) -> bool | JSONResponse:
        print(f"---------- PostgreSQL.Notify({conn.info.dbname}) ----------")
        if print_data:
            print(f"channel: {channel}")
            print(f"payload: {str(payload)}")
        
        returnValue: Any
        try:
            mycursor: cursor = conn.cursor(cursor_factory=NamedTupleCursor)
            mycursor.execute(query=f"NOTIFY {channel}, %s; ", vars=(str(payload).replace("'", "\""),))
            returnValue = True
        except Exception as e:
            print("Exception")
            print(str(e.pgerror))
            print("--------------------------------------------")
            returnValue = JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=HTTP_Response(success=False, message=f"There was an error: {str(e)}", data={}))
        finally:
            if mycursor: mycursor.close()
            return returnValue
    
    @staticmethod
    def __filter_postgresql_error_message(e: Exception) -> str:
        returnValue = str(e)
        if isinstance(e, errors.UniqueViolation):
            returnValue = ""
            matches_lang = (
                r'Key \((.*?)\)=\((.*?)\) already exists', 
                r'Ya existe la llave \((.*?)\)=\((.*?)\)'
            )
            for match_lang in matches_lang:
                matches = re.findall(match_lang, e.pgerror)
                if matches:
                    for match in matches:
                        columns = match[0].split(', ')
                        values = match[1].split(', ')
                        for column, value in zip(columns, values):
                            returnValue += ("\n" if len(returnValue) > 0 else "") + f"{value} already exists"
        return returnValue
