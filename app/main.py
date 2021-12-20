from typing import List
import os

from fastapi import FastAPI, HTTPException, APIRouter, Request, Response, status
from pydantic import BaseModel
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
load_dotenv()


DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_HOST_READ = os.getenv("DATABASE_HOST_READ")
DATABASE_HOST_WRITE = os.getenv("DATABASE_HOST_WRITE")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = "" # os.getenv("DATABASE_PASSWORD")


class PostMonitorProcess(BaseModel):
    user_id: str
    computation_id: str
    vcpu_usage: int
    memory_usage: int

# Coule use PostMonitorProcess instead of BaseModel but that would put "id" at the bottom
# which would mess up the structure of sq statements later
class GetMonitorProcess(BaseModel):
    id: int
    user_id: str
    computation_id: str
    vcpu_usage: int
    memory_usage: int


app = FastAPI()
router = APIRouter()

@router.get("/api/monitor/processes", response_model=List[GetMonitorProcess])
@router.get("/api/monitor/processes/", response_model=List[GetMonitorProcess], include_in_schema=False)
async def list_user_processes(req: Request):
    """Get all process monitors from all users from the database

    Returns:
        List[GetMonitorProcess]: A list of GetMonitorProcesses 
    """

    # Only admin has access to this endpoint
    role = req.headers.get("Role")

    if(role != "admin"):
        detail = req.headers
        raise HTTPException(status_code=403)


    print(DATABASE_NAME)
    # Getting the GetMonitorProcess properties to use in sql statement,
    # because the order of columns needs to be explicit (i.e. not = '*') because 
    # the query_result has no keys, only values.
    columns = ", ".join(GetMonitorProcess.schema().get("properties").keys()) 
    sql: str = "SELECT %s FROM monitor" % columns
    query_result = readDB(sql)
    print(query_result)

    processes: List[GetMonitorProcess] = []
    for process in query_result:
        processes.append(
            GetMonitorProcess(id=process[0],
                              user_id=process[1],
                              computation_id=process[2],
                              vcpu_usage=process[3],
                              memory_usage=process[4]))

    return processes

@router.get("/api/monitor/processes/{user_id}", response_model=List[GetMonitorProcess])
@router.get("/api/monitor/processes/{user_id}/", response_model=List[GetMonitorProcess], include_in_schema=False)
async def list_user_processes(user_id: str, req: Request):
    """Get all process monitors from a specific user from the database

    Args:
        user_id (str): The user id

    Returns:
        List[GetMonitorProcess]: A list of GetMonitorProcesses
    """

    #Both admin and user has access to this endpoint. But it needs to be to a specific user. 
    userId = req.headers.get("UserId")
    role = req.headers.get("Role")

    if(userId != user_id and role != "admin"):
        raise HTTPException(status_code=403)



    columns = ", ".join(GetMonitorProcess.schema().get("properties").keys()) 
    sql: str = "SELECT %s FROM monitor" % columns + " WHERE user_id = %s"
    query_result = readDB(sql, (user_id,))

    processes: List[GetMonitorProcess] = []
    for process in query_result:
        processes.append(
            GetMonitorProcess(id=process[0],
                              user_id=process[1],
                              computation_id=process[2],
                              vcpu_usage=process[3],
                              memory_usage=process[4]))

    return processes


@router.delete("/api/monitor/processes/{user_id}")
@router.delete("/api/monitor/processes/{user_id}/", include_in_schema=False)
async def delete_user_process(user_id: str, req: Request, response: Response):
    """Delete all process monitors from a user from the database

    Args:
        user_id (str): A user id

    Returns:
        str: A status (may change)
    """


    # Only the admin role has access to this endponit. 
    role = req.headers.get("Role")
    userId = req.headers.get("UserId")

    if(role != "admin" and userId != user_id):
        raise HTTPException(status_code=403) 

    if(process_exists(column="user_id", value=user_id) == False):
        response.status_code = status.HTTP_204_NO_CONTENT
        return "No proccesses for the user " + user_id

    sql: str = "DELETE FROM monitor WHERE user_id = %s"

    writeDB(sql, (user_id,))

    return "Successfully deleted processes with user_id = %s" % user_id


@router.post("/api/monitor/process", response_model=GetMonitorProcess)
@router.post("/api/monitor/process/", response_model=GetMonitorProcess, include_in_schema=False)
async def create_user_process(process: PostMonitorProcess, req: Request):
    """Add a process monitor to the database

    Args:
        process (GetMonitorProcess): Process data

    Returns:
        str: A status
    """

    # Only admin role has access to this endpoint.
    role = req.headers.get("Role")

    if(role != "admin"):
        raise HTTPException(status_code=403)

    if(process_exists(column="computation_id", value=process.computation_id)):
        raise HTTPException(
            status_code=409, detail="A process with computation_id = '%s' already exists." % process.computation_id)

    process_dict: dict = process.dict()
    sql, values = mysql_query_insert(process_dict, "monitor")

    writeDB(sql, values)

    return sync_get_user_process(process.computation_id)


@router.get("/api/monitor/process/{computation_id}", response_model=GetMonitorProcess)
@router.get("/api/monitor/process/{computation_id}/", response_model=GetMonitorProcess, include_in_schema=False)
async def get_user_process(computation_id: str, req: Request):
    """Just runs sync_get_user_process"""

    # Only admin role has access to this endpoint.
    role = req.headers.get("Role")

    if(role != "admin"):
        raise HTTPException(status_code=403)

    return sync_get_user_process(computation_id)


@router.delete("/api/monitor/process/{computation_id}")
@router.delete("/api/monitor/process/{computation_id}/", include_in_schema=False)
async def delete_user_process(computation_id: str, req: Request, response: Response):
    """Delete a single process monitor from the database

    Args:
        computation_id (str): A computation id

    Returns:
        str: A status
    """

    # Only admin role has access to this endpoint.
    role = req.headers.get("Role")
    computationID = req.headers.get("computation_id")

    if(role != "admin" and computationID != computation_id):
        raise HTTPException(status_code=403)

    if(process_exists(column="computation_id", value=computation_id) == False):
        response.status_code = status.HTTP_204_NO_CONTENT
        return "No procces exists with the computation_id " + computation_id

    sql: str = "DELETE FROM monitor WHERE computation_id = %s"
    writeDB(sql, (computation_id,))

    return "Successfully deleted process with computation_id = %s" % computation_id

app.include_router(router)

def sync_get_user_process(computation_id):
    """Gets a single user process by computation_id. This function was created as a synchronous function,
    so it can be re-used

    Args:
        computation_id (int): A computation id

    Returns:
        (GetMonitorProcess): A single GetMonitorProcess
    """
    columns = ", ".join(GetMonitorProcess.schema().get("properties").keys()) 
    sql: str = "SELECT %s FROM monitor" % columns + " WHERE computation_id = %s"
    values: tuple = (computation_id,)
    query_result = readDB(sql, values)

    if(len(query_result) == 0):
        raise HTTPException(
            status_code=404, detail="A process with computation_id = '%s' does not exist." % computation_id)
    else:
        process_tuple = query_result[0]
        process = GetMonitorProcess(id=process_tuple[0],
                                    user_id=process_tuple[1],
                                    computation_id=process_tuple[2],
                                    vcpu_usage=process_tuple[3],
                                    memory_usage=process_tuple[4])
        return process

def mysql_query_insert(dict: dict, table: str):
    """Create a prepared sql statement along with its values from a dictionary and a table name

    Args:
        dict (dict): The dictionary whose values should be inserted into the database
        table (str): The table to insert into

    Returns:
        tuple(str, tuple): The prepared statement (str) and the values (tuple)
    """
    placeholders = ', '.join(['%s'] * len(dict))
    columns = ', '.join("`" + str(x).replace('/', '_') +
                        "`" for x in dict.keys())
    values = tuple(dict.values())
    prepared_statement: str = "INSERT INTO %s ( %s ) VALUES ( %s );" % (
        table, columns, placeholders)

    return prepared_statement, values


def process_exists(column: str, value):
    """Checks if a specific value on a specific column exists in the database.

    Args:
        column (str): Name of column in databasa
        value ([type]): Value to check if exists in column

    Returns:
        bool: True if value exists
    """
    sql: str = "SELECT COUNT(*) FROM monitor WHERE " + column + " = %s"
    values: tuple = (value, )

    result = readDB(sql, values)
    print(result)
    process_exists: bool = 0 < result[0][0]

    return process_exists


def writeDB(sql_prepared_statement: str, sql_placeholder_values: tuple = ()):
    """Takes a prepared statement with values and writes to database

    Args:
        sql_prepared_statement (str): an sql statement with (optional) placeholder values
        sql_placeholder_values (tuple, optional): The values for the prepared statement. Defaults to ().
    """
    connection = mysql.connector.connect(database=DATABASE_NAME,
                                         host=DATABASE_HOST_WRITE,
                                         user=DATABASE_USER,
                                         password=DATABASE_PASSWORD
                                         )

    try:
        if (connection.is_connected()):
            cursor = connection.cursor(prepared=True)
            cursor.execute(sql_prepared_statement, sql_placeholder_values)
            connection.commit()
    except Error as e:
        raise HTTPException(
            status_code=500, detail="Error while contacting database. " + str(e))
    finally:
        cursor.close()
        connection.close()


def readDB(sql_prepared_statement: str, sql_placeholder_values: tuple = ()):
    """Takes a prepared statement with values and makes a query to the database

    Args:
        sql_prepared_statement (str): an sql statement with (optional) placeholder values
        sql_placeholder_values (tuple, optional): The values for the prepared statement. Defaults to ().

    Returns:
        List(tuple): The fetched result
    """
    connection = mysql.connector.connect(database=DATABASE_NAME,
                                         host=DATABASE_HOST_READ,
                                         user=DATABASE_USER,
                                         password=DATABASE_PASSWORD
                                         )
    try:
        if (connection.is_connected()):
            cursor = connection.cursor(prepared=True)
            cursor.execute(sql_prepared_statement, sql_placeholder_values)
            result = cursor.fetchall()
            return result
    except Error as e:
        raise HTTPException(
            status_code=500, detail="Error while contacting database. " + str(e))
    finally:
        cursor.close()
        connection.close()
