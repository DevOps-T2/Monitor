from typing import List
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

app = FastAPI()
load_dotenv()

DATABASE = os.getenv("DATABASE")
DATABASE_HOST_READ = os.getenv("DATABASE_HOST_READ")
DATABASE_HOST_WRITE = os.getenv("DATABASE_HOST_WRITE")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")


class PostMonitorProcess(BaseModel):
    user_id: str
    computation_id: str
    vcpu_usage: int
    memory_usage: int

class GetMonitorProcess(PostMonitorProcess):
    id: int


@app.get("/monitor/processes/", response_model=List[GetMonitorProcess])
async def list_user_processes():
    """Get all process monitors from all users from the database

    Returns:
        List[MonitorProcess]: A list of MonitorProcesses 
    """
    sql: str = "SELECT * FROM monitor"
    query_result = readDB(sql)

    processes: List[GetMonitorProcess] = []
    for process in query_result:
        processes.append(
            GetMonitorProcess(id=process[0],
                           user_id=process[1],
                           computation_id=process[2],
                           vcpu_usage=process[3],
                           memory_usage=process[4]))

    return processes


@app.get("/monitor/processes/{user_id}", response_model=List[GetMonitorProcess])
async def list_user_processes(user_id: str):
    """Get all process monitors from a specific user from the database

    Args:
        user_id (str): The user id

    Returns:
        List[MonitorProcess]: A list of MonitorProcesses
    """
    sql: str = "SELECT * FROM monitor WHERE user_id = %s"
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


@app.delete("/monitor/processes/{user_id}")
async def delete_user_process(user_id: str):
    """Delete all process monitors from a user from the database

    Args:
        user_id (str): A user id

    Returns:
        str: A status (may change)
    """
    if(process_exists(column="user_id", value=user_id) == False):
        raise HTTPException(
            status_code=404, detail="No process with user_id = '%s' exists." % user_id)

    sql: str = "DELETE FROM monitor WHERE user_id = %s"

    writeDB(sql, (user_id,))

    return "done boss"


@app.post("/monitor/process/")
async def create_user_process(process: PostMonitorProcess):
    """Add a process monitor to the database

    Args:
        process (MonitorProcess): Process data

    Returns:
        str: A status
    """
    if(process_exists(column="computation_id", value=process.computation_id)):
        raise HTTPException(
            status_code=409, detail="A process with computation_id = '%s' already exists." % process.computation_id)

    process_dict: dict = process.dict()
    sql, values = mysql_query_insert(process_dict, "monitor")

    writeDB(sql, values)

    return "done boss"


@app.delete("/monitor/process/{computation_id}")
async def delete_user_process(computation_id: str):
    """Delete a single process monitor from the database

    Args:
        computation_id (str): A computation id

    Returns:
        str: A status
    """

    if(process_exists(column="computation_id", value=computation_id) == False):
        raise HTTPException(
            status_code=404, detail="A process with computation_id = '%s' does not exist." % computation_id)

    sql: str = "DELETE FROM monitor WHERE computation_id = %s"
    writeDB(sql, (computation_id,))

    return "done boss"


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
    process_exists: bool = 0 < result[0][0]

    return process_exists


def writeDB(sql_prepared_statement: str, sql_placeholder_values: tuple = ()):
    """Take a prepared statement with values and writes to database

    Args:
        sql_prepared_statement (str): an sql statement with (optional) placeholder values
        sql_placeholder_values (tuple, optional): The values for the prepared statement. Defaults to ().
    """
    connection = mysql.connector.connect(database=DATABASE,
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
    """Take a prepared statement with values and makes a query to the database

    Args:
        sql_prepared_statement (str): an sql statement with (optional) placeholder values
        sql_placeholder_values (tuple, optional): The values for the prepared statement. Defaults to ().

    Returns:
        List(tuple): The fetched result
    """
    connection = mysql.connector.connect(database=DATABASE,
                                         host=DATABASE_HOST_READ,
                                         user=DATABASE_USER,
                                         password=DATABASE_PASSWORD
                                         )

    if (connection.is_connected()):
        cursor = connection.cursor(prepared=True)
        cursor.execute(sql_prepared_statement, sql_placeholder_values)
        result = cursor.fetchall()

    return result
