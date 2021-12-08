from typing import List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mysql.connector
from mysql.connector import Error

app = FastAPI()

class MonitorProcess(BaseModel):
    id: str
    user_id: str
    computation_id: str
    vcpu_usage: int
    memory_usage: int

@app.get("/monitor/processes/", response_model=List[MonitorProcess])
async def list_user_processes():
    """Get all process monitors from all users from the database

    Returns:
        List[MonitorProcess]: A list of MonitorProcesses 
    """
    sql: str = "SELECT * FROM monitor"
    query_result = readDB(sql)

    processes: List[MonitorProcess] = []
    for process in query_result:
        processes.append(
            MonitorProcess(id=process[0],
                           user_id=process[1],
                           computation_id=process[2],
                           vcpu_usage=process[3],
                           memory_usage=process[4]))

    return processes

@app.get("/monitor/processes/{user_id}", response_model=List[MonitorProcess])
async def list_user_processes(user_id: str):
    """Get all process monitors from a specific user from the database

    Args:
        user_id (str): The user id

    Returns:
        List[MonitorProcess]: A list of MonitorProcesses
    """
    sql: str = "SELECT * FROM monitor WHERE user_id = %s"
    query_result = readDB(sql, (user_id,))

    processes: List[MonitorProcess] = []
    for process in query_result:
        processes.append(
            MonitorProcess(id=process[0],
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
    sql: str = "DELETE FROM monitor WHERE user_id = %s"

    writeDB(sql, (user_id,))

    return "done boss"

@app.post("/monitor/process/")
async def create_user_process(process: MonitorProcess):
    """Add a process monitor to the database

    Args:
        process (MonitorProcess): Process data

    Returns:
        str: A status
    """
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

def writeDB(sql_prepared_statement: str, sql_placeholder_values: tuple=()):
    """Take a prepared statement with values and writes to database

    Args:
        sql_prepared_statement (str): an sql statement with (optional) placeholder values
        sql_placeholder_values (tuple, optional): The values for the prepared statement. Defaults to ().
    """
    connection = mysql.connector.connect(host='localhost',
                                         database='cloudsolver',
                                         user='root',
                                         password='4321'
                                         )

    if (connection.is_connected()):
        cursor = connection.cursor(prepared=True)
        cursor.execute(sql_prepared_statement, sql_placeholder_values)
        connection.commit()


def readDB(sql_prepared_statement: str, sql_placeholder_values: tuple=()):
    """Take a prepared statement with values and makes a query to the database

    Args:
        sql_prepared_statement (str): an sql statement with (optional) placeholder values
        sql_placeholder_values (tuple, optional): The values for the prepared statement. Defaults to ().

    Returns:
        List(tuple): The fetched result
    """
    connection = mysql.connector.connect(host='localhost',
                                         database='cloudsolver',
                                         user='root',
                                         password='4321'
                                         )

    if (connection.is_connected()):
        cursor = connection.cursor(prepared=True)
        cursor.execute(sql_prepared_statement, sql_placeholder_values)
        result = cursor.fetchall()

    return result