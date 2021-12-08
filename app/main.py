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
    sql: str = "DELETE FROM monitor WHERE user_id = %s"

    writeDB(sql, (user_id,))

    return "done boss"

@app.post("/monitor/process/")
async def create_user_process(process: MonitorProcess):
    process_dict: dict = process.dict()
    sql, values = mysql_query_insert(process_dict, "monitor")

    writeDB(sql, values)

    return "done boss"


@app.delete("/monitor/process/{computation_id}")
async def delete_user_process(computation_id: str):
    sql: str = "DELETE FROM monitor WHERE computation_id = %s"
    writeDB(sql, (computation_id,))

    return "done boss"


def mysql_query_insert(dict: dict, table: str):
    placeholders = ', '.join(['%s'] * len(dict))
    columns = ', '.join("`" + str(x).replace('/', '_') +
                        "`" for x in dict.keys())
    values = tuple(dict.values())
    prepared_statement: str = "INSERT INTO %s ( %s ) VALUES ( %s );" % (
        table, columns, placeholders)

    return prepared_statement, values


def writeDB(sql_prepared_statement: str, sql_placeholder_values: tuple=()):
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


def writeToDB(user_id: str, operation: str, item):
    try:
        connection = mysql.connector.connect(host='quotas-mysql-0',
                                             database='Default',
                                             user='root'
                                             )

        if connection.is_connected():
            mycursor = connection.cursor()
            if operation == "upMemory":
                # Check to see if the userID exits in the database
                sqlquery = "SELECT COUNT(*) FROM quotastabel WHERE User_id =" + \
                    "\"" + user_id + "\""
                mycursor.execute(sqlquery)
                resultFromquery = mycursor.fetchall()[0][0]

                if resultFromquery == 1:
                    mysqlquery = "UPDATE quotastabel SET Memory = " + "\'" + \
                        str(item) + "\' " + "WHERE User_id = " + \
                        "\"" + user_id + "\""
                    mycursor.execute(mysqlquery)

                    connection.commit()

                    print("Memory for user " + user_id + " updated")

                    statusCode = 200

                    return statusCode

                else:
                    print("No user found")
                    statusCode = 404

                    return statusCode

            elif operation == "upVcpu":
                # Check to see if the userID exits in the database
                sqlquery = "SELECT COUNT(*) FROM quotastabel WHERE User_id =" + \
                    "\"" + user_id + "\""
                mycursor.execute(sqlquery)
                resultFromquery = mycursor.fetchall()[0][0]

                if resultFromquery == 1:
                    mysqlquery = "UPDATE quotastabel SET Vcpu = " + "\'" + \
                        str(item) + "\' " + "WHERE User_id = " + \
                        "\"" + user_id + "\""
                    mycursor.execute(mysqlquery)

                    connection.commit()

                    print("CPU for user " + user_id + " updated")

                    statusCode = 200

                    return statusCode

                else:
                    print("No user found")

                    statusCode = 404

                    return statusCode

    except Error as e:
        print("Error while connecting to MySQL", e)

    finally:
        if connection.is_connected():
            mycursor.close()
            connection.close()
            print("MySQL connection is closed")


def readFromDB(user_id):
    try:
        connection = mysql.connector.connect(host='quotas-mysql-0',
                                             database='Default',
                                             user='root'
                                             )

        if connection.is_connected():
            mycursor = connection.cursor()
            # Check to see if the userID exits in the database
            sqlquery = "SELECT COUNT(*) FROM quotastabel WHERE User_id =" + \
                "\"" + user_id + "\""
            mycursor.execute(sqlquery)
            resultFromquery = mycursor.fetchall()[0][0]

            if resultFromquery == 1:
                mysqlquery = "SELECT Vcpu, Memory From quotastabel WHERE User_id = " + \
                    "\"" + user_id + "\""
                mycursor.execute(mysqlquery)

                resultFromquery = mycursor.fetchall()
                print("succes user_id found")

                valueCPU = resultFromquery[0][0]
                valueMemory = resultFromquery[0][1]

                print("For user " + user_id +
                      " the number of vcpus is " + str(valueCPU))
                print("For user " + user_id +
                      " the number of Memory is " + str(valueMemory))

                return valueMemory, valueCPU

            else:
                valueCPU = -1
                valueMemory = -1

                return valueMemory, valueCPU

    except Error as e:
        print("Error while connecting to MySQL", e)

    finally:
        if connection.is_connected():
            mycursor.close()
            connection.close()
            print("MySQL connection is closed")
