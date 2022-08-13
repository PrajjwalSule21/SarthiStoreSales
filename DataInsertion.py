import mysql.connector
from sqlalchemy import create_engine
from mysql.connector import errorcode
import sys
import pandas as pd

data = pd.read_csv('f:/Ineuron_Internship/Sarthi Store Analysis/Rawdata/Sarthi Store.csv')
# print(data.head(2))


def Database():
    connection = mysql.connector.connect(user='root',
                                         password='mysql',
                                         host='localhost')
    cursor = connection.cursor()
    try:
        # creating a database
        dbname = "SarthiStore"
        db_query = f"CREATE DATABASE IF NOT EXISTS {dbname}"
        cursor.execute(db_query)

    except Exception as e:
        print("Database Error:" + str(e))

    else:
        return dbname


def Table(database):
    connection = mysql.connector.connect(user='root',
                                         password='mysql',
                                         host='localhost')
    cursor = connection.cursor()
    try:
        table = 'sarthistores'
        table1_query = f""" CREATE TABLE IF NOT EXISTS {database}.{table}(
                                    Id INT AUTO_INCREMENT NOT NULL,
                                    Item VARCHAR(50),
                                    Quantity INT,
                                    Category VARCHAR(50),
                                    Customer VARCHAR(50),
                                    Customer_Type TEXT(60),
                                    Price INT,
                                    PRIMARY KEY(Id)
                                    );
                                    """
        cursor.execute(table1_query)
    except Exception as e:
        print("Table Error: " + str(e))

    else:
        return database, table

def Datainsertion():
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                           .format(user="root",
                                   pw="mysql",
                                   db="SarthiStore"))

    try:
        data.to_sql('sarthistores', con=engine, if_exists='append', chunksize=1000, index=False)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
            sys.exit(0)
        else:
            print( "Datainsertion Error is:" +" "+ str(err))
            sys.exit(0)

if __name__ == '__main__':
    database = Database()
    table = Table(database)
    Datainsertion()
