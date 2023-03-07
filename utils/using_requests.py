import os
import requests
import psycopg2
from psycopg2.extras import execute_values

DATABASE = os.environ.get('DATABASE')
USER = os.environ.get('USER')
PASSWORD = os.environ.get('PASSWORD')
HOST = os.environ.get('HOST')
PORT = os.environ.get('PORT')

filepath = 'https://random-data-api.com/api/cannabis/random_cannabis?size=10'
tablename = 'api_raw_data'


def extract_data():
    api_response = requests.get(filepath).json()
    return api_response


def load_data(data):
    try:
        connection = psycopg2.connect(user=USER,
                                      password=PASSWORD,
                                      host=HOST,
                                      port=PORT,
                                      database=DATABASE)
        cursor = connection.cursor()

        columns = data[0].keys()
        query = f"insert into {tablename} ({','.join(columns)}) values %s"

        values = [[value for value in line.values()] for line in data]

        execute_values(cursor, query, values)
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into table", error)

    finally:
        if connection:
            cursor.close()
            connection.close()


def run_process():
    load_data(extract_data())
