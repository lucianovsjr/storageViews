import time
import logging
import sqlite3
import pymssql

from datetime import datetime
from pymongo import MongoClient
from decouple import config


logging.basicConfig(filename='app.log', level=logging.INFO)
logging.info('Exec Inicio {}'.format(datetime.now()))

sqlite_conn = sqlite3.connect(config('SQLITE_DATABASE'))
sqlite_cursor = sqlite_conn.cursor()

sqlite_cursor.execute('SELECT name FROM sqlite_master WHERE name = "views"')
if not sqlite_cursor.fetchone():
    sqlite_cursor.execute('CREATE TABLE views (name TEXT, enabled INTEGER)')

mssql_conn = pymssql.connect(
    config('MSSQL_HOST'),
    config('MSSQL_USER'),
    config('MSSQL_PASSWORD'),
    config('MSSQL_DATABASE')
)
mssql_cursor = mssql_conn.cursor(as_dict=True)

try:
    mssql_cursor.execute('SELECT DISTINCT(ObjectName) FROM VPERMVIEWS')
except Exception as e:
    print(e)

for row in mssql_cursor:
    view_name = row.get('ObjectName')

    sqlite_cursor.execute(f'SELECT name FROM views WHERE name = "{view_name}"')
    if not sqlite_cursor.fetchone():
        sqlite_cursor.execute(f'INSERT INTO views VALUES ("{view_name}", 1)')

views = []
sqlite_cursor.execute('SELECT name FROM views WHERE enabled = 1')
for row in sqlite_cursor:
    views.append(row[0])
views = []

sqlite_conn.commit()
sqlite_conn.close()

mongo_client = MongoClient(config('MONGO_HOST'))
mongo_db = mongo_client.views

for view in views:
    mongo_collection = mongo_db[view]

    time_start = time.time()
    try:
        mssql_cursor.execute(f'SELECT * FROM {view}')
    except Exception as e:
        logging.info('{} Erro {}'.format(view, e)

    for row in mssql_cursor:
        row_validated = dict()
        for key, value in row.items():
            row_validated[key.replace('.', '_')] = value

        mongo_collection.insert_one(row_validated)

    time_end = time.time()
    logging.info('{} Time {:.2f}m'.format(view, (time_end - time_start) / 60))

logging.info('Exec Fim {}'.format(datetime.now()))

mssql_conn.close()
