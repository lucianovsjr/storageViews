import time
import logging
import sqlite3
import pymssql

from datetime import datetime
from pymongo import MongoClient
from decouple import config, Csv


DISCARD_VIEWS = config('DISCARD_VIEWS', cast=Csv())
DROP_USER = config('DROP_USER', cast=bool)

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
# Sincronizar Views
for row in mssql_cursor:
    view_name = row.get('ObjectName')

    sqlite_cursor.execute(f'SELECT name, enabled FROM views WHERE name = "{view_name}"')
    sqlite_row = sqlite_cursor.fetchone()

    if not sqlite_row:
        sqlite_cursor.execute(f'INSERT INTO views VALUES ("{view_name}", 1)')
        logging.info(f'New View {view_name}')

    if view_name in DISCARD_VIEWS:
        sqlite_cursor.execute(f'UPDATE views SET enabled = 0 WHERE name = "{view_name}"')
        logging.info(f'Discard View {view_name}')

views = []
sqlite_cursor.execute('SELECT name FROM views WHERE enabled = 1')
for row in sqlite_cursor:
    views.append(row[0])

sqlite_conn.commit()
sqlite_conn.close()

mongo_client = MongoClient(config('MONGO_HOST'))
mongo_db = mongo_client.views

# Sincronizar usários
try:
    mssql_cursor.execute('''
        SELECT
            SUBSTRING(DatabaseUserName, 15, len(DatabaseUserName) - 13) AS DatabaseUserName,
            Stuff(
                (
                    Select ',' + Cast(ObjectName As varchar(100)) From VPERMVIEWS VIEWS
                    Where VIEWS.DatabaseUserName = VPERMVIEWS.DatabaseUserName
                        For Xml Path('')
                ), 1, 1, ''
            ) AS ObjectName
        FROM VPERMVIEWS
        GROUP BY DatabaseUserName
        ORDER BY DatabaseUserName
    ''')
except Exception as e:
    print(e)
for row in mssql_cursor:
    db_user = row.get('DatabaseUserName')
    db_pass = '123456'
    views_name = row.get('ObjectName')

    roles_info = mongo_db.command('rolesInfo', db_user)
    mongo_db.command(
        'updateRole' if roles_info['roles'] else 'createRole',
        db_user,
        privileges = [{
            'actions': ['find'],
            'resource': {'db': 'views', 'collection': view_name}
        } for view_name in views_name.split(',')],
        roles = []
    )

    users_info = mongo_db.command('usersInfo', db_user)
    if not users_info['users']:
        logging.info(f'New User {db_user} {db_pass}')
        mongo_db.command('createUser', db_user, pwd=db_pass, roles=[db_user])

    if DROP_USER:
        mongo_db.command('dropRole', db_user)
        mongo_db.command('dropUser', db_user)

for view in views:
    mongo_collection = mongo_db[view]

    time_start = time.time()
    try:
        mssql_cursor.execute(f'DELETE FROM {view}')
        mssql_cursor.execute(f'SELECT * FROM {view}')
    except Exception as e:
        logging.info('{} Erro {}'.format(view, e))

    for row in mssql_cursor:
        row_validated = dict()
        for key, value in row.items():
            row_validated[key.replace('.', '_')] = value

        mongo_collection.insert_one(row_validated)

    time_end = time.time()
    logging.info('{} Time {:.2f}m'.format(view, (time_end - time_start) / 60))

logging.info('Exec Fim {}'.format(datetime.now()))

mssql_conn.close()
