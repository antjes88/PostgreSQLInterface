from dotenv import load_dotenv
import os
from postgresql_interface.postgresql_interface import postgres_sql_connector_factory

if not os.path.isfile('.env'):
    raise Exception('You need a file call .env with DATABASE_URL of the Heroku PostgreSQL Database you are going '
                    'to connect to.')
else:
    load_dotenv(dotenv_path='.env')
    database_url = os.environ['DATABASE_URL']


db_conn = postgres_sql_connector_factory(vendor='heroku', database_url=database_url)

print(db_conn)
db_conn.query("SELECT current_date")
