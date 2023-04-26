from dotenv import load_dotenv
import os
from postgresql_interface.postgresql_interface import postgres_sql_connector_factory

# Heroku
if not os.path.isfile('.env'):
    raise FileNotFoundError(
        'You need a file call .env with DATABASE_URL of the Heroku PostgreSQL Database you are going to connect to.'
    )
else:
    load_dotenv(dotenv_path='.env')
    database_url = os.environ['DATABASE_URL']


db_conn = postgres_sql_connector_factory(vendor='heroku', database_url=database_url)

print(db_conn)
db_conn.query("SELECT current_date")


# GCP
if not os.path.isfile('.env'):
    raise FileNotFoundError(
        'You need a file call .env with DATABASE_URL of the Heroku PostgreSQL Database you are going to connect to.'
    )
else:
    load_dotenv(dotenv_path='.env')
    host = os.environ['HOST']
    database_name = os.environ['DATABASE_NAME']
    user_name = os.environ['USER_NAME']
    user_password = os.environ['USER_PASSWORD']
    port = os.environ['PORT']

db_conn = postgres_sql_connector_factory(
    vendor='GCP', host=host, database_name=database_name, user_name=user_name, user_password=user_password, port=port)

print(db_conn)
a = db_conn.query("SELECT current_date")
