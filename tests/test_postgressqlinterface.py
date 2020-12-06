import os
import pandas as pd
import datetime as dt
import pytest
from dotenv import load_dotenv
from PostgresSQLInterface.postgresqlinterface import PosgreSQL


"""
To be able to execute this test statements you need to provide a .env file with the url to connect to a database
"""

load_dotenv(dotenv_path='.env')
env_var = os.environ['DATABASE_URL']


@pytest.fixture(scope='module')
def execute_insert_query():
    heroku_db = PosgreSQL(env_var)
    statement_create_table = """
                            Drop table if exists test.simple; 
                            drop schema if exists test;
                            
                            CREATE SCHEMA test
                        
                            create table test.simple (
                                Id INT not null,
                                Name VARCHAR(100) NOT NULL,
                                Activated BOOLEAN not null,
                                Date DATE NOT NULL
                            )
    """
    heroku_db.execute(statement_create_table)
    to_insert = pd.DataFrame.from_dict({'id': [1, 2, 3, 4],
                                        'name': ['Mercedes', 'Toyota', 'Suzuki', 'BMW'],
                                        'activated': [True, False, False, True],
                                        'date': [dt.date(2020, 1, 1), dt.date(2020, 2, 2),
                                                 dt.date(2020, 3, 3), dt.date(2020, 4, 4)]})
    heroku_db.insert_table('test.simple', to_insert.copy())
    simple = heroku_db.query("SELECT * FROM test.simple")

    yield to_insert.sort_index().sort_index(axis=1) == simple.sort_index().sort_index(axis=1)

    heroku_db.execute("Drop table test.simple; drop schema test")


def test_execute_insert_query_heroku(execute_insert_query):
    assert execute_insert_query.all().all()


####################################################################################


@pytest.fixture(scope='module')
def update():
    heroku_db = PosgreSQL(env_var)
    statement_create_table = """
                            Drop table if exists test.simple; 
                            drop schema if exists test;
                            
                            CREATE SCHEMA test
                        
                            create table test.simple (
                                Id INT not null,
                                Name VARCHAR(100) NOT NULL,
                                Activated BOOLEAN not null,
                                Date DATE NOT NULL
                            )
    """
    heroku_db.execute(statement_create_table)

    to_insert = pd.DataFrame.from_dict({'id': [1, 2, 3, 4],
                                        'name': ['Mercedes', 'Toyota', 'Suzuki', 'BMW'],
                                        'activated': [True, False, False, True],
                                        'date': [dt.date(2020, 1, 1), dt.date(2020, 2, 2),
                                                 dt.date(2020, 3, 3), dt.date(2020, 4, 4)]})
    heroku_db.insert_table('test.simple', to_insert.copy())

    to_update = pd.DataFrame.from_dict({'id': [1, 2, 3, 4],
                                        'name': ['Ford', 'Vauxhall', 'SEAT', 'FIAT'],
                                        'activated': [False, True, True, False],
                                        'date': [dt.date(2020, 1, 1), dt.date(2020, 2, 2),
                                                 dt.date(2020, 3, 3), dt.date(2020, 4, 4)]})
    heroku_db.update_table('test.simple', to_update, ['id', 'date'])

    simple = heroku_db.query("SELECT * FROM test.simple")
    yield to_update.sort_index().sort_index(axis=1) == simple.sort_index().sort_index(axis=1)

    heroku_db.execute("Drop table test.simple; drop schema test")


def test_execute_insert_update_query(update):
    assert update.all().all()
