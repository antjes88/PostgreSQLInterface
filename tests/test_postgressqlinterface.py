import os
import pandas as pd
import numpy as np
import datetime as dt
import pytest
from dotenv import load_dotenv
from postgresql_interface.postgresql_interface import PostgreSQL

if os.path.isfile('.env'):
    load_dotenv(dotenv_path='.env')
env_var = os.environ['DATABASE_URL']


@pytest.fixture(scope='module')
def execute_insert_query():
    heroku_db = PostgreSQL(env_var)
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
    """
    GIVEN a dataframe to insert into a heroku database
    WHEN it is inserted into the database
    THEN check that the values on the dataframe have been correctly created in the database
    :param execute_insert_query: fixture above
    :return:
    """
    assert execute_insert_query.all().all()


####################################################################################


@pytest.fixture(scope='module')
def update():
    heroku_db = PostgreSQL(env_var)
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
    """
    GIVEN a table in a heroku database
    WHEN it is updated
    THEN check that the values in the database are correctly updated
    :param update: fixture above
    :return:
    """
    assert update.all().all()


###############################################################################################

@pytest.fixture(scope='function')
def heroku_conn():
    heroku_db = PostgreSQL(env_var)
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
                                        'name': ['Mercedes', np.nan, 'Suzuki', 'BMW'],
                                        'activated': [True, False, False, True],
                                        'date': [dt.date(2020, 1, 1), dt.date(2020, 2, 2),
                                                 dt.date(2020, 3, 3), dt.date(2020, 4, 4)]})
    heroku_db.insert_table('test.simple', to_insert.copy())

    yield heroku_db

    heroku_db.execute("Drop table test.simple; drop schema test")


def test_delete_several_cols(heroku_conn):
    """
    GIVEN a table in a heroku database
    WHEN some rows are deleted using several columns in the where clause
    THEN check that actually those rows are deleted
    :param heroku_conn: fixture above
    :return:
    """
    to_delete = pd.DataFrame.from_dict({'id': [1, 2, 3, 4],
                                        'date': [dt.date(2020, 1, 1), dt.date(2020, 2, 2),
                                                 dt.date(2020, 3, 3), dt.date(2020, 4, 4)]})
    heroku_conn.delete_from_table('test.simple', to_delete)
    simple = heroku_conn.query("SELECT * FROM test.simple")

    assert simple.shape[0] == 0


def test_delete_one_col(heroku_conn):
    """
    GIVEN a table in a heroku database
    WHEN some rows are deleted using one column in the where clause
    THEN check that actually those rows are deleted
    :param heroku_conn: fixture above
    :return:
    """
    to_delete = pd.DataFrame.from_dict({'id': [1, 2, 3, 4]})
    heroku_conn.delete_from_table('test.simple', to_delete)
    simple = heroku_conn.query("SELECT * FROM test.simple")

    assert simple.shape[0] == 0
