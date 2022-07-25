import pandas as pd
import numpy as np
import datetime as dt
import pytest
from postgresql_interface.postgresql_interface import postgres_sql_connector_factory


@pytest.fixture(scope='function')
def execute_insert_query(create_env_variables_heroku):
    heroku_db = postgres_sql_connector_factory(vendor='heroku', database_url=create_env_variables_heroku)
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
    heroku_db.SQLWriter.create_insert_table_statement('test.simple', to_insert.copy())
    heroku_db.insert_table('test.simple', to_insert.copy())
    simple = heroku_db.query("SELECT * FROM test.simple")

    yield to_insert, simple

    heroku_db.execute("Drop table if exists test.simple; drop schema if exists test")


def test_execute_insert_query(execute_insert_query):
    """
    GIVEN a dataframe to insert into a heroku database
    WHEN it is inserted into the database
    THEN check that the values on the dataframe have been correctly created in the database
    :param execute_insert_query: fixture above
    :return:
    """
    assert execute_insert_query[0].equals(execute_insert_query[1])


####################################################################################


@pytest.fixture(scope='function')
def update(create_env_variables_heroku):
    heroku_db = postgres_sql_connector_factory(vendor='heroku', database_url=create_env_variables_heroku)
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
                                        'date': [dt.datetime(2020, 1, 1), dt.date(2020, 2, 2),
                                                 dt.datetime(2020, 3, 3), dt.date(2020, 4, 4)]})
    heroku_db.insert_table('test.simple', to_insert.copy())

    to_update = pd.DataFrame.from_dict({'id': [1, 2, 3, 4],
                                        'name': ['Ford', 'Vauxhall', 'SEAT', 'FIAT'],
                                        'activated': [False, True, True, False],
                                        'date': [dt.date(2020, 1, 1), dt.date(2020, 2, 2),
                                                 dt.date(2020, 3, 3), dt.date(2020, 4, 4)]})
    heroku_db.update_table('test.simple', to_update, ['Id', 'Date'])

    simple = heroku_db.query("SELECT * FROM test.simple")
    yield to_update, simple

    heroku_db.execute("Drop table if exists test.simple; drop schema if exists test")


def test_execute_insert_update_query(update):
    """
    GIVEN a table in a heroku database
    WHEN it is updated
    THEN check that the values in the database are correctly updated
    :param update: fixture above
    :return:
    """
    assert update[0].equals(update[1])


###############################################################################################
@pytest.fixture(scope='function')
def heroku_conn(create_env_variables_heroku):
    heroku_db = postgres_sql_connector_factory(vendor='heroku', database_url=create_env_variables_heroku)
    statement_create_table = """
                            Drop table if exists test.simple; 
                            drop schema if exists test;
                            
                            CREATE SCHEMA test
                        
                            create table test.simple (
                                Id INT not null,
                                Name VARCHAR(100)  NULL,
                                Activated BOOLEAN not null,
                                Date DATE NOT NULL
                            )
    """
    heroku_db.execute(statement_create_table)

    to_insert = pd.DataFrame.from_dict({'id': [1, 2, 3, 4],
                                        'name': ['Mercedes', np.nan, 'Suzuki', 'BMW'],
                                        'activated': [True, False, False, True],
                                        'date': [dt.datetime(2020, 1, 1), dt.datetime(2020, 2, 2),
                                                 dt.date(2020, 3, 3), dt.date(2020, 4, 4)]})
    heroku_db.insert_table('test.simple', to_insert.copy())

    yield heroku_db

    heroku_db.execute("Drop table if exists test.simple; drop schema if exists test")


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
    before_delete = heroku_conn.query("SELECT * FROM test.simple")
    heroku_conn.delete_from_table('test.simple', to_delete)
    simple = heroku_conn.query("SELECT * FROM test.simple")

    assert before_delete.shape[0] == 4
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
    before_delete = heroku_conn.query("SELECT * FROM test.simple")
    heroku_conn.delete_from_table('test.simple', to_delete)
    simple = heroku_conn.query("SELECT * FROM test.simple")

    assert before_delete.shape[0] == 4
    assert simple.shape[0] == 0


def test_truncate_true(heroku_conn):
    """
    GIVEN a table in a heroku database
    WHEN an insert is done after a truncate
    THEN check that previous values are actually deleted
    :param heroku_conn: fixture above
    :return:
    """
    # this is to make sure that actually there is something in the table before truncating it
    n_rows = heroku_conn.query("SELECT COUNT(*) AS count FROM test.simple").loc[0, 'count']

    # actual test
    to_insert = pd.DataFrame.from_dict({'id': [1, 2], 'name': ['Ford', 'Tesla'], 'activated': [True, False],
                                        'date': [dt.date(2020, 1, 1), dt.date(2020, 2, 2)]})
    heroku_conn.insert_table('test.simple', to_insert.copy(), truncate=True)
    simple = heroku_conn.query("SELECT * FROM test.simple")

    # first assert is to make sure that actually there is something before truncating the table
    assert n_rows > 0
    assert to_insert.equals(simple)
