import psycopg2
import pandas as pd
from abc import ABCMeta, abstractmethod
from postgresql_interface.sql_writer import SQLWriter
import warnings


class PostgresSQLConnector(metaclass=ABCMeta):
    """
    Abstract Class to use as base for an API to interact with PostgreSQL databases at different platforms.

    To use it, you will need to overwrite the create_connection() method on your child class.
    """
    @staticmethod
    def close_connection(cursor, conn):
        """
        Method to close a database connection to a PostgreSQL database.

        Args:
            cursor: Allows Python code to execute PostgreSQL command in a database session.
                class Cursor from psycopg2
            conn: handles connection to a PostgreSQL database. class Connection from psycopg2
        """
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    @abstractmethod
    def create_connection(self):
        """
        Method to create a database connection to a PostgreSQL database

        Raises:
            NotImplementedError: Must be overridden on children class
        """
        NotImplementedError("Must be overridden on children class")

    class SQLWriter(SQLWriter):
        pass

    def query(self, statement):
        """
        Retrieves data from a sql statement as a Pandas dataframe.
        It handles transactions with databases. It handles full connection life with the database and ensures that
        connection is closed at the end no matter if the query was successful or unsuccessful.

        Args:
            statement: sql statement to evaluate at database. Must be a str.

        Returns:
            dataframe resulting from query to database.

        Raises:
            psycopg2.Error: in case of a problem handling query to database.
        """
        conn, cursor, error = None, None, None
        df = pd.DataFrame()
        try:
            cursor, conn = self.create_connection()
            df = pd.read_sql_query(statement, conn)

        except psycopg2.Error as e:
            error = e
        finally:
            self.close_connection(cursor, conn)
            if error:
                raise Exception(error)

        return df

    def execute(self, statement):
        """
        Execute a sql statement in database.
        Transaction is fully handle by the method. The strategy is that transaction is only committed if all statement
        execution is successful. It handles full connection life with the database and ensures that
        connection is closed at the end no matter if the query was successful or unsuccessful.

        Args:
            statement: sql statement with SQL to be executed at database. Must be a str.

        Raises:
            psycopg2.Error: in case of a problem handling query to database.
        """
        conn, cursor, error = None, None, None
        try:
            cursor, conn = self.create_connection()
            cursor.execute(statement)
            conn.commit()
        except psycopg2.Error as e:
            error = e
        finally:
            self.close_connection(cursor, conn)
            if error:
                raise Exception(error)

    def insert_table(self, table_name, df, print_sql=False, truncate=False, sql_injection_check_enabled=True):
        """
        This method is to insert new values in a table. It is able to manage insertion of null values.

            INSERT INTO table_name (df.column[0], df.column[1], ... df.column[n])
                VALUES (df.loc[0, column[0]], df.loc[0, column[1]], ... df.loc[0, column[n]]),
                       (df.loc[1, column[0]], df.loc[1, column[1]], ... df.loc[1, column[n]]),
                       .
                       .
                       .
                       (df.loc[n, column[0]], df.loc[n, column[1]], ... df.loc[n, column[n]]);

        Args:
            table_name: name of the table where data is going to be inserted, it must include the table schema.
            df: dataframe of values to insert into the table.
            print_sql: boolean to indicate if sql statement must be print on python console.
            truncate: before inserting data into a table, it is truncated.
            sql_injection_check_enabled: allows to disable SQL Injection check.

        Raises:
            psycopg2.Error: in case of a problem handling query to database.
        """
        if (df.shape[0] == 0) & (df.columns.to_list().__len__() > 0):
            warnings.warn("Dataframe provided to insert into %s is empty." % table_name)

        else:
            statement = self.SQLWriter.create_insert_table_statement(
                table_name, df, truncate, sql_injection_check_enabled=sql_injection_check_enabled)
            if print_sql:
                print(statement)
            self.execute(statement)

    def update_table(self, table_name, df, where_identifier, print_sql=False, sql_injection_check_enabled=True):
        """
        This method is to update values in a table taking into account the where_identifier. It creates one UPDATE
        statement for each row in df.

            WITH to_update = [col for col in df.columns if col not in where_identifier]
                UPDATE table_name
                SET to_update[0] = df.loc[0, to_update[0]]
                    .
                    .
                    .
                    to_update[n] = df.loc[0, to_update[n]]
                WHERE where_identifier[0] = df.loc[0, where_identifier[0]]
                      .
                      .
                      .
                      where_identifier[n] = df.loc[0, where_identifier[n]];
                .
                .
                .
                UPDATE table_name
                SET to_update[0] = df.loc[n, to_update[0]]
                    .
                    .
                    .
                    to_update[n] = df.loc[n, to_update[n]]
                WHERE where_identifier[0] = df.loc[n, where_identifier[0]]
                      .
                      .
                      .
                      where_identifier[n] = df.loc[n, where_identifier[n]];

        Args:
            table_name: name of the table to update included schema.
            df: dataframe with the data to update in the table.
            where_identifier: list of columns to list on the where clause.
            print_sql: boolean to indicate if sql statement must be print on python console.
            sql_injection_check_enabled: allows to disable SQL Injection check.

        Raises:
            psycopg2.Error: in case of a problem handling query to database.
        """
        if (df.shape[0] == 0) & (df.columns.to_list().__len__() > 0):
            warnings.warn("Dataframe provided to insert into %s is empty." % table_name)

        elif len([col for col in df.columns if col.upper() not in [col.upper() for col in where_identifier]]) <= 0:
            raise Exception(
                "Cannot create update operation on table %s as there are no columns to update" % table_name)

        else:
            statement = self.SQLWriter.create_update_table_statement(
                table_name, df.copy(), where_identifier, sql_injection_check_enabled=sql_injection_check_enabled)
            if print_sql:
                print(statement)
            self.execute(statement)

    def delete_from_table(self, table_name, df, print_sql=False, sql_injection_check_enabled=True):
        """
        Method to delete rows from a table. It deletes rows from table_name based on the where clause created with
        values and columns of df.

            IF len(df.columns) == 1:
                DELETE FROM table_name  WHERE df.column IN (df.values)
            IF len(df.columns) > 1:
                DELETE FROM table_name  WHERE df.column[0] == 'df.loc[0, column[0]]'......
                                            AND df.column[n] == 'df.loc[0, column[n]]';
                .
                .
                .
                DELETE FROM table_name  WHERE df.column[0] == 'df.loc[n, column[0]]'......
                                            AND df.column[n] == 'df.loc[n, column[n]]';

        Args:
            table_name: name of the table to update included schema.
            df: dataframe with the columns of the table to be included on the where clause.
            print_sql: boolean to indicate if sql statement must be print on python console.
            sql_injection_check_enabled: allows to disable SQL Injection check.

        Raises:
            psycopg2.Error: in case of a problem handling query to database.
        """
        if (df.shape[0] == 0) & (df.columns.to_list().__len__() > 0):
            warnings.warn("Dataframe provided to insert into %s is empty." % table_name)

        else:
            statement = self.SQLWriter.create_delete_from_table_statement(
                table_name, df, sql_injection_check_enabled=sql_injection_check_enabled)
            if print_sql:
                print(statement)
            self.execute(statement)


class PostgresHeroku(PostgresSQLConnector):
    """
    API to interact with PostgreSQL databases at different platforms.

    Args:
        database_url: connection to database. It has next schema:
            postgres://{user_name}:{password}@{host_name}:{port_number}/{database_name}
            you can find it in your Heroku datastore settings. Database credentials -> URI
        ssl_mode: ssl_mode
    """
    def __init__(self, database_url, ssl_mode='require'):
        self.vendor = 'Heroku'
        self.database_url = database_url
        self.sslmode = ssl_mode

    def create_connection(self):
        """
        Method to create a database connection to a Heroku-Amazon PostgreSQL database

        Raises:
            psycopg2.Error: in case of a problem handling query to database.
        """
        cursor, conn = None, None

        try:
            conn = psycopg2.connect(self.database_url, sslmode=self.sslmode)
            cursor = conn.cursor()

        except psycopg2.Error as e:
            self.close_connection(cursor, conn)
            raise Exception(e)

        return cursor, conn

    def __repr__(self):
        return 'PostgresHeroku({database_url}, "' + self.sslmode + '")'

    def __str__(self):
        return 'API to interact with a Heroku PostgresSQL database using simple SQL style methods'

# class PostgresGCP(PostgresSQLConnector):
#     def __init__(self, host, database_name, user_name, user_password):
#         """
#         Initialisation of the class
#         :param host: connection to server
#         :param database_name: name of the database
#         :param user_name: name of a user with permission to connect and perform the desires operations on the database
#         :param user_password: password of the user
#         """
#         self.vendor = 'GCP'
#         self.host = host
#         self.database = database_name
#         self.user = user_name
#         self.password = user_password
#
#     def create_connection(self):
#         """
#         Method to create a database connection to a Heroku-Amazon PostgreSQL database
#         :return: Connection object and cursor or None
#         """
#         cursor, conn = None, None
#
#         try:
#             conn = psycopg2.connect(host=self.host, database=self.database, user=self.user, password=self.password)
#             cursor = conn.cursor()
#
#         except psycopg2.Error as e:
#             self.close_connection(cursor, conn)
#             raise Exception(e)
#
#         return cursor, conn


def postgres_sql_connector_factory(vendor, **kwargs):
    """
    Factory method that returns the right API given the provided vendor.

    Example of use:
    ```
    from postgresql_interface.postgresql_interface import postgres_sql_connector_factory
    db_conn = postgres_sql_connector_factory(vendor='heroku', database_url=database_url)
    ```

    Args:
        vendor: name of the cloud vendor. Must be a str.
        **kwargs: named arguments to be passed to the API sql database.

    Returns:
        depending on vendor:
            - Heroku: object of class PostgresHeroku

    Raises:
        ValueError: if the vendor is not yet implemented
    """
    if vendor.upper() == 'HEROKU':
        return PostgresHeroku(**kwargs)
    # elif vendor.upper() == 'GCP':
    #     return PostgresGCP(**kwargs)
    else:
        raise ValueError('No valid vendor has been provided when instantiating the class.')
