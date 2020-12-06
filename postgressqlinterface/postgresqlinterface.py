import psycopg2
import pandas as pd


class PostgreSQL:
    """Connect and interact with a posgreSQL database"""
    def __init__(self, database_url, sslmode='require', host='HEROKU'):
        """
        Initialise the class
        :param database_url: connection to database
        :param sslmode: sslmode
        """
        if host.upper() == 'HEROKU':
            self.database_url = database_url
            self.sslmode = sslmode
        else:
            raise Exception('No valid host has been provided when instantiating Pos')

    @staticmethod
    def close_connection(cursor, conn):
        """
        Method to close a database connection to a PosgreSQL database
        """
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    def create_connection(self):
        """
        Method to create a database connection to a Posgresql database
        :return: Connection object and cursor or None
        """
        cursor, conn = None, None

        try:
            conn = psycopg2.connect(self.database_url, sslmode=self.sslmode)
            cursor = conn.cursor()

        except psycopg2.Error as e:
            print(e)
            self.close_connection(cursor, conn)

        return cursor, conn

    def query(self, statement):
        """
        Method to retrieve data from a table.
        :param statement: sql statement

        :return: df: dataframe of the sql
        """
        conn, cursor = None, None
        df = pd.DataFrame()
        try:
            cursor, conn = self.create_connection()
            df = pd.read_sql_query(statement, conn)

        except psycopg2.Error as e:
            print(e)
        finally:
            self.close_connection(cursor, conn)

        return df

    def execute(self, statement):
        """
        Execute a statement in database
        param execute: parameter to execute
        """
        conn, cursor = None, None
        try:
            cursor, conn = self.create_connection()
            cursor.execute(statement)
            conn.commit()
        except psycopg2.Error as e:
            print(e)
        finally:
            self.close_connection(cursor, conn)

    def insert_table(self, table_name, df, print_sql=False):
        """
        This method is to insert new values in a table. It is able to manage insertion of null values.
        :param table_name: name of the table where data is going to be inserted, it must include the schema
        :param df: dataframe of values to insert into the table
        :param print_sql: boolean to indicate if sql statement must be print on python console
        :return:
        """
        # Creation of Insert into and list of columns
        base_statement = 'INSERT INTO %s (' % table_name
        for col in df.columns.values.tolist():
            base_statement += ' %s,' % col
        base_statement = base_statement[:-1] + ') VALUES ('

        # this is for nan detection to input NULL values
        nan_df = df.isna()

        # creation of insert statements
        sql_insert = ''
        for iindex in df.index.values.tolist():
            statement = base_statement
            for value in df.columns.values.tolist():
                if nan_df.loc[iindex, value]:
                    statement += " NULL,"
                else:
                    statement += " '%s'," % df.loc[iindex, value]
            statement = statement[:-1] + ')'

            sql_insert = sql_insert + '; ' + statement

        if print_sql:
            print(sql_insert)

        self.execute(sql_insert)

    def update_table(self, table_name, df, where_identifier, print_sql=False):
        """
        This method is to update values in a table taking into account the where_identifier. It creates one UPDATE
        statement for each row in df.
        :param table_name: name of the table to update
        :param df: dataframe with the data to update in the table
        :param where_identifier: list of columns to list on the where clause
        :param print_sql: boolean to indicate if sql statement must be print on python console
        :return:
        """
        df.reset_index(drop=True, inplace=True)

        # this is for nan detection to input NULL values
        nan_df = df.isna()

        # creation of the sql statement
        statement = ''
        for i in df.index.values.tolist():
            statement += ' UPDATE %s SET ' % table_name
            for col in [col_y for col_y in df.columns.values.tolist() if col_y not in where_identifier]:
                statement += " %s = " % col
                if nan_df.loc[i, col]:
                    statement += " NULL,"
                else:
                    statement += " '%s'," % (df.loc[i, col])

            statement = statement[:-1] + " WHERE "

            for j in range(0, len(where_identifier)):
                if j > 0:
                    statement += ' AND '
                statement += " %s = " % where_identifier[j]
                if nan_df.loc[i, where_identifier[j]]:
                    statement += " NULL "
                else:
                    statement += " '%s' " % (df.loc[i, where_identifier[j]])
            statement += '; '

        if print_sql:
            print(statement)

        self.execute(statement)
