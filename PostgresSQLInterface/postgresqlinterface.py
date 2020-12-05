import psycopg2
import pandas as pd


class PosgreSQL:
    """Connect and interact with a posgreSQL database in Heroku"""
    def __init__(self, database_url, sslmode='require', host='Heroku'):
        """
        Initialise the class
        :param database_url: connection to database
        :param sslmode: sslmode
        """
        if host == 'Heroku':
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
        Method to retrieve data from a table. It can convert dates from the format in the database to the format desired
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

    def insert_table(self, table_name, df, time_format=None, print_sql=False):
        """
        This method is to insert new values on the tables. It is able to manage insertion of null values.

        :param table_name: name of the table where data is going to be inserted, it must include the schema
        :param df: dataframe of values to insert into the table
        :param time_format: dict with key as column name and content as format desired
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

        if time_format:
            for key in time_format:
                df[key] = df[key].dt.strftime(time_format[key])

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
        This method is to update values in a table taking into account the where_identifier
        :param table_name: name of the table to update
        :param df: dataframe with the data to update in the table
        :param where_identifier: column identifier used to update the table
        :param print_sql: boolean to indicate if sql statement must be print on python console
        :return:
        """
        df.reset_index(drop=True, inplace=True)
        if df.shape[0] != len(where_identifier):
            return 'Length of dataframe and where_identifier differ.'
        base_statement = 'UPDATE %s SET ' % table_name

        for i in df.index.values.tolist():
            statement = base_statement
            for col in [col_y for col_y in df.columns.values.tolist() if i != where_identifier]:
                statement += "%s = '%s', '" % (col, df.loc[i, col])
            statement = statement[:-2] + ("WHERE %s = '%s'" % (where_identifier, df.loc[i, where_identifier]))

            if print_sql:
                print(statement)

            self.execute(statement)

    def delete_from_table(self, table_name, col, col_values, print_sql=False):
        """
        Method to delete rows from a table base on a column value
        :param table_name: name of the table from which data is going to be drop
        :param col: name of the column for the where clause
        :param col_values: list of values to identify the row
        :param print_sql: boolean to indicate if sql statement must be print on python console
        :return:
        """
        number_types = (int, float, complex)
        base_statement = 'DELETE FROM %s WHERE %s = ' % (table_name, col)

        for value in col_values:
            statement = base_statement
            if isinstance(value, number_types):
                statement += ("%s" % value)
            else:
                statement += ("'%s'" % value)
            if print_sql:
                print(statement)
            self.execute(statement)
