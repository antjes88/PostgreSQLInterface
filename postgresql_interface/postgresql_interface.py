import psycopg2
import pandas as pd


class PostgreSQL:
    """Connect and interact with a PostgreSQL database"""
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
            raise Exception('No valid host has been provided when instantiating the class.')

    @staticmethod
    def close_connection(cursor, conn):
        """
        Method to close a database connection to a PostgreSQL database
        """
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    def create_connection(self):
        """
        Method to create a database connection to a PostgreSQL database
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
        Execute a sql statement in database
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

            INSERT INTO table_name (df.column[0], df.column[1], ... df.column[n])
                VALUES (df.loc[0, column[0]], df.loc[0, column[1]], ... df.loc[0, column[n]]),
                       (df.loc[1, column[0]], df.loc[1, column[1]], ... df.loc[1, column[n]]),
                       .
                       .
                       .
                       (df.loc[n, column[0]], df.loc[n, column[1]], ... df.loc[n, column[n]]);

        :param table_name: name of the table where data is going to be inserted, it must include the schema
        :param df: dataframe of values to insert into the table
        :param print_sql: boolean to indicate if sql statement must be print on python console
        :return:
        """
        # Creation of Insert into and list of columns
        statement = 'INSERT INTO %s (' % table_name
        for col in df.columns.values.tolist():
            statement += ' %s,' % col
        statement = statement[:-1] + ') VALUES '

        # this is for nan detection to input NULL values
        nan_df = df.isna()

        # creation of insert statements
        for iindex in df.index.values.tolist():
            statement += ' ('
            for value in df.columns.values.tolist():
                if nan_df.loc[iindex, value]:
                    statement += " NULL,"
                else:
                    statement += " '%s'," % df.loc[iindex, value]
            statement = statement[:-1] + '),'

        statement = statement[:-1] + ';'

        if print_sql:
            print(statement)

        self.execute(statement)

    def update_table(self, table_name, df, where_identifier, print_sql=False):
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

        :param table_name: name of the table to update included schema
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

    def delete_from_table(self, table_name, df, print_sql=False):
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

        clause
        :param table_name: name of the table to update included schema
        :param df: dataframe with the columns of the table to be included on the where clause
        :param print_sql: boolean to indicate if sql statement must be print on python console
        :return:
        """
        if df.shape[0] == 0:
            raise Exception("Please, provide a non-empty dataframe.")

        elif len(df.columns.to_list()) == 1:
            col = df.columns.to_list()[0]
            nan_df = df.isna()
            statement = 'DELETE FROM %s WHERE %s IN (' % (table_name, col)
            for i in df.index.values.tolist():
                if nan_df.loc[i, col]:
                    statement += " NULL,"
                else:
                    statement += " '%s'," % (df.loc[i, col])
            statement = statement[:-1] + ')'

        else:
            nan_df = df.isna()
            cols = df.columns.to_list()
            statement = ''
            for i in df.index.values.tolist():
                statement += ' DELETE FROM %s WHERE ' % table_name
                for j in range(0, len(cols)):
                    if j > 0:
                        statement += ' AND '
                    statement += " %s = " % cols[j]
                    if nan_df.loc[i, cols[j]]:
                        statement += " NULL "
                    else:
                        statement += " '%s' " % (df.loc[i, cols[j]])
                statement += '; '

        if print_sql:
            print(statement)

        self.execute(statement)
