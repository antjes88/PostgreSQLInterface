import numbers
from postgresql_interface.sql_injection_bodyguard import SQLInjectionBodyguard


class SQLWriter:
    """
    Trait that writes sql statements given an input dataframe.

    Current valid methods are:
    - INSERT INTO
    - UPDATE
    - DELETE FROM
    """
    @staticmethod
    def create_insert_table_statement(table_name, df, truncate=False, sql_injection_check_enabled=True):
        """
        This method returns a sql statement to insert new values into a table.

            INSERT INTO table_name (df.column[0], df.column[1], ... df.column[n])
                VALUES (df.loc[0, column[0]], df.loc[0, column[1]], ... df.loc[0, column[n]]),
                       (df.loc[1, column[0]], df.loc[1, column[1]], ... df.loc[1, column[n]]),
                       .
                       .
                       .
                       (df.loc[n, column[0]], df.loc[n, column[1]], ... df.loc[n, column[n]]);

        - Args:
            table_name: name of the table where data is going to be inserted, it must include the schema
            df: dataframe of values to insert into the table
            truncate: before inserting data into a table, it is truncated if boolean is True
            sql_injection_check_enabled: allows to disable SQL Injection check

        - Returns:
            String containing the INSERT INTO sql statement

        - Raises:
            ErrorPossibleSQLInjectionDetected: if a possible SQL injection is detected
        """
        if truncate:
            statement = 'TRUNCATE TABLE %s; ' % table_name
        else:
            statement = ''

        statement += 'INSERT INTO %s (' % table_name
        for col in df.columns.values.tolist():
            SQLInjectionBodyguard.check_string_on_insert(col, col, enabled=sql_injection_check_enabled)
            statement += ' %s,' % col
        statement = statement[:-1] + ') VALUES '

        df.reset_index(drop=True, inplace=True)
        nan_df = df.isna()
        for index in df.index.values.tolist():
            statement += ' ('
            for column in df.columns.values.tolist():
                if nan_df.loc[index, column]:
                    statement += " NULL,"
                else:
                    if isinstance(df.loc[index, column], numbers.Number):
                        statement += " %s ," % df.loc[index, column]
                    elif isinstance(df.loc[index, column], str):
                        SQLInjectionBodyguard.check_string_on_insert(
                            df.loc[index, column], column, enabled=sql_injection_check_enabled)
                        statement += " '%s' ," % df.loc[index, column]
                    else:
                        statement += " '%s' ," % df.loc[index, column]
            statement = statement[:-1] + '),'

        statement = statement[:-1] + ';'

        return statement

    @staticmethod
    def create_update_table_statement(table_name, df, where_identifier, sql_injection_check_enabled=True):
        """
        This method returns a sql statement to update values in a table taking into account the where_identifier.
        It creates one UPDATE statement for each row in df.

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

        - Args:
            table_name: name of the table to update included schema
            df: dataframe with the data to update in the table
            where_identifier: list of columns to list on the where clause
            sql_injection_check_enabled: allows to disable SQL Injection check

        - Returns:
            String containing the UPDATE ... SET sql statement

        - Raises:
            ErrorPossibleSQLInjectionDetected: if a possible SQL injection is detected
        """
        df.reset_index(drop=True, inplace=True)

        for col in df.columns:
            df.rename(columns={col: col.upper()}, inplace=True)
        where_identifier_upper = [col.upper() for col in where_identifier]

        nan_df = df.isna()
        statement = ''
        for i in df.index.values.tolist():
            statement += ' UPDATE %s SET ' % table_name
            for col in [col_y for col_y in df.columns.values.tolist() if col_y not in where_identifier_upper]:
                SQLInjectionBodyguard.check_string_on_insert(col, col, enabled=sql_injection_check_enabled)
                statement += " %s = " % col
                if nan_df.loc[i, col]:
                    statement += " NULL,"
                else:
                    SQLInjectionBodyguard.check_string_on_insert(
                        df.loc[i, col], col, enabled=sql_injection_check_enabled)
                    statement += " '%s'," % (df.loc[i, col])

            statement = statement[:-1]

            if where_identifier_upper.__len__() > 0:
                statement += " WHERE "
                for j in range(0, len(where_identifier_upper)):
                    if j > 0:
                        statement += ' AND '
                    SQLInjectionBodyguard.check_string_on_insert(
                        where_identifier_upper[j], where_identifier_upper[j], enabled=sql_injection_check_enabled)
                    statement += " %s = " % where_identifier_upper[j]
                    if nan_df.loc[i, where_identifier_upper[j]]:
                        statement += " NULL "
                    else:
                        SQLInjectionBodyguard.check_string_on_insert(
                            df.loc[i, where_identifier_upper[j]],
                            where_identifier_upper[j],
                            enabled=sql_injection_check_enabled
                        )
                        statement += " '%s' " % (df.loc[i, where_identifier_upper[j]])

            statement += '; '

        return statement

    @staticmethod
    def create_delete_from_table_statement(table_name, df, sql_injection_check_enabled=True):
        """
        This method returns a sql statement to delete rows from a table.
        It deletes rows from table_name based on the where clause created with values and columns of df.

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

        - Args:
            table_name: name of the table to update included schema
            df: dataframe with the columns of the table to be included on the where clause
            sql_injection_check_enabled: allows to disable SQL Injection check

        - Returns:
            String containing the DELETE FROM sql statement

        - Raises:
            ErrorPossibleSQLInjectionDetected: if a possible SQL injection is detected
        """
        df.reset_index(drop=True, inplace=True)
        if len(df.columns.to_list()) == 1:
            col = df.columns.to_list()[0]
            nan_df = df.isna()
            SQLInjectionBodyguard.check_string_on_insert(col, col, enabled=sql_injection_check_enabled)
            statement = 'DELETE FROM %s WHERE %s IN (' % (table_name, col)
            for i in df.index.values.tolist():
                if nan_df.loc[i, col]:
                    statement += " NULL,"
                else:
                    SQLInjectionBodyguard.check_string_on_insert(
                        df.loc[i, col], col, enabled=sql_injection_check_enabled)
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
                    SQLInjectionBodyguard.check_string_on_insert(
                        cols[j], cols[j], enabled=sql_injection_check_enabled)
                    statement += " %s = " % cols[j]
                    if nan_df.loc[i, cols[j]]:
                        statement += " NULL "
                    else:
                        SQLInjectionBodyguard.check_string_on_insert(
                            df.loc[i, cols[j]], cols[j], enabled=sql_injection_check_enabled)
                        statement += " '%s' " % (df.loc[i, cols[j]])
                statement += '; '

        return statement
