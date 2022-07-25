from postgresql_interface.custom_errors import ErrorPossibleSQLInjectionDetected


class SQLInjectionBodyguard:
    """
    Trait that contains methods to evaluate possible SQL Injections
    """
    @staticmethod
    def check_string_on_insert(string, column, enabled=True):
        """
        Methods that looks in string for a possible SQL Injection. In order to do that, looks for in the string for
        a "'". If it is found, then looks after it for a ")" and a ";". If all of that is found then
        raises ErrorPossibleSQLInjectionDetected. If enabled is False, the check is not executed
        - Args:
            string: this is the value to check.
            column: column name of the value check. To indicate to user the problematic data.
            enabled: if the check has to be executed, True. Otherwise, False

        - Raises:
            ErrorPossibleSQLInjectionDetected: if a possible SQL injection is detected
        """
        if enabled:
            if type(string) == str:
                my_string = string.upper().replace(" ", "")
                if "'" in my_string:
                    if (")" in my_string[my_string.find("'"):]) & (";" in my_string[my_string.find("'"):]):
                        raise ErrorPossibleSQLInjectionDetected(
                            "ERROR: A possible intent of SQL Injection has been found on field: '%s'. Insert operation "
                            "interrupted. problematic value: '%s'" % (column, string))
