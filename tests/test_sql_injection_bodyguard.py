from postgresql_interface.sql_injection_bodyguard import SQLInjectionBodyguard
from postgresql_interface.custom_errors import ErrorPossibleSQLInjectionDetected
import pytest
import datetime as dt


@pytest.mark.parametrize(
    "value, col",
    [
        ("ASDF' ); DROP TABLE ; select to_char('asdasd", "dummy_value"),
        ("',''); DROP DATABASE; ", "my_col")
    ],
)
def test_raise_error(value, col):
    """
    GIVEN a value to be inserted into a sql statement that tries to do an SQL Injection
    WHEN it is passed through SQLInjectionBodyguard.check_string_on_insert()
    THEN the SQL Injection should be detected
    """
    with pytest.raises(ErrorPossibleSQLInjectionDetected) as e:
        assert SQLInjectionBodyguard.check_string_on_insert(value, col)
    assert str(e.value) == ("ERROR: A possible intent of SQL Injection has been found on field: '%s'. "
                            "Insert operation interrupted. problematic value: '%s'" % (col, value))


@pytest.mark.parametrize(
    "value, col",
    [
        ("asde,);'", "dummy_value"),
        (100, "id"),
        ("100", "id"),
        (dt.date.today(), "date")
    ],
)
def test_no_error_raise(value, col):
    """
    GIVEN a value to be inserted into a sql statement that tries to do an SQL Injection
    WHEN it is passed through SQLInjectionBodyguard.check_string_on_insert()
    THEN the SQL Injection should be detected
    """
    SQLInjectionBodyguard.check_string_on_insert(value, col)
    assert True