# PostgreSQLInterface 
## Introduction
This package allows loading and extracting data from a PostgresSQL Database using pandas dataframes and simple SQL style methods. 
At the moment, only has been implemented for Heroku and GCP databases. Nonetheless, Heroku testing has been deprecated due 

Extension to other vendors is easily achievable. 

## Structure
The library has a parent abstract class that contains the main methods, then a children class implements the 
particularities of a vendor, finally, a factory method handles the creation of the objects.  
Below, you can find a little introduction to all relevant methods. For more information, read the doc of the method
or take a look to the pytest tests in the repo.


# GCP
## Instantiate the class
To instantiate the class to connect to a Heroku Database:
```
from postgresql_interface.postgresql_interface import postgres_sql_connector_factory
db_conn = postgres_sql_connector_factory(
            vendor='gcp', host=host, database_name=database_name, 
            user_name=user_name, user_password=user_password, port=port
)
```

## Read data from Database
To retrieve a query from the database:
```
my_table = db_conn.query("SELECT * FROM test.data")
```
It is also possible to do more complicated queries:
```
my_table = db_conn.query("SELECT * FROM test.data d JOIN test.references r ON d.referencesid = r.id")
```

## Insert data
To insert data into a table:
```
db_conn.insert_table('test.simple', to_insert.copy())
```

## Update a table
To update data into a table:
```
db_conn.update_table('test.simple', to_update, ['id', 'date'])
```

## Delete from table
To delete from a table:
```
db_conn.delete_from_table('test.simple', to_delete)
```

## Execute a general statement
To execute a general SQL statement you can use the method *execute*. This method returns no data. 
It is important to notice that it is not the SQL execute command. As an example, you can use it to create a schema:
```
db_conn.execute("CREATE SCHEMA test")
```
You can also use it to execute a stored procedure
```
db_conn.execute("EXECUTE test.sp_test1 @input = '%s" @ input)
```


# Heroku
## Instantiate the class
To instantiate the class to connect to a Heroku Database:
```
from postgresql_interface.postgresql_interface import postgres_sql_connector_factory
db_conn = postgres_sql_connector_factory(vendor='heroku', database_url=database_url)
```
*database_url* can be found on the section *Config Vars* inside the tab *Settings* of your Heroku app or on the 
section *Database Credentials* of the tab *Settings* of your Heroku Datastore.


## Read data from Database
To retrieve a query from the database:
```
my_table = db_conn.query("SELECT * FROM test.data")
```
It is also possible to do more complicated queries:
```
my_table = db_conn.query("SELECT * FROM test.data d JOIN test.references r ON d.referencesid = r.id")
```

## Insert data
To insert data into a table:
```
db_conn.insert_table('test.simple', to_insert.copy())
```

## Update a table
To update data into a table:
```
db_conn.update_table('test.simple', to_update, ['id', 'date'])
```

## Delete from table
To delete from a table:
```
db_conn.delete_from_table('test.simple', to_delete)
```

## Execute a general statement
To execute a general SQL statement you can use the method *execute*. This method returns no data. 
It is important to notice that it is not the SQL execute command. As an example, you can use it to create a schema:
```
db_conn.execute("CREATE SCHEMA test")
```
You can also use it to execute a stored procedure
```
db_conn.execute("EXECUTE test.sp_test1 @input = '%s" @ input)
```

## Tests
To be able to execute the tests, it is necessary to provide a '.env' file with the url to connect to a GCP database.
Currently, Heroku testing is disabled due to change in pricing.
