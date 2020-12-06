# PostgreSQLInterface 
## Introduction
This package allows to load and extract data from a PosgreSQL Database using simple SQL style methods. At the moment,
it has only been tested on a Heroku Postgres Database, though it should not be hard to extended the *init* method 
to be able to interact with other cloud or premises providers that uses PosgresSQL technology (maybe the method 
*create_connection* will also need to be modified). It can be also use as a reference to create new interfaces to connect
to other RDBS the same way this one is inspired on the one to connect to SQLServer located on the library berryworld.   

## Structure
The library has an unique class that handles the relations to a database allowing to load and extract data.

## Instantiate the class
To instantiate the class to connect to a Heroku Database:
```
heroku_db = PosgreSQL(database_url)
```
*database_url* can be found on the section *Config Vars* inside the tab *Settings* of your Heroku app or on the 
section *Database Credentials* of the tab *Settings* of your Heroku Datastore.

## Read data from Database
To retrieve a query from the database:
```
my_table = heroku._db.query("SELECT * FROM test.data")
```
It is also possible to do more complicated queries:
```
my_table = heroku._db.query("SELECT * FROM test.data d JOIN test.references r ON d.referencesid = r.id")
```

## Insert data
To insert data into a table
```
heroku_db.insert_table('test.simple', to_insert.copy())
```

## Update a table
To insert data into a table
```
heroku_db.update_table('test.simple', to_update, ['id', 'date'])
```


## Execute a general statement
To execute a general SQL statement you can use the method *execute*. This method returns no data. 
It is important to notice that it is not the SQL execute command. As an example, you can use it to create a schema:
```
heroku._db.execute("CREATE SCHEMA test")
```
You can also use it to execute a stored procedure
```
heroku._db.execute("EXECUTE test.sp_test1 @input = '%s" @ input)
```

## Tests
To be able to execute the tests, it is necessary to provide a '.env' file with the url to connect to a Heroku database.
