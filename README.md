DBSource (renaming pending) is an abstraction for communication with SQL databases in Java. It also features the
possibility of defining query permutations for different database types and versions to bridge the gap where out dear
Standard Query Language is not quite as standard as we would like it.

Querying a database requires one to be specific about the types of each column in SQL query:

```java
Query countQuery = Queries.of("SELECT count(*) AS count FROM ...", new HashMap() {{
    put("count", Long.TYPE);
}});

// The long form
Database database = null;
QueryStatement statement = null;
long count;

try {
    database = connection.write();
    statement = database.createQuery(countQuery);
    cursor = statement.query(database);
    long count = cursor.get("count");
} finally {
    // Closing a database resource's parent closes all its child resources.
    connection.close(database);
}

// ... or using provided utility methods for that Java 8 swagger
Connections.write(connection, database -> {
    Statements.query(database, countQuery, cursor -> System.out.println(cursor.get("count")));
});
```
   
Updating the database occurs within transactions which are explicitly committed or rolled back:

```java
Query insertQuery = Queries.of("INSERT INTO t (a) VALUES (:a)", new HashMap() {{ put("a", String.class); }});
Database database = null;
UpdateStatement statement = null;
Transaction transaction = null;
long count;

try {
    database = connection.write();
    statement = database.createUpdate(insertQuery);
    long count = statement.update();
    /* ... */
    database.commit(transaction);
} catch (RuntimeException e) {
    database.rollback(transaction);
    /* ... */
} finally {
    connection.close(database);
}
```

By default, opening a database only requires a connection object. Where you would typically do

```java
try {
    Class.forName(driverClass);
} catch (ClassNotFoundException e) {
    /* ... */;
}

java.sql.Connection connection;

try {
    connection = DriverManager.getConnection(url, properties);
} catch (SQLException e) {
    /* ... */;
}
```

you can now do

```java
Connection connection = new JdbcConnection("org.sqlite.JDBC", "jdbc:sqlite:~/test.db");
Database database = connection.write();
```

However, since many JDBC implementations lack support for all getters and setters as well as full
`PreparedStatement#getGeneratedKeys()` support, opening a JDBC connection allows you to define how you want your
objects set and gotten and your generated keys provided.

```java
Connection connection = new JdbcConnection(
        "org.sqlite.JDBC",
        "jdbc:sqlite:~/test.db",
        new Properties(),
        new DefaultJdbcAccessorFactory(),
        new FunctionJdbcGeneratedKeysSelector("last_insert_rowid()", Long.TYPE));
```
