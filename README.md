DBSource (renaming pending) is an abstraction for communication with SQL databases in Java. It also features the
possibility of defining query permutations for different database types and versions to bridge the gap where out dear
Standard Query Language is not quite as standard as we would like it.

Querying a database requires one to be specific about the types of each column in SQL query:

```java
Query countQuery = Queries.of("SELECT count(*) AS count FROM ...", new HashMap() {{ put("count", Long.TYPE); }});

try (Database db = /*...*/;
     QueryStatement selectStatement = db.createQuery(countQuery);
     DatabaseCursor cursor = selectStatement.query()) {
    long count = cursor.get("count");
    ...
}
```
    
Updating the database always requires a transaction.  If a transaction is closed without committing it is rolled back.

```java
Query insertQuery = Queries.of("INSERT INTO t (a) VALUES (:a)", new HashMap() {{ put("a", String.class); }});

try (Database db = /*...*/;
     UpdateStatement updateStatement = db.createUpdate(insertQuery);
     Transaction transaction = db.transact()) {
    updateStatement.bind("a", "example");
    long updateCount = updateStatement.update(transaction);
    ...
    transaction.commit();
}
```

By default, opening a database only requires a connection object.

```java
Database db = JdbcDatabase.open(DriverManager.getConnection("jdbc:sqlite:~/test.db"));
```

However, since many JDBC implementations lack support for all getters and setters as well as full
`PreparedStatement#getGeneratedKeys()` support, opening a JDBC connection allows you to define how you want your
objects set and gotten and your generated keys provided.

```java
Database db = JdbcDatabase.open(DriverManager.getConnection("jdbc:sqlite:~/test.db"),
        new DefaultJdbcAccessorFactory(),
        new FunctionJdbcGeneratedKeysSelector("last_insert_id()"));
```
