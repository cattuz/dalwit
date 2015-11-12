DBSource (renaming pending) is an abstraction for communication with SQL databases in Java. It also features the
possibility of defining query permutations for different database types and versions to bridge the gap where out dear
Standard Query Language is not quite as standard as we would like it.

Querying a database requires one to be specific about the types of each column in SQL query:

```java
Query countQuery = Queries.of("SELECT count(*) AS count FROM ...",
        new HashMap() {{ put("count", Long.TYPE); }});

try (Database db = /*...*/;
     QueryStatement selectStatement = db.createQuery(countQuery);
     DatabaseCursor cursor = selectStatement.query()) {
    long count = cursor.get("count");
    ...
}
```
    
Updating the database always requires a transaction:

```java
Query insertQuery = Queries.of("INSERT INTO t (a) VALUES (:a)",
        new HashMap() {{ put("a", String.class); }});

try (TransactionDatabase db = /*...*/;
     UpdateStatement updateStatement = db.createUpdate(insertQuery);
     Transaction transaction = db.transact()) {
    updateStatement.bind("a", "example");
    long updateCount = updateStatement.update(transaction);
    ...
}
```
    
Since many JDBC implementations lack support for all getters and setters as well as full
`PreparedStatement#getGeneratedKeys()` support, opening a JDBC connection requires you to define how you want your
objects set and gotten and your generated keys provided.

```java
TransactionDatabase db = JdbcDatabase.openWritable(
        "jdbc:sqlite:~/test.db",
        new Properties(),
        JdbcDatabase.accessors,
        new GeneratedKeysFunctionSelector("last_insert_id()"));
```
