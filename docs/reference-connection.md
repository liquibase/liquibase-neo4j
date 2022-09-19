# Neo4j Connection URI

`liquibase-neo4j` accepts only URI in the JDBC format, supported
the [underlying JDBC connector](https://github.com/neo4j-contrib/neo4j-jdbc).

Only connections through the Bolt protocol variants are supported.
Connections through HTTP or embedded are not supported by the extension.

- ✅ `jdbc:neo4j:bolt://host:port` is supported
- ✅ `jdbc:neo4j:bolt+s://host:port` is supported
- ✅ `jdbc:neo4j:bolt+ssc://host:port` is supported
- ✅ `jdbc:neo4j:neo4j://host:port` is supported
- ✅ `jdbc:neo4j:neo4j+s://host:port` is supported
- ✅ `jdbc:neo4j:neo4j+ssc://host:port` is supported
- ❌ `jdbc:neo4j:http://host:port` is NOT supported
- ❌ `jdbc:neo4j:https://host:port` is NOT supported
- ❌ `jdbc:neo4j:file:///path/to/neo4j` is NOT supported

Starting with Neo4j 4 (Enterprise Edition), a Neo4j server may host several databases.
The `database` URI parameter can be added to target a specific database:

- `jdbc:neo4j:bolt://localhost?database=myDb`
- `jdbc:neo4j:neo4j+ssc://example.com?database=otherDb`
- `jdbc:neo4j:bolt+s://example.com?database=yetAnotherDb`

The general list of supported URI parameters is
documented [here](https://github.com/neo4j-contrib/neo4j-jdbc#list-of-supported-neo4j-configuration-parameters).
Most of the parameters should be left unspecified, as they can interact with the extension in unpredictable ways.

{!includes/_abbreviations.md!}
