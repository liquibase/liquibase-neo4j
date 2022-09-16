# Reference

## The Mental Model

### Changes

Liquibase manages the execution of database changes.
A change can be a simple statement written in a dialect understood by the target database.
This is for instance the case of the ["SQL" change](https://docs.liquibase.com/change-types/sql.html) (aliased to Cypher
by `liquibase-neo4j`).

A change can also be a complex transformation operation, such as
["Rename Table"](https://docs.liquibase.com/change-types/rename-table.html).

The interest of complex changes is two-fold:

 1. it encapsulates several basic operations, which would otherwise be tedious and error-prone to write
 2. most of these complex changes are portable across different database management systems 

!!! note
    In the case of `liquibase-neo4j`, portability is not a concern. `liquibase-neo4j` only supports Neo4j. There is 
    currently no plan to support other graph databases.
    This may change when the [upcoming GQL standard](https://www.gqlstandards.org/) matures and the ecosystem fully embraces it.

### Change Sets and Transactions

Changes are grouped into change sets.

Changes of a single change set are run in a single transaction by default. Either all the changes succeed or they all fail.

!!! important
    Neo4j [schema operations](https://neo4j.com/docs/getting-started/current/cypher-intro/schema/)
    and data operations cannot be part of the same transaction. They must be defined in different change sets.
 
### Change Logs and Consistency

Change sets are defined in change logs, usually stored as flat files on the local file system.

A change log may include other change logs and/or define change sets.

Change log files can be written in [different formats](https://docs.liquibase.com/concepts/changelogs/changelog-formats.html).
Formats can even be mixed (for instance: a top-level XML change log can include a SQL change log).

!!! note
    Change sets are uniquely identified by an ID and an author name, as well as the change log path that defines it.
    This prevents unintended collisions.

The Liquibase runtime requires a single change log entry point.
It will resolve all change log inclusions to a linear sequence of change sets.

Change sets may depend on each other. For instance, a previous change set may define a table and the next one
assumes the table exists and inserts seed data into it.

The kind of consistency model required to achieve this within a distributed system is called [causal consistency](https://en.wikipedia.org/wiki/Causal_consistency), 
sometimes coined as ["Read Your Own Write"](https://jepsen.io/consistency/models/read-your-writes).

In the case of Neo4j, a bookmark mechanism fulfills that property. A bookmark is passed along a transaction and identifies
a state that the targeted instance needs to reach before processing that transaction.

Based upon this idea, native Neo4j drivers define sessions.
Sessions are bound to a single thread of execution and automatically chains the 
bookmarks of a previous successful transaction to the next one.

The JDBC connector used by `liquibase-neo4j` relies on the native Java driver and ties a single JDBC connection to a single
Neo4j session. A single Liquibase invocation results in a single connection to be open.
As a consequence, the causal consistency property is automatically ensured.

### Persistence and History Graph

Liquibase is by default an append-only change executor.
Indeed, instead of altering existing change sets, the most common approach is to append new change sets to the existing 
sequence.

!!! tip
    Altering change sets is prohibited by default. Change sets are immutable.
    In order to make them mutable, the change set needs to set its attribute `runOnChange` to
    `true`.

Liquibase does not re-run change sets by default. Once they are executed against a target database,
they are not run again.

!!! tip
    In order to re-run change sets, their attribute `runAlways` needs to be set to `true`.

The executed change sets need to be persisted. 
`liquibase-neo4j` stores them as a history graph (the RDBMS equivalent is the [`DATABASECHANGELOG` table](https://docs.liquibase.com/concepts/tracking-tables/databasechangelog-table.html)).

![`liquibase-neo4j` schema](assets/images/liquibase-neo4j-schema.svg)

!!! note
    `liquibase-neo4j` stores this history graph in the same database (tenant) as the one the change log runs against.
    Executing a change log against one database and persisting the history in another is not supported.

!!! important
    **`liquibase-neo4j` makes no guarantee** that the history graph schema remains unchanged from version to version, even 
    during a non-major version upgrade.
    Users are not expected to manipulate the history graph directly.
    If the schema changes, `liquibase-neo4j` will automatically internal migrations against the history graph persisted
    by a former version.

### Concurrency

`liquibase-neo4j` must not be executed concurrently against the same server and database.
Similar to the [`DATABASECHANGELOGLOCK` table](https://docs.liquibase.com/concepts/tracking-tables/databasechangeloglock-table.html), 
`liquibase-neo4j` attempts to store a unique `__LiquibaseLock` node. If this fails, this means another execution is going on and the execution stops.

## Neo4j Connection URI

The URI that `liquibase-neo4j` accepts must follow the JDBC URL format supported by
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

## Supported Features

!!! info
    _Supported_ means that `liquibase-neo4j` has been verified to work with the features in question.
    Other features may also work but have not been reported as such.
    If a particular Liquibase feature is missing from the documentation, please [open an issue]({{ github_repo }}/issues/new)

### Change log format and inclusion

The extension supports [polyglot](https://docs.liquibase.com/concepts/basic/other-formats.html) change logs (XML, SQL,
YAML ...).
The SQL format has been aliased to the more idiomatic Cypher format.

Cypher file names must end with `.cypher`.
Cypher change logs must start with the comment `--liquibase formatted cypher`.
Here is an example of a supported Cypher file:

```cypher
--liquibase formatted cypher
--changeset fbiville:count-movies runAlways:true
MATCH (m:Movie) WITH COUNT(m) AS count MERGE (c:Count) SET c.value = count
--rollback MATCH (c:Count) DETACH DELETE c
```

Cypher files that are part of a [folder inclusion](#change-log-inclusion) must only contain a single Cypher queries
and **no** comment directive.

The extension supports change log [file](https://docs.liquibase.com/concepts/advanced/include.html)
and [folder](https://docs.liquibase.com/concepts/advanced/includeall.html) inclusion.

### Run on change, run always

Change set [runOnChange](https://docs.liquibase.com/concepts/advanced/runonchange.html), and runAlways attributes are
fully supported.

### Logical file path

The extension supports change set [logical file path](https://docs.liquibase.com/concepts/advanced/logicalfilepath.html).

### SQL and rollback changes

The built-in [SQL](https://docs.liquibase.com/change-types/community/sql.html)
and [rollback](https://docs.liquibase.com/workflows/liquibase-community/using-rollback.html) changes are supported.
The SQL change is also aliased to `cypher`.

!!! warning
    When using XML change logs, the `cypher` tag needs to be prepended with the corresponding extension namespace prefix.
    In the following example, the prefix is `neo4j`:

    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <databaseChangeLog xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
    xmlns:neo4j="http://www.liquibase.org/xml/ns/dbchangelog-ext"
    xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">

    <changeSet id="my-movie-init" author="fbiville">
        <neo4j:cypher>MERGE (:Movie {title: 'My Life'})</neo4j:cypher>
        <rollback>MATCH (m:Movie {title: 'My Life'}) DETACH DELETE m</rollback>
    </changeSet>
    ```

### Preconditions

The extension defines two Neo4j-specific preconditions:

- `version` which asserts the expected Neo4j version
- `edition` which asserts the expected Neo4j edition (Community or Enterprise)

It also supports some
built-in [preconditions](https://docs.liquibase.com/concepts/changelogs/preconditions.html#available-preconditions):

- `sqlCheck` (aliased to `cypherCheck`)
- `dbms` (restricting to `neo4j`)

!!! warning
    When using XML change logs, the `cypherCheck`, `version` and `edition` tags need to be prepended with the corresponding extension namespace prefix.
    In the following example, the prefix is `neo4j`:
    
    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <databaseChangeLog xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
    xmlns:neo4j="http://www.liquibase.org/xml/ns/dbchangelog-ext"
    xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">

    <changeSet id="some-change-set" author="some-author">
        <preConditions onFail="CONTINUE">
          <and>
            <neo4j:cypherCheck expectedResult="0">
                MATCH (n)
                WHERE NONE(label IN LABELS(n) WHERE label STARTS WITH '__Liquibase')
                RETURN COUNT(n)
            </neo4j:cypherCheck>
            <or>
              <neo4j:edition enterprise="true" />
              <neo4j:version matches="4.4" />
            </or>
          </and>
        </preConditions>
        <!-- [...] -->
    </changeSet>
    ```

All boolean operators are supported.

### Tagging

Both [imperative](https://docs.liquibase.com/commands/community/tag.html)
and [declarative](https://docs.liquibase.com/change-types/community/tag-database.html) tagging are supported.

### Filtering

Change set [context and label](https://www.liquibase.org/blog/contexts-vs-labels) filtering is fully supported.

### Graph refactorings

#### Node Merge

```xml

<neo4j:mergeNodes fragment="(m:Movie {title: 'My Life'}) WITH m ORDER BY id(m) ASC" outputVariable="m">
    <neo4j:propertyPolicy nameMatcher="name" mergeStrategy="KEEP_FIRST"/>
    <neo4j:propertyPolicy nameMatcher="par.*" mergeStrategy="KEEP_LAST"/>
    <neo4j:propertyPolicy nameMatcher=".*" mergeStrategy="KEEP_ALL"/>
</neo4j:mergeNodes>
```

Specify a Cypher query fragment, which defines the nodes to match for the merge operation. If fewer than two nodes are
matched, the merge is a no-op.

Specify the variable in that fragment that refer to the matched nodes. This is reused to create the internal merge
Cypher queries to execute.

Finally, make sure to define the merge policy for each persisted property.
The extension goes through every unique property name, selects the first matching merge policy, in declaration order.
If at least one property name does not match a policy, the merge fails and is canceled.
Once the policy is matched for the property name, one of the following operations happens:

- `KEEP_FIRST`: the first defined property value for that name is kept
- `KEEP_LAST`: the last defined property value for that name is kept
- `KEEP_ALL`: all defined property values are aggregated into an array (even if only a single value is found)

!!! note
    "first" and "last" are defined by the ordering of the specified Cypher query fragment. It is strongly advised to
    explicitly order the matched nodes with the `ORDER BY` clause like in the example.

{!includes/_abbreviations.md!}