# Features

## Cypher Change Log Format

|Required plugin version|4.9.0.1|

The extension supports [polyglot](https://docs.liquibase.com/concepts/basic/other-formats.html) change logs (XML, SQL,
YAML ...).
The SQL format has been aliased to the more idiomatic Cypher format.

Cypher file names must end with `.cypher`.
Cypher change logs must start with the comment `--liquibase formatted cypher`.
Here is an example of a supported Cypher file:

```cypher
--liquibase formatted cypher
--changeset fbiville:count-movies runAlways:true
MATCH (m:Movie) WITH count(m) AS count MERGE (c:Count) SET c.value = count
--rollback MATCH (c:Count) DETACH DELETE c
```

Cypher files that are part of a [folder inclusion](#change-log-inclusion) must only contain a single Cypher query
and **no** comment directive.

The extension supports change log [file](https://docs.liquibase.com/concepts/advanced/include.html)
and [folder](https://docs.liquibase.com/concepts/advanced/includeall.html) inclusion.

## Cypher and Rollback Changes

|Required plugin version (Cypher alias) |4.7.1.1|

The built-in [SQL](https://docs.liquibase.com/change-types/community/sql.html)
and [rollback](https://docs.liquibase.com/workflows/liquibase-community/using-rollback.html) changes are supported.
The SQL change is also aliased to `cypher`.

=== "XML"

    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <databaseChangeLog xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
        xmlns:neo4j="http://www.liquibase.org/xml/ns/dbchangelog-ext"
        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">

        <changeSet id="my-movie-init" author="fbiville">
            <neo4j:cypher>CREATE (:Movie {title: 'My Life'})</neo4j:cypher>
            <rollback>MATCH (m:Movie {title: 'My Life'}) DETACH DELETE m</rollback>
        </changeSet>
    </databaseChangeLog>
    ```

    !!! warning
         - The `cypher` XML tag needs to be prepended with the corresponding extension namespace prefix.
         - If the query contains XML special characters such as `<` or `>`, make sure to surround the query content with 
        `<![CDATA[` at the beginning and `]]>` at the end.

=== "JSON"

    ```json
    {"databaseChangeLog": [
        {"changeSet": {
            "id": "my-movie-init",
            "author": "fbiville",
            "changes": [
                {
                    "cypher": "CREATE (:Movie {title: 'My Life'})"
                }
            ],
            "rollback": [
                {
                    "cypher": "MATCH (m:Movie {title: 'My Life'}) DETACH DELETE m"
                }
            ]
        }}
    ]}
    ```

=== "YAML"

    ```yaml
    databaseChangeLog:
    - changeSet:
      id: my-movie-init
      author: fbiville
      changes:
        - cypher: 'CREATE (:Movie {title: ''My Life''})'
      rollback:
        - cypher: "MATCH (m:Movie {title: 'My Life'}) DETACH DELETE m"
    ```

=== "Cypher"

    ```cypher
    -- liquibase formatted cypher

    -- changeset fbiville:my-movie-init
    CREATE (:Movie {title: 'My Life'})
    -- rollback MATCH (m:Movie {title: 'My Life'}) DETACH DELETE m
    ```

## Neo4j Preconditions

Learn more about preconditions in the [Liquibase documentation](https://docs.liquibase.com/concepts/changelogs/preconditions.html), 
especially the [failure and error handling section](https://docs.liquibase.com/concepts/changelogs/preconditions.html#handling-failures-and-errors).

### Built-in precondition support

The extension has been successfully tested with the following built-in [precondition](https://docs.liquibase.com/concepts/changelogs/preconditions.html#available-preconditions) since the very first release of the plugin:

 - `dbms` (targeting `Neo4j`)
 - `sqlCheck` (aliased as `cypherCheck`, see below)

Others may work but have not been tested.

### Version check

|Required plugin version|4.9.0|

The `version` precondition asserts the runtime Neo4j version **starts with** the specified string.
It can be combined with other preconditions with the standard boolean operators.

=== "XML"

    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <databaseChangeLog xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
        xmlns:neo4j="http://www.liquibase.org/xml/ns/dbchangelog-ext"
        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">

        <changeSet id="my-neo4j-lts-deployment" author="fbiville">
            <preConditions onFail="CONTINUE">
                <neo4j:version matches="4.4"/>
            </preConditions>
            <neo4j:cypher>CREATE (:Neo4j {lts: true})</neo4j:cypher>
        </changeSet>
    </databaseChangeLog>
    ```

=== "JSON"

    ```json
    {"databaseChangeLog": [
        {
          "changeSet": {
            "id": "my-neo4j-lts-deployment",
            "author": "fbiville",
            "preConditions": [
              {
                "onFail": "CONTINUE"
              },
              {
                "version": {
                  "matches": "4.4"
                }
              }
            ],
            "changes": [
              {
                "cypher": "CREATE (:Neo4j {lts: true})"
              }
            ]
          }
        }
    ]}
    ```

=== "YAML"

    ```yaml
    databaseChangeLog:
        - changeSet:
          id: my-neo4j-44-deployment
          author: fbiville
          preConditions:
          - onFail: 'CONTINUE'
          - version:
              matches: '4.4'
          changes:
          - cypher: 'CREATE (:Neo4j {lts: true})'
    ```

### Edition check

|Required plugin version|4.9.0|

The `edition` check asserts whether the target Neo4j deployment is the Community Edition or Enterprise Edition.
It can be combined with other preconditions with the standard boolean operators.

=== "XML"

    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <databaseChangeLog xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
        xmlns:neo4j="http://www.liquibase.org/xml/ns/dbchangelog-ext"
        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">

        <changeSet id="my-neo4j-ee-deployment" author="fbiville">
            <preConditions onFail="CONTINUE">
                <neo4j:edition enterprise="true"/>
            </preConditions>
            <neo4j:cypher>CREATE (:Neo4j {enterprise: true})</neo4j:cypher>
        </changeSet>
    </databaseChangeLog>
    ```

=== "JSON"

    ```json
    {"databaseChangeLog": [
        {
          "changeSet": {
            "id": "my-neo4j-ee-deployment",
            "author": "fbiville",
            "preConditions": [
              {
                "onFail": "CONTINUE"
              },
              {
                "edition": {
                  "enterprise": true
                }
              }
            ],
            "changes": [
              {
                "cypher": "CREATE (:Neo4j {enterprise: true})"
              }
            ]
          }
        }
    ]}
    ```

=== "YAML"

    ```yaml
    databaseChangeLog:
        - changeSet:
          id: my-neo4j-ee-deployment
          author: fbiville
          preConditions:
          - edition:
          enterprise: true
          - onFail: 'CONTINUE'
          changes:
          - cypher: 'CREATE (:Neo4j {enterprise: true})'
    ```

### Cypher check alias

|Required plugin version|4.9.0|

The `cypherCheck` aliases the existing [`sqlCheck`](https://docs.liquibase.com/concepts/changelogs/preconditions.html#available-preconditions) precondition.
Cypher formatted change log files can only use `sqlCheck` at the moment.
`cypherCheck` can be combined with other preconditions with the standard boolean operators.

!!! warning
    Before version 4.21.1.2, JSON and YAML change logs had to specify a `sql` attribute instead of `cypher`.

=== "XML"

    ```xml
    <?xml version="1.0" encoding="UTF-8"?>
    <databaseChangeLog xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
        xmlns:neo4j="http://www.liquibase.org/xml/ns/dbchangelog-ext"
        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog https://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">
    
        <changeSet id="my-neo4j-deployment" author="fbiville">
            <preConditions onFail="CONTINUE">
                <neo4j:cypherCheck expectedResult="0">MATCH (n:Neo4j) RETURN count(n)</neo4j:cypherCheck>
            </preConditions>
            <neo4j:cypher>CREATE (:Neo4j)</neo4j:cypher>
        </changeSet>
    
    </databaseChangeLog>
    ```

=== "JSON"

    ```json
    {"databaseChangeLog": [
        {
            "changeSet": {
                "id": "my-neo4j-deployment",
                "author": "fbiville",
                "preConditions": [
                    {"onFail": "CONTINUE"},
                    {"cypherCheck": {
                        "expectedResult": "0",
                        "cypher": "MATCH (n:Neo4j) RETURN count(n)"
                    }}
                ],
                "changes": [
                    {"cypher": {
                        "cypher": "CREATE (:Neo4j)"
                    }}
                ]
            }
        }
    ]}
    ```

=== "YAML"

    ```yaml
    databaseChangeLog:
    - changeSet:
          id: my-neo4j-deployment
          author: fbiville
          preConditions:
          - onFail: 'CONTINUE'
          - cypherCheck:
              expectedResult: '0'
              cypher: MATCH (n:Neo4j) RETURN count(n)
          changes:
          - cypher:
                cypher: CREATE (:Neo4j)
    ```

## Insert Change

|Required plugin version|4.22.0|

The change allows to define the creation of individual nodes, with a single label specified with `labelName`.

=== "XML"

    ```xml
    <neo4j:insert labelName="Person">
        <column name="id" value="8987212b-a6ff-48a1-901f-8c4b39bd6d9e" type="uuid"/>
        <column name="first_name" value="Florent"/>
        <column name="last_name" value="Biville"/>
        <column name="birth_date" valueDate="1950-01-01T22:23:24+02:00" type="date"/>
        <column name="picture" value="DLxmEfVUC9CAmjiNyVphWw==" type="blob"/>
    </neo4j:insert>
    ```

=== "JSON"

    ```json
    {"insert": {
        "labelName": "Person",
        "columns": [
            {"column": {
                "name": "id",
                "type": "uuid",
                "value": "8987212b-a6ff-48a1-901f-8c4b39bd6d9e"
            }},
            {"column": {
                "name": "first_name",
                "value": "Florent"
            }},
            {"column": {
                "name": "last_name",
                "value": "Biville"
            }},
            {"column": {
                "name": "birth_date",
                "type": "date",
                "valueDate": "1950-01-01T22:23:24+02:00"
            }},
            {"column": {
                "name": "picture",
                "type": "blob",
                "value": "DLxmEfVUC9CAmjiNyVphWw=="
            }}
        ]
    }}
    ```

=== "YAML"

    ```yaml
    - insert:
        labelName: Person
        columns:
        - column:
            name: id
            type: uuid
            value: 8987212b-a6ff-48a1-901f-8c4b39bd6d9e
        - column:
            name: first_name
            value: Florent
        - column:
            name: last_name
            value: Biville
        - column:
            name: birth_date
            type: date
            valueDate: 1950-01-01T22:23:24+02:00
        - column:
            name: picture
            type: blob
            value: DLxmEfVUC9CAmjiNyVphWw==
    ```

Please refer to the [Load Data](#load-data) documentation for the supported value types for each column.

## Load Data

|Required Liquibase core version|4.11.0|
|Required plugin version|4.16.1.1|

The general documentation of this change is available [here](https://docs.liquibase.com/change-types/load-data.html).

The table below details how each supported data type is mapped to its Neo4j counterpart:

| Load Data Type | Liquibase Java Type                          | Example Value                               | Resulting Neo4j Java Type |
|----------------|----------------------------------------------|---------------------------------------------|---------------------------|
| `BLOB`         | `String`                                     | `DLxmEfVUC9CAmjiNyVphWw==` (base64-encoded) | `byte[]`                  |
| `BOOLEAN`      | `Boolean`                                    | `true` or `false`                           | `Boolean`                 |
| `CLOB`         | `String`                                     |                                             | `String`                  |
| `DATE`         | `java.sql.Timestamp`                         | `2018-02-01T12:13:14`                       | `java.time.LocalDateTime` |
| `DATE`         | `java.sql.Date`                              | `2018-02-01`                                | `java.time.LocalDate`     |
| `DATE`         | `java.sql.Time`                              | `12:13:14`                                  | `java.time.LocalTime`     |
| `DATE`         | `liquibase.statement.DatabaseFunction`       | `2018-02-01T12:13:14+02:00`                 | `java.time.ZonedDateTime` |
| `NUMERIC`      | `liquibase.change.ColumnConfig.ValueNumeric` | `42` or `42.0`                              | `Long` or `Double`        |
| `STRING`       | `String`                                     | `"a string"`                                | `String`                  |
| `UUID`         | `String`                                     | `1bc59ddb-8d4d-41d0-9c9a-34e837de5678`      | `String`                  |

`SKIP` is also supported: the value will be ignored.

!!!warning
    `BLOB` files (see [issue](https://github.com/liquibase/liquibase-neo4j/issues/304)), `CLOB` files (see [issue](https://github.com/liquibase/liquibase-neo4j/issues/304)), `SEQUENCE`, `COMPUTED`, `OTHER` and `UNKNOWN`
    load data types are currently unsupported.

Make sure to use the right `valueXxx` attribute:

- `valueBoolean` for boolean values
- `valueDate` for date/time values
- `valueNumeric` for numeric values
- `value` for everything else

## Graph refactorings

### Node Merge

|Required plugin version|4.13.0|

=== "XML"

    ```xml
    <neo4j:mergeNodes fragment="(m:Movie {title: 'My Life'}) WITH m ORDER BY id(m) ASC" outputVariable="m">
        <neo4j:propertyPolicy nameMatcher="name" mergeStrategy="KEEP_FIRST"/>
        <neo4j:propertyPolicy nameMatcher="par.*" mergeStrategy="KEEP_LAST"/>
        <neo4j:propertyPolicy nameMatcher=".*" mergeStrategy="KEEP_ALL"/>
    </neo4j:mergeNodes>
    ```

=== "JSON"

    ```json
    {
        "mergeNodes": {
            "fragment": "(m:Movie {title: 'My Life'}) WITH m ORDER BY id(m) ASC",
            "outputVariable": "m",
            "propertyPolicies": [
                {
                    "propertyPolicy": {
                        "mergeStrategy": "KEEP_FIRST",
                        "nameMatcher": "name"
                    }
                },
                {
                    "propertyPolicy": {
                        "mergeStrategy": "KEEP_LAST",
                        "nameMatcher": "par.*"
                    }
                },
                {
                    "propertyPolicy": {
                        "mergeStrategy": "KEEP_ALL",
                        "nameMatcher": ".*"
                    }
                }
            ]
        }
    }
    ```

=== "YAML"

    ```yaml
    - mergeNodes:
        fragment: '(m:Movie {title: ''My Life''}) WITH m ORDER BY id(m) ASC'
        outputVariable: m
        propertyPolicies:
        - propertyPolicy:
            mergeStrategy: 'KEEP_FIRST'
            nameMatcher: name
        - propertyPolicy:
            mergeStrategy: 'KEEP_LAST'
            nameMatcher: par.*
        - propertyPolicy:
            mergeStrategy: 'KEEP_ALL'
            nameMatcher: .*
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

### Node Property Extraction

|Required plugin version|4.17.2|

=== "XML"

    === "without relationships"
        ```xml
        <neo4j:extractProperty property="genre"
                               fromNodes="(m:Movie) WITH m ORDER BY id(m) ASC"
                               nodesNamed="m">
            <neo4j:toNodes withLabel="Genre" withProperty="genre" merge="true" />
        </neo4j:extractProperty>
        ```

    === "with relationships"

        ```xml
        <neo4j:extractProperty property="genre" 
                               fromNodes="(m:Movie) WITH m ORDER BY id(m) ASC"
                               nodesNamed="m">
            <neo4j:toNodes withLabel="Genre" withProperty="genre" merge="true">
                <neo4j:linkedFromSource withType="HAS_GENRE" 
                                        withDirection="OUTGOING"
                                        merge="true" />
            </neo4j:toNodes>
        </neo4j:extractProperty>
        ```

=== "JSON"

    === "without relationships"

        ```json
        {
            "extractProperty": {
                "fromNodes": "(m:Movie) WITH m ORDER BY id(m) ASC",
                "nodesNamed": "m",
                "property": "genre",
                "toNodes": {
                    "withLabel": "Genre",
                    "withProperty": "genre",
                    "merge": true
                }
            }
        }
        ```
    
    === "with relationships"

        ```json
        {
            "extractProperty": {
                "fromNodes": "(m:Movie) WITH m ORDER BY id(m) ASC",
                "nodesNamed": "m",
                "property": "genre",
                "toNodes": {
                    "withLabel": "Genre",
                    "withProperty": "genre",
                    "merge": true,
                    "linkedFromSource": {
                        "withDirection": "OUTGOING",
                        "withType": "HAS_GENRE",
                        "merge": true
                    }
                }
            }
        }
        ```

=== "YAML"

    === "without relationships"

        ```yaml
        - extractProperty:
            fromNodes: '(m:Movie) WITH m ORDER BY id(m) ASC'
            nodesNamed: 'm'
            property: 'genre'
            toNodes:
              withLabel: 'Genre'
              withProperty: 'genre'
              merge: true
        ```

    === "with relationships"

        ```yaml
        - extractProperty:
            fromNodes: '(m:Movie) WITH m ORDER BY id(m) ASC'
            nodesNamed: 'm'
            property: 'genre'
            toNodes:
              withLabel: 'Genre'
              withProperty: 'genre'
              merge: true
              linkedFromSource:
                withDirection: 'OUTGOING'
                withType: 'HAS_GENRE'
                merge: true
        ```

The node property extraction refactoring allows to extract node properties into their own nodes.
As for the [node merge refactoring](#node-merge), the nodes to extract properties from are specified as a Cypher
fragment
(`fromNodes` attribute) and the variable name (`nodesNamed` attribute) bound to these nodes.
The property name to extract is specified with the `property` attribute.

The source nodes matched by the Cypher fragment will have their property removed.
That property will be set on the extracted nodes, with the name described by the `withProperty` attribute.
The extracted nodes' label is defined with the `withLabel` attribute.
Set the `merge` property to `true` to avoid duplicates with potentially existing nodes with the same label and property.
The default behavior is to create extracted nodes every time.

Optionally, the extracted nodes can be linked with the source nodes.
In that case, a type and a direction need to be specified with respectively the `withType` and `withDirection`
attributes.

!!! note
    The relation direction is from the perspective of the source nodes.
    In the example, `OUTGOING` means the relationship starts from the source node and goes out to the extracted node.
    Conversely, `INCOMING` would mean the relationship comes in the source node from the extracted node.

It is also possible to avoid relationship duplicates by setting the corresponding `merge`
attribute to `true`. The default is to always create relationships.

!!! warning
    `merge=false` on nodes with `merge=true` on relationships will trigger a validation warning.
    Indeed, creating extracting nodes imply that new relationships will be created as well.
    Setting `merge=true` on relationships in that case incur an unnecessary execution penalty.

## Change Set's `runInTransaction`

|Required plugin version|4.19.0|

The default value of `runInTransaction` is `true`. This means that all changes of a given change set run in a single,
explicit transaction.

This is the right default and should be changed only if you need any of the following two Cypher constructs:

- [since Neo4j 4.4]  [`CALL {} IN TRANSACTIONS`](https://neo4j.com/docs/cypher-manual/current/clauses/call-subquery/#subquery-call-in-transactions)
- [until Neo4j 4.4]  [`PERIODIC COMMIT`](https://neo4j.com/docs/cypher-manual/4.4/query-tuning/using/#query-using-periodic-commit-hint)

Indeed, using those constructs without disabling `runInTransaction` fails:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
                   xmlns:neo4j="http://www.liquibase.org/xml/ns/dbchangelog-ext"
                   xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">

  <changeSet id="my-movie-init" author="fbiville">
    <neo4j:cypher>CALL { CREATE (:Movie {title: "Me, Myself and I"}) } IN TRANSACTIONS</neo4j:cypher>
  </changeSet>
</databaseChangeLog>
```

The error message after executing the change set is similar to:

```
A query with 'CALL { ... } IN TRANSACTIONS' can only be executed in an implicit transaction, but tried to execute in an explicit transaction.
```

Setting `runInTransaction` to `false` on a change set means that all its changes are going to run **in their own
auto-commit (or implicit) transaction**.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
                   xmlns:neo4j="http://www.liquibase.org/xml/ns/dbchangelog-ext"
                   xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">

  <changeSet id="my-movie-init" author="fbiville" runInTransaction="false">
    <neo4j:cypher>CALL { CREATE (:Movie {title: "Me, Myself and I"}) } IN TRANSACTIONS</neo4j:cypher>
  </changeSet>
</databaseChangeLog>
```

`runInTransaction` is a sharp tool and can lead to unintended consequences, as illustrated below:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
                   xmlns:neo4j="http://www.liquibase.org/xml/ns/dbchangelog-ext"
                   xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">

  <changeSet id="my-movie-init" author="fbiville" runInTransaction="false">
    <neo4j:cypher>CREATE (:Movie {title: "Me, Myself and I"})</neo4j:cypher> <!-- this is going to run -->
    <neo4j:cypher>this is not valid cypher</neo4j:cypher><!-- this is going to fail -->
  </changeSet>
</databaseChangeLog>
```

The first change of the change set successfully runs, but the second fails.
More importantly, the enclosing change set `"my-movie-init"` is **not** stored in the history graph.

Re-running this change set results in the `Movie` node being inserted again, since Liquibase has no knowledge of the
change set having run before.

In situations where `runInTransactions="false"` cannot be avoided, make sure the affected change set's queries are
idempotent ([constraints](https://neo4j.com/docs/cypher-manual/current/constraints/) must be defined in a prior change
set and using Cypher's `MERGE` instead of `CREATE` usually helps).
