# Custom Features

## Cypher change log format

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

Cypher files that are part of a [folder inclusion](#change-log-inclusion) must only contain a single Cypher query
and **no** comment directive.

The extension supports change log [file](https://docs.liquibase.com/concepts/advanced/include.html)
and [folder](https://docs.liquibase.com/concepts/advanced/includeall.html) inclusion.

### Cypher and rollback changes

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

## Neo4j preconditions

The extension defines three Neo4j-specific preconditions:

- `version` which asserts the expected Neo4j version
- `edition` which asserts the expected Neo4j edition (Community or Enterprise)
- `cypherCheck` which aliases the existing [`sqlCheck`](https://docs.liquibase.com/concepts/changelogs/preconditions.html#available-preconditions) precondition

It also supports some
built-in [preconditions](https://docs.liquibase.com/concepts/changelogs/preconditions.html#available-preconditions):

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

## Graph refactorings

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
