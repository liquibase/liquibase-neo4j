# Features

## Cypher Change Log Format

|Required plugin version|4.9.0.1|

The extension supports [polyglot](https://docs.liquibase.com/concepts/basic/other-formats.html) change logs (XML, SQL,
YAML ...).
The SQL format has been aliased to the more idiomatic Cypher format.

Cypher file names must end with `.cypher`.
Cypher change logs must start with the comment `--liquibase formatted cypher`.
Here is an example of a supported Cypher file:

~~~~cypher
{! include '../src/test/resources/e2e/cypher-alias/changeLog.cypher' !}
~~~~

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

    ~~~~xml
    {! include '../src/test/resources/e2e/rollback/changeLog.xml' !}
    ~~~~

    !!! warning
         - The `cypher` XML tag needs to be prepended with the corresponding extension namespace prefix.
         - If the query contains XML special characters such as `<` or `>`, make sure to surround the query content with 
        `<![CDATA[` at the beginning and `]]>` at the end.

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/rollback/changeLog.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/rollback/changeLog.yaml' !}
    ~~~~

=== "Cypher"

    ~~~~cypher
    {! include '../src/test/resources/e2e/rollback/changeLog.cypher' !}
    ~~~~

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

    ~~~~xml
    {! include '../src/test/resources/e2e/preconditions/neo4jVersionChangeLog.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/preconditions/neo4jVersionChangeLog.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/preconditions/neo4jVersionChangeLog.yaml' !}
    ~~~~

### Edition check

|Required plugin version|4.9.0|

The `edition` check asserts whether the target Neo4j deployment is the Community Edition or Enterprise Edition.
It can be combined with other preconditions with the standard boolean operators.

=== "XML"

    ~~~~xml
    {! include '../src/test/resources/e2e/preconditions/neo4jEditionChangeLog.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/preconditions/neo4jEditionChangeLog.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/preconditions/neo4jEditionChangeLog.yaml' !}
    ~~~~

### Cypher check alias

|Required plugin version|4.9.0|

The `cypherCheck` aliases the existing [`sqlCheck`](https://docs.liquibase.com/concepts/changelogs/preconditions.html#available-preconditions) precondition.
Cypher formatted change log files can only use `sqlCheck` at the moment.
`cypherCheck` can be combined with other preconditions with the standard boolean operators.

!!! warning
    Before version 4.21.1.2, JSON and YAML change logs had to specify a `sql` attribute instead of `cypher`.

=== "XML"

    ~~~~xml
    {! include '../src/test/resources/e2e/preconditions/cypherCheckChangeLog.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/preconditions/cypherCheckChangeLog.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/preconditions/cypherCheckChangeLog.yaml' !}
    ~~~~

## Insert Change

|Required plugin version|4.21.1|

The change allows to define the creation of individual nodes, with a single label specified with `labelName`.

=== "XML"

    ~~~~xml
    {! include '../src/test/resources/e2e/insert/changeLog.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/insert/changeLog.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/insert/changeLog.yaml' !}
    ~~~~

Please refer to the [Load Data](#load-data) documentation for the supported value types for each column.

## Load Data

|Required Liquibase core version|4.11.0|
|Required plugin version|4.16.1.1|


Assuming the following (S)CSV `data.scsv` file:

~~~~csv
{! include '../src/test/resources/e2e/load-data/data.scsv' !}
~~~~

=== "XML"

    ~~~~xml
    {! include '../src/test/resources/e2e/load-data/changeLog.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/load-data/changeLog.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/load-data/changeLog.yaml' !}
    ~~~~

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

    ~~~~xml
    {! include '../src/test/resources/e2e/merge-nodes/changeLog.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/merge-nodes/changeLog.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/merge-nodes/changeLog.yaml' !}
    ~~~~

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

### Node Property Extraction

|Required plugin version|4.17.2|

=== "XML"
    ~~~~xml
    {! include '../src/test/resources/e2e/extract-property/changeLog.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/extract-property/changeLog.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/extract-property/changeLog.yaml' !}
    ~~~~

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
    Indeed, creating extracted nodes imply that new relationships will be created as well.
    Setting `merge=true` on relationships in that case incur an unnecessary execution penalty.

### Label Rename

|Required plugin version|4.24.1|

The label rename refactoring allows to rename one label to another, matching all or some of its nodes, in a single
transaction or in batches.

As illustrated below, the main attributes of the refactoring are:

- `from`: value of the existing label
- `to`: value of the new label, replacing the existing one


#### Global Rename

=== "XML"
    ~~~~xml
    {! include '../src/test/resources/e2e/rename-label/changeLog-simple.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/rename-label/changeLog-simple.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/rename-label/changeLog-simple.yaml' !}
    ~~~~

Since this operation can potentially affect a lot of nodes, running the change in a single transaction may be
infeasible since the transaction would likely run either too slow, or even run out of memory.

To prevent this, the enclosing change set's `runInTransaction` can be set to `false`.
This results in the rename being executed in batches.

!!! warning
    This setting only works if the target Neo4j instance supports `CALL {} IN TRANSACTIONS` (version 4.4 and later).
    If not, the Neo4j plugin will run the label rename in a single, autocommit transaction.
    
    Please also remember that Neo4j isolation level is "read-committed". As such, some of the nested transactions
    may actually affect more or fewer elements if concurrent transactions overlap.
    
    Finally, make sure to read about [the consequences of changing `runInTransaction`](#change-sets-runintransaction).

=== "XML"
    ~~~~xml
    {! include '../src/test/resources/e2e/rename-label/changeLog-simple-batched.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/rename-label/changeLog-simple-batched.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/rename-label/changeLog-simple-batched.yaml' !}
    ~~~~

As shown above, the `batchSize` attribute can be set in order to control how many transactions are going to be executed.
If the attribute is not set, the batch size will depend on the Neo4j server's default value.

#### Partial Rename

The following attributes can also be set, in order to match only a subset of the nodes with the label specified in `from`:

 - `fragment` specifies the pattern to match the nodes against
 - `outputVariable` specifies the Cypher variable name defined in `fragment` that denotes the targeted nodes

!!!note
    The nodes that are going to be rename sit at the intersection of what is defined in `fragment` and the nodes with
    label specified by `from`.
    In other words, if none of the nodes defined in `fragment` carry the label defined in `from`, the rename
    is not going to modify any of those.

=== "XML"
    ~~~~xml
    {! include '../src/test/resources/e2e/rename-label/changeLog-pattern.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/rename-label/changeLog-pattern.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/rename-label/changeLog-pattern.yaml' !}
    ~~~~

Since this operation can potentially affect a lot of nodes, running the change in a single transaction may be
infeasible since the transaction would likely run either too slow, or even run out of memory.

To prevent this, the enclosing change set's `runInTransaction` can be set to `false`.
This results in the rename being executed in batches.

!!! warning
    This setting only works if the target Neo4j instance supports `CALL {} IN TRANSACTIONS` (version 4.4 and later).
    If not, the Neo4j plugin will run the label rename in a single, autocommit transaction.

    Please also remember that Neo4j isolation level is "read-committed". As such, some of the nested transactions
    may actually affect more or fewer elements if concurrent transactions overlap.
    
    Finally, make sure to read about [the consequences of changing `runInTransaction`](#change-sets-runintransaction).


=== "XML"
    ~~~~xml
    {! include '../src/test/resources/e2e/rename-label/changeLog-pattern-batched.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/rename-label/changeLog-pattern-batched.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/rename-label/changeLog-pattern-batched.yaml' !}
    ~~~~

As shown above, the `batchSize` attribute can be set in order to control how many transactions are going to be executed.
If the attribute is not set, the batch size will depend on the Neo4j server's default value.

## Change Set's `runInTransaction`

|Required plugin version|4.19.0|

The default value of `runInTransaction` is `true`. This means that all changes of a given change set run in a single,
explicit transaction.

This is the right default and should be changed only if you need any of the following two Cypher constructs:

- [since Neo4j 4.4]  [`CALL {} IN TRANSACTIONS`](https://neo4j.com/docs/cypher-manual/current/clauses/call-subquery/#subquery-call-in-transactions)
- [until Neo4j 4.4]  [`PERIODIC COMMIT`](https://neo4j.com/docs/cypher-manual/4.4/query-tuning/using/#query-using-periodic-commit-hint)

Indeed, using those constructs without disabling `runInTransaction` fails with a similar error message:

```
A query with 'CALL { ... } IN TRANSACTIONS' can only be executed in an implicit transaction, but tried to execute in an explicit transaction.
```

Setting `runInTransaction` to `false` on a change set means that all its changes are going to run **in their own
auto-commit (or implicit) transaction**.

=== "XML"
    ~~~~xml
    {! include '../src/test/resources/e2e/autocommit/changeLog.xml' !}
    ~~~~

=== "JSON"

    ~~~~json
    {! include '../src/test/resources/e2e/autocommit/changeLog.json' !}
    ~~~~

=== "YAML"

    ~~~~yaml
    {! include '../src/test/resources/e2e/autocommit/changeLog.yaml' !}
    ~~~~

=== "Cypher"

    ~~~~yaml
    {! include '../src/test/resources/e2e/autocommit/changeLog.cypher' !}
    ~~~~

`runInTransaction` is a sharp tool and can lead to unintended consequences.

If any of the change of the enclosing change set fails, the change set is **not** going to be stored in the history graph.

Re-running this change set results in all changes being run again, even the ones that successfully ran before.

In situations where `runInTransactions="false"` cannot be avoided, make sure the affected change set's queries are
idempotent ([constraints](https://neo4j.com/docs/cypher-manual/current/constraints/) must be defined in a prior change
set and using Cypher's `MERGE` instead of `CREATE` usually helps).

{! include-markdown 'includes/_abbreviations.md' !}
