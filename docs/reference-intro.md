# Mental Model

Learn the main concepts that describe the execution model of Liquibase.

## Changes

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

## Change Sets and Transactions

Changes are grouped into change sets.

Changes of a single change set are run in a single transaction by default. Either all the changes succeed or they all fail.

!!! important
    Neo4j [schema operations](https://neo4j.com/docs/getting-started/current/cypher-intro/schema/)
    and data operations cannot be part of the same transaction. They are better defined in different change sets.

You can disable this behavior by setting the change set `runInTransaction` attribute to `false`.
In that case, each change of the change set runs in its own auto-commit transaction.

Learn more about the best practice around [the `runInTransaction` usage](/reference-features/#change-sets-runintransaction).
 
## Change Logs and Consistency

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

## Persistence and History Graph

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
    If the schema changes, `liquibase-neo4j` will automatically run internal migrations against the history graph
    persisted by a former version.

## Concurrency

`liquibase-neo4j` must not be executed concurrently against the same server and database.
Similar to the [`DATABASECHANGELOGLOCK` table](https://docs.liquibase.com/concepts/tracking-tables/databasechangeloglock-table.html), 
`liquibase-neo4j` attempts to store a unique `__LiquibaseLock` node. If this fails, this means another execution is going on and the execution stops.

{! include-markdown 'includes/_abbreviations.md' !}
