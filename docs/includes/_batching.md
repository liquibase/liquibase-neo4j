Since this operation can potentially affect a lot of data, running the change in a single transaction may be
infeasible since the transaction would likely run either too slow, or even out of memory.

To prevent this, `enableBatchImport` must be set to `true`.
Since it relies on `CALL {} IN TRANSACTIONS` under the hood, the enclosing change set's `runInTransaction` must also be set to `false`.
This results in the change being executed in batches.

!!! warning
    This setting only works if the target Neo4j instance supports `CALL {} IN TRANSACTIONS` (version 4.4 and later).
    If not, the Neo4j plugin will run the change in a single, autocommit transaction.

    Make sure to read about [the consequences of changing `runInTransaction`](#change-sets-runintransaction).

The `batchSize` attribute controls how many transactions run.
If the attribute is not set, the batch size is defined on the Neo4j server side.

The [error policy of inner transactions](https://neo4j.com/docs/cypher-manual/current/subqueries/subqueries-in-transactions/#error-behavior),
introduced in Neo4j 5.7, is configurable with the `batchErrorPolicy` attribute, and accepts the following values:
 - `CONTINUE`
 - `BREAK`
 - `FAIL`

Inner transactions can also be configured to [run in parallel](https://neo4j.com/docs/cypher-manual/current/subqueries/subqueries-in-transactions/#error-behavior) since Neo4j 5.21.
The boolean `concurrent` attribute controls that behavior. Concurrency is disabled by default.
