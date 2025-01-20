package liquibase.ext.neo4j.change

import liquibase.changelog.ChangeSet
import liquibase.database.core.MySQLDatabase
import liquibase.ext.neo4j.database.KernelVersion
import liquibase.ext.neo4j.database.Neo4jDatabase
import spock.lang.Specification

class RenamePropertyChangeTest extends Specification {

    def "supports only Neo4j targets"() {
        expect:
        new RenamePropertyChange().supports(database) == result

        where:
        database            | result
        new Neo4jDatabase() | true
        null                | false
        new MySQLDatabase() | false
    }

    def "rejects invalid configuration"() {
        given:
        def renamePropertyChange = new RenamePropertyChange()
        renamePropertyChange.from = from
        renamePropertyChange.to = to
        renamePropertyChange.enableBatchImport = enableBatchImport
        renamePropertyChange.batchSize = batchSize
        def changeSet = Mock(ChangeSet)
        changeSet.runInTransaction >> runInTx
        renamePropertyChange.setChangeSet(changeSet)
        def database = Mock(Neo4jDatabase)
        database.supportsCallInTransactions() >> withCIT
        database.getKernelVersion() >> Mock(KernelVersion)

        expect:
        renamePropertyChange.validate(database).getErrorMessages() == [error]

        where:
        runInTx | withCIT | enableBatchImport | batchSize | from  | to    | error
        true    | false   | false             | null      | null  | "new" | "missing name (from)"
        true    | false   | false             | null      | ""    | "new" | "missing name (from)"
        true    | false   | false             | null      | null  | "new" | "missing name (from)"
        true    | false   | false             | null      | ""    | "new" | "missing name (from)"
        true    | false   | false             | null      | "old" | null  | "missing name (to)"
        true    | false   | false             | null      | "old" | ""    | "missing name (to)"
        true    | false   | false             | null      | "old" | null  | "missing name (to)"
        true    | false   | false             | null      | "old" | ""    | "missing name (to)"
        true    | false   | false             | null      | "old" | ""    | "missing name (to)"
        false   | false   | true              | -1L       | "old" | "new" | "batch size, if set, must be strictly positive"
        false   | false   | false             | 50L       | "old" | "new" | "batch size must be set only if enableBatchImport is set to true"
        true    | false   | true              | 50L       | "old" | "new" | "enableBatchImport can be true only if the enclosing change set's runInTransaction attribute is set to false"
    }
}
