package liquibase.ext.neo4j.change

import liquibase.changelog.ChangeSet
import liquibase.database.core.MySQLDatabase
import liquibase.ext.neo4j.database.KernelVersion
import liquibase.ext.neo4j.database.Neo4jDatabase
import spock.lang.Specification

class RenameLabelChangeTest extends Specification {

    def "supports only Neo4j targets"() {
        expect:
        new RenameLabelChange().supports(database) == result

        where:
        database            | result
        new Neo4jDatabase() | true
        null                | false
        new MySQLDatabase() | false
    }

    def "rejects invalid configuration"() {
        given:
        def renameLabelChange = new RenameLabelChange()
        renameLabelChange.from = from
        renameLabelChange.to = to
        renameLabelChange.enableBatchImport = enableBatchImport
        renameLabelChange.batchSize = batchSize
        renameLabelChange.fragment = fragment
        renameLabelChange.outputVariable = outputVariable
        def changeSet = Mock(ChangeSet)
        changeSet.runInTransaction >> runInTx
        renameLabelChange.setChangeSet(changeSet)
        def database = Mock(Neo4jDatabase)
        database.supportsCallInTransactions() >> withCIT
        database.getKernelVersion() >> Mock(KernelVersion)

        expect:
        renameLabelChange.validate(database).getErrorMessages() == [error]

        where:
        runInTx | withCIT | from    | to     | enableBatchImport | batchSize | fragment     | outputVariable | error
        false   | true    | null    | "Film" | true              | 1000L     | null         | null           | "missing label (from)"
        false   | true    | ""      | "Film" | true              | 1000L     | null         | null           | "missing label (from)"
        false   | false   | null    | "Film" | true              | 1000L     | null         | null           | "missing label (from)"
        false   | false   | ""      | "Film" | true              | 1000L     | null         | null           | "missing label (from)"
        false   | true    | "Movie" | null   | true              | 1000L     | null         | null           | "missing label (to)"
        false   | true    | "Movie" | ""     | true              | 1000L     | null         | null           | "missing label (to)"
        false   | false   | "Movie" | null   | true              | 1000L     | null         | null           | "missing label (to)"
        false   | false   | "Movie" | ""     | true              | 1000L     | null         | null           | "missing label (to)"
        false   | true    | "Movie" | "Film" | true              | 1000L     | "(n)"        | null           | "both fragment and outputVariable must be set (only fragment is currently set), or both must be unset"
        false   | false   | "Movie" | "Film" | true              | 1000L     | null         | "n"            | "both fragment and outputVariable must be set (only outputVariable is currently set), or both must be unset"
        false   | false   | "Movie" | "Film" | true              | 1000L     | "(__node__)" | "__node__"     | "__node__ is a reserved variable name, outputVariable must be renamed and fragment accordingly updated"
        false   | true    | "Movie" | "Film" | true              | -1L       | null         | null           | "batch size, if set, must be strictly positive"
        false   | false   | "Movie" | "Film" | true              | -1L       | null         | null           | "batch size, if set, must be strictly positive"
        true    | false   | "Movie" | "Film" | false             | 1000L     | null         | null           | "batch size must be set only if enableBatchImport is set to true"
        true    | false   | "Movie" | "Film" | true              | 1000L     | null         | null           | "enableBatchImport can be true only if the enclosing change set's runInTransaction attribute is set to false"
    }
}
