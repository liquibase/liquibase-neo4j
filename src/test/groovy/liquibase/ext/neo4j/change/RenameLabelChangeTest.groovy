package liquibase.ext.neo4j.change

import liquibase.changelog.ChangeSet
import liquibase.database.core.MySQLDatabase
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
        renameLabelChange.batchSize = batchSize
        renameLabelChange.fragment = fragment
        renameLabelChange.outputVariable = outputVariable
        def changeSet = Mock(ChangeSet)
        changeSet.runInTransaction >> runInTx
        renameLabelChange.setChangeSet(changeSet)
        def database = Mock(Neo4jDatabase)
        database.supportsCallInTransactions() >> withCIT

        expect:
        renameLabelChange.validate(database).getErrorMessages() == [error]

        where:
        runInTx | withCIT | from    | to     | batchSize | fragment     | outputVariable | error
        false   | true    | null    | "Film" | 1000L     | null         | null           | "missing label (from)"
        false   | true    | ""      | "Film" | 1000L     | null         | null           | "missing label (from)"
        false   | false   | null    | "Film" | 1000L     | null         | null           | "missing label (from)"
        false   | false   | ""      | "Film" | 1000L     | null         | null           | "missing label (from)"
        false   | true    | "Movie" | null   | 1000L     | null         | null           | "missing label (to)"
        false   | true    | "Movie" | ""     | 1000L     | null         | null           | "missing label (to)"
        false   | false   | "Movie" | null   | 1000L     | null         | null           | "missing label (to)"
        false   | false   | "Movie" | ""     | 1000L     | null         | null           | "missing label (to)"
        false   | true    | "Movie" | "Film" | 1000L     | "(n)"        | null           | "both fragment and outputVariable must be set (only fragment is currently set), or both must be unset"
        false   | false   | "Movie" | "Film" | 1000L     | null         | "n"            | "both fragment and outputVariable must be set (only outputVariable is currently set), or both must be unset"
        false   | false   | "Movie" | "Film" | 1000L     | "(__node__)" | "__node__"     | "__node__ is a reserved variable name, outputVariable must be renamed and fragment accordingly updated"
        false   | true    | "Movie" | "Film" | -1L       | null         | null           | "batch size, if set, must be strictly positive"
        false   | false   | "Movie" | "Film" | -1L       | null         | null           | "batch size, if set, must be strictly positive"
        true    | false   | "Movie" | "Film" | 1000L     | null         | null           | "batch size must be set only if the enclosing change set's runInTransaction attribute is set to false"
    }
}
