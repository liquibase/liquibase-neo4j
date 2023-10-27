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

    def "rejects invalid mandatory fields"() {
        given:
        def renameLabelChange = new RenameLabelChange()
        renameLabelChange.from = from
        renameLabelChange.to = to
        renameLabelChange.batchSize = batchSize
        def changeSet = Mock(ChangeSet)
        changeSet.runInTransaction >> runInTx
        renameLabelChange.setChangeSet(changeSet)
        def database = Mock(Neo4jDatabase)
        database.supportsCallInTransactions() >> withCIT

        expect:
        renameLabelChange.validate(database).getErrorMessages() == [error]

        where:
        runInTx | withCIT | from    | to     | batchSize | error
        false   | true    | null    | "Film" | 1000L     | "missing label (from)"
        false   | true    | ""      | "Film" | 1000L     | "missing label (from)"
        false   | false   | null    | "Film" | 1000L     | "missing label (from)"
        false   | false   | ""      | "Film" | 1000L     | "missing label (from)"
        false   | true    | "Movie" | null   | 1000L     | "missing label (to)"
        false   | true    | "Movie" | ""     | 1000L     | "missing label (to)"
        false   | false   | "Movie" | null   | 1000L     | "missing label (to)"
        false   | false   | "Movie" | ""     | 1000L     | "missing label (to)"
        false   | true    | "Movie" | "Film" | -1L       | "batch size, if set, must be strictly positive"
        false   | false   | "Movie" | "Film" | -1L       | "batch size, if set, must be strictly positive"
        true    | true    | "Movie" | "Film" | 1000L     | "the enclosing change set's runInTransaction attribute must be set to false, it is currently true"
    }
}
