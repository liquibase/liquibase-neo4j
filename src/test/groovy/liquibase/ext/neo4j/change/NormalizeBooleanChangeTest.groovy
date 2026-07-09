package liquibase.ext.neo4j.change

import liquibase.changelog.ChangeSet
import liquibase.database.core.MySQLDatabase
import liquibase.ext.neo4j.database.KernelVersion
import liquibase.ext.neo4j.database.Neo4jDatabase
import liquibase.statement.core.RawParameterizedSqlStatement
import spock.lang.Specification

class NormalizeBooleanChangeTest extends Specification {

    def "supports only Neo4j targets"() {
        expect:
        new NormalizeBooleanChange().supports(database) == result

        where:
        database            | result
        new Neo4jDatabase() | true
        null                | false
        new MySQLDatabase() | false
    }

    def "rejects invalid configuration"() {
        given:
        def change = new NormalizeBooleanChange()
        change.property = property
        change.trueValues = trueValues
        change.enableBatchImport = enableBatchImport
        change.batchSize = batchSize
        def changeSet = Mock(ChangeSet)
        changeSet.runInTransaction >> runInTx
        change.setChangeSet(changeSet)
        def database = Mock(Neo4jDatabase)
        database.getKernelVersion() >> KernelVersion.V5_26_0

        expect:
        change.validate(database).getErrorMessages().contains(error)

        where:
        runInTx | enableBatchImport | batchSize | property  | trueValues | error
        true    | false             | null      | null      | "YES"      | "missing property name"
        true    | false             | null      | ""        | "YES"      | "missing property name"
        false   | false             | 50L       | "watched" | "YES"      | "batch size must be set only if enableBatchImport is set to true"
        true    | true              | null      | "watched" | "YES"      | "enableBatchImport can be true only if the enclosing change set's runInTransaction attribute is set to false"
    }

    def "parses comma-separated trueValues"() {
        given:
        def change = new NormalizeBooleanChange()
        change.setTrueValues("YES, y")

        expect:
        change.trueValues == "YES, y"
    }

    def "generates batched cypher when enableBatchImport is true"() {
        given:
        def change = new NormalizeBooleanChange()
        change.property = "watched"
        change.trueValues = "YES,y"
        change.falseValues = "no,n"
        change.enableBatchImport = true
        change.batchSize = 1L
        def changeSet = Mock(ChangeSet)
        changeSet.runInTransaction >> false
        change.setChangeSet(changeSet)
        def database = Mock(Neo4jDatabase)
        database.getKernelVersion() >> KernelVersion.V5_26_0

        when:
        def statement = change.generateStatements(database)[0] as RawParameterizedSqlStatement

        then:
        statement.sql.contains("CALL { WITH e SET")
        statement.sql.contains("IN TRANSACTIONS OF 1 ROWS")
    }

    def "generates unbatched cypher when enableBatchImport is false"() {
        given:
        def change = new NormalizeBooleanChange()
        change.property = "watched"
        change.trueValues = "YES,y"
        change.falseValues = "no,n"
        def database = Mock(Neo4jDatabase)
        database.getKernelVersion() >> KernelVersion.V5_26_0

        when:
        def statement = change.generateStatements(database)[0] as RawParameterizedSqlStatement

        then:
        statement.sql.contains("SET e.`watched` = CASE")
        statement.sql.contains("ELSE e.`watched` END")
        !statement.sql.contains("IN TRANSACTIONS")
    }

    def "deletes unmatched values when deleteUnmatched is true"() {
        given:
        def change = new NormalizeBooleanChange()
        change.property = "watched"
        change.trueValues = "YES,y"
        change.falseValues = "no,n"
        change.deleteUnmatched = true
        def database = Mock(Neo4jDatabase)
        database.getKernelVersion() >> KernelVersion.V5_26_0

        when:
        def statement = change.generateStatements(database)[0] as RawParameterizedSqlStatement

        then:
        statement.sql.contains("ELSE null END")
        !statement.sql.contains("ELSE e.`watched` END")
    }
}
