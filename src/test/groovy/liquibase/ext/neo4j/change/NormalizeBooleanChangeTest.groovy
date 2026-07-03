package liquibase.ext.neo4j.change

import liquibase.changelog.ChangeSet
import liquibase.database.core.MySQLDatabase
import liquibase.ext.neo4j.database.KernelVersion
import liquibase.ext.neo4j.database.Neo4jDatabase
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
}
