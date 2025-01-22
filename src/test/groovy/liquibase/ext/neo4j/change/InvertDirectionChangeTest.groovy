package liquibase.ext.neo4j.change

import liquibase.changelog.ChangeSet
import liquibase.database.core.MySQLDatabase
import liquibase.ext.neo4j.database.KernelVersion
import liquibase.ext.neo4j.database.Neo4jDatabase
import spock.lang.Specification

class InvertDirectionChangeTest extends Specification {

    def "supports only Neo4j targets"() {
        expect:
        new InvertDirectionChange().supports(database) == result

        where:
        database            | result
        new Neo4jDatabase() | true
        null                | false
        new MySQLDatabase() | false
    }

    def "rejects invalid configuration"() {
        given:
        def renameLabelChange = new InvertDirectionChange()
        renameLabelChange.type = type
        renameLabelChange.enableBatchImport = enableBatchImport
        renameLabelChange.batchSize = batchSize
        renameLabelChange.fragment = fragment
        renameLabelChange.outputVariable = outputVariable
        def changeSet = Mock(ChangeSet)
        changeSet.runInTransaction >> runInTx
        renameLabelChange.setChangeSet(changeSet)
        def database = Mock(Neo4jDatabase)
        def version = (withCIT) ? KernelVersion.V4_4_0 : KernelVersion.V4_3_0
        database.getKernelVersion() >> version

        expect:
        renameLabelChange.validate(database).getErrorMessages() == [error]

        where:
        runInTx | withCIT | type        | enableBatchImport | batchSize | fragment           | outputVariable | error
        false   | true    | null        | true              | 1000L     | "()-[r]->()"       | "r"            | "missing type"
        false   | true    | ""          | true              | 1000L     | "()-[r]->()"       | "r"            | "missing type"
        false   | true    | "SOME_TYPE" | true              | 1000L     | "()-[r]->()"       | null           | "both fragment and outputVariable must be set (only fragment is currently set), or both must be unset"
        false   | false   | "SOME_TYPE" | true              | 1000L     | null               | "r"            | "both fragment and outputVariable must be set (only outputVariable is currently set), or both must be unset"
        false   | false   | "SOME_TYPE" | true              | 1000L     | "()-[__rel__]->()" | "__rel__"      | "outputVariable __rel__ clashes with the reserved variable name: __rel__. outputVariable must be renamed and fragment accordingly updated"
        false   | true    | "SOME_TYPE" | true              | -1L       | null               | null           | "batch size, if set, must be strictly positive"
        false   | false   | "SOME_TYPE" | true              | -1L       | null               | null           | "batch size, if set, must be strictly positive"
        true    | false   | "SOME_TYPE" | false             | 1000L     | null               | null           | "batch size must be set only if enableBatchImport is set to true"
        true    | false   | "SOME_TYPE" | true              | 1000L     | null               | null           | "enableBatchImport can be true only if the enclosing change set's runInTransaction attribute is set to false"
    }
}
