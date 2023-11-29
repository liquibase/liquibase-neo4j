package liquibase.ext.neo4j.change

import liquibase.changelog.ChangeSet
import liquibase.database.core.MySQLDatabase
import liquibase.ext.neo4j.database.Neo4jDatabase
import spock.lang.Specification

class RenameTypeChangeTest extends Specification {

    def "supports only Neo4j targets"() {
        expect:
        new RenameTypeChange().supports(database) == result

        where:
        database            | result
        new Neo4jDatabase() | true
        null                | false
        new MySQLDatabase() | false
    }

    def "rejects invalid configuration"() {
        given:
        def renameLabelChange = new RenameTypeChange()
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
        database.getNeo4jVersion() >> databaseVersion
        database.supportsCallInTransactions() >> withCIT

        expect:
        renameLabelChange.validate(database).getErrorMessages() == [error]

        where:
        databaseVersion | runInTx | withCIT | from      | to          | enableBatchImport | batchSize | fragment           | outputVariable | error
        "3.5"           | false   | true    | "SEEN_BY" | "VIEWED_BY" | true              | 1000L     | null               | null           | "this version of Neo4j is not supported and has reached end of life, please upgrade"
        "4.4"           | false   | true    | null      | "VIEWED_BY" | true              | 1000L     | null               | null           | "missing type (from)"
        "4.4"           | false   | true    | null      | "VIEWED_BY" | true              | 1000L     | null               | null           | "missing type (from)"
        "4.4"           | false   | true    | ""        | "VIEWED_BY" | true              | 1000L     | null               | null           | "missing type (from)"
        "4.4"           | false   | false   | null      | "VIEWED_BY" | true              | 1000L     | null               | null           | "missing type (from)"
        "4.4"           | false   | false   | ""        | "VIEWED_BY" | true              | 1000L     | null               | null           | "missing type (from)"
        "4.4"           | false   | true    | "SEEN_BY" | null        | true              | 1000L     | null               | null           | "missing type (to)"
        "4.4"           | false   | true    | "SEEN_BY" | ""          | true              | 1000L     | null               | null           | "missing type (to)"
        "4.4"           | false   | false   | "SEEN_BY" | null        | true              | 1000L     | null               | null           | "missing type (to)"
        "4.4"           | false   | false   | "SEEN_BY" | ""          | true              | 1000L     | null               | null           | "missing type (to)"
        "4.4"           | false   | true    | "SEEN_BY" | "VIEWED_BY" | true              | 1000L     | "()-[r]->()"       | null           | "both fragment and outputVariable must be set (only fragment is currently set), or both must be unset"
        "4.4"           | false   | false   | "SEEN_BY" | "VIEWED_BY" | true              | 1000L     | null               | "n"            | "both fragment and outputVariable must be set (only outputVariable is currently set), or both must be unset"
        "4.4"           | false   | false   | "SEEN_BY" | "VIEWED_BY" | true              | 1000L     | "()-[__rel__]->()" | "__rel__"      | "outputVariable __rel__ clashes with the reserved variable name: __rel__. outputVariable must be renamed and fragment accordingly updated"
        "4.4"           | false   | true    | "SEEN_BY" | "VIEWED_BY" | true              | -1L       | null               | null           | "batch size, if set, must be strictly positive"
        "4.4"           | false   | false   | "SEEN_BY" | "VIEWED_BY" | true              | -1L       | null               | null           | "batch size, if set, must be strictly positive"
        "4.4"           | true    | false   | "SEEN_BY" | "VIEWED_BY" | false             | 1000L     | null               | null           | "batch size must be set only if enableBatchImport is set to true"
        "4.4"           | true    | false   | "SEEN_BY" | "VIEWED_BY" | true              | 1000L     | null               | null           | "enableBatchImport can be true only if the enclosing change set's runInTransaction attribute is set to false"
    }
}
