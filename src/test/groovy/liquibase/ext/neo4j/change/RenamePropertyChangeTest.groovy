package liquibase.ext.neo4j.change


import liquibase.database.core.MySQLDatabase
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
        def renameLabelChange = new RenamePropertyChange()
        renameLabelChange.from = from
        renameLabelChange.to = to
        expect:
        renameLabelChange.validate(Mock(Neo4jDatabase)).getErrorMessages() == [error]

        where:
        from      | to          | error
        null      | "VIEWED_BY" | "missing name (from)"
        ""        | "VIEWED_BY" | "missing name (from)"
        null      | "VIEWED_BY" | "missing name (from)"
        ""        | "VIEWED_BY" | "missing name (from)"
        "SEEN_BY" | null        | "missing name (to)"
        "SEEN_BY" | ""          | "missing name (to)"
        "SEEN_BY" | null        | "missing name (to)"
        "SEEN_BY" | ""          | "missing name (to)"
    }
}
