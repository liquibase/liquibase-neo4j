package liquibase.ext.neo4j.change

import liquibase.database.core.MySQLDatabase
import liquibase.ext.neo4j.database.Neo4jDatabase
import spock.lang.Specification

class InsertNodeChangeTest extends Specification {

    def "supports only Neo4j targets"() {
        expect:
        new InsertNodeChange().supports(database) == result

        where:
        database            | result
        new Neo4jDatabase() | true
        null                | false
        new MySQLDatabase() | false
    }

    def "requires label name to be set"() {
        given:
        def insert = new InsertNodeChange()
        insert.labelName = label

        and:
        def errors = insert.validate(new Neo4jDatabase())

        expect:
        errors.hasErrors() == (error != null)
        if (error != null) {
            errors.errorMessages == [error]
        }

        where:
        label    | error
        "Foobar" | null
        null     | "label name for insert must be specified and not blank"
        ""       | "label name for insert must be specified and not blank"
        "  "     | "label name for insert must be specified and not blank"
    }

    def "sets label name via table name"() {
        given:
        def insert = new InsertNodeChange()
        insert.tableName = label

        and:
        def errors = insert.validate(new Neo4jDatabase())

        expect:
        errors.hasErrors() == (error != null)
        if (error != null) {
            errors.errorMessages == [error]
        }

        where:
        label    | error
        "Foobar" | null
        null     | "label name for insert must be specified and not blank"
        ""       | "label name for insert must be specified and not blank"
        "  "     | "label name for insert must be specified and not blank"
    }
}
