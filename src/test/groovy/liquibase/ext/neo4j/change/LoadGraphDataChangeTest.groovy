package liquibase.ext.neo4j.change

import liquibase.database.core.MySQLDatabase
import liquibase.ext.neo4j.database.Neo4jDatabase
import spock.lang.Specification

class LoadGraphDataChangeTest extends Specification {


    def "supports only Neo4j targets"() {
        expect:
        new LoadGraphDataChange().supports(database) == result

        where:
        database            | result
        new Neo4jDatabase() | true
        null                | false
        new MySQLDatabase() | false
    }
}
