package liquibase.ext.neo4j.database.jdbc

import spock.lang.Specification

class SupportedJdbcUrlTest extends Specification {

    def "normalizes JDBC URL for Neo4j"() {
        expect:

        where:
        url                                        | result
        "jdbc:neo4j:bolt://localhost"              | "bolt://localhost"
        "jdbc:neo4j:neo4j+ssc://localhost?foo=bar" | "neo4j+ssc://localhost"
    }
}
