package liquibase.ext.neo4j.database.jdbc


import spock.lang.Specification

import java.sql.Driver
import java.sql.DriverManager

class Neo4jDriverTest extends Specification {

    def "accepts JDBC Neo4j URL"() {
        expect:
        new Neo4jDriver().acceptsURL(url) == accepts

        where:
        url                                         | accepts
        "jdbc:neo4j:bolt://localhost/"              | true
        "jdbc:neo4j:bolt+s://localhost/"            | true
        "jdbc:neo4j:bolt+ssc://localhost/"          | true
        "jdbc:neo4j:bolt+routing://localhost:1234/" | true
        "jdbc:neo4j:neo4j://localhost/"             | true
        "jdbc:neo4j:neo4j+s://localhost/"           | true
        "jdbc:neo4j:neo4j+ssc://localhost/"         | true
        "jdbc:neo4j:nope+ssc://localhost/"          | false
        "jdbc:mysql://localhost/db"                 | false
        ""                                          | false
        null                                        | false
    }

    // see section 9.2 of JDBC 4.2 spec
    def "finds registered driver via static initializer"() {
        given:
        Class.forName("liquibase.ext.neo4j.database.jdbc.Neo4jDriver")

        when:
        def driver = DriverManager.getDriver("jdbc:neo4j:neo4j://example.com")

        then:
        driver != null
    }

    // see section 9.2.1 of JDBC 4.2 spec
    def "finds registered driver via serviceloader declaration"() {
        when:
        def drivers = ServiceLoader.load(Driver.class)

        then:
        drivers.find { it instanceof Neo4jDriver } != null
    }
}
