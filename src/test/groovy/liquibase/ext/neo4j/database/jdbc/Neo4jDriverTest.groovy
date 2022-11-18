package liquibase.ext.neo4j.database.jdbc


import spock.lang.Specification

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
}
