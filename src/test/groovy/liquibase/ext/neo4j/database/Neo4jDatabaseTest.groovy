package liquibase.ext.neo4j.database


import spock.lang.Specification

class Neo4jDatabaseTest extends Specification {

    def "instantiates Neo4j database"() {
        expect:
        new Neo4jDatabase().getDefaultDriver(url) == expected

        where:
        url                                         | expected
        "jdbc:neo4j:bolt://localhost/"              | "org.neo4j.jdbc.bolt.BoltDriver"
        "jdbc:neo4j:bolt+s://localhost/"            | "org.neo4j.jdbc.bolt.BoltDriver"
        "jdbc:neo4j:bolt+ssc://localhost/"          | "org.neo4j.jdbc.bolt.BoltDriver"
        "jdbc:neo4j:bolt+routing://localhost:1234/" | "org.neo4j.jdbc.boltrouting.BoltRoutingNeo4jDriver"
        "jdbc:neo4j:neo4j://localhost/"             | "org.neo4j.jdbc.boltrouting.BoltRoutingNeo4jDriver"
        "jdbc:neo4j:neo4j+s://localhost/"           | "org.neo4j.jdbc.boltrouting.BoltRoutingNeo4jDriver"
        "jdbc:neo4j:neo4j+ssc://localhost/"         | "org.neo4j.jdbc.boltrouting.BoltRoutingNeo4jDriver"
        "jdbc:mysql://localhost/db"                 | null
        null                                        | null
    }
}
