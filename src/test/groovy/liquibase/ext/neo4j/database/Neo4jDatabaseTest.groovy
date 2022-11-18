package liquibase.ext.neo4j.database


import spock.lang.Specification

class Neo4jDatabaseTest extends Specification {

    def "instantiates Neo4j database"() {
        expect:
        new Neo4jDatabase().getDefaultDriver(url) == expected

        where:
        url                                         | expected
        "jdbc:neo4j:bolt://localhost/"              | "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
        "jdbc:neo4j:bolt+s://localhost/"            | "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
        "jdbc:neo4j:bolt+ssc://localhost/"          | "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
        "jdbc:neo4j:bolt+routing://localhost:1234/" | "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
        "jdbc:neo4j:neo4j://localhost/"             | "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
        "jdbc:neo4j:neo4j+s://localhost/"           | "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
        "jdbc:neo4j:neo4j+ssc://localhost/"         | "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
        "jdbc:mysql://localhost/db"                 | null
        null                                        | null
    }
}
