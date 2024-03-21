package liquibase.ext.neo4j.database

import liquibase.CatalogAndSchema
import liquibase.exception.LiquibaseException
import liquibase.ext.neo4j.ReflectionUtils
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
        "jdbc:neo4j:neo4j://localhost/"             | "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
        "jdbc:neo4j:neo4j+s://localhost/"           | "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
        "jdbc:neo4j:neo4j+ssc://localhost/"         | "liquibase.ext.neo4j.database.jdbc.Neo4jDriver"
        "jdbc:mysql://localhost/db"                 | null
        null                                        | null
    }

    def "does not drop objects if catalog does not match"() {
        given:
        def database = new Neo4jDatabase()
        ReflectionUtils.setField("currentDatabase", database, "my-db")

        and:
        def catalog = new CatalogAndSchema("another-db", null)

        when:
        database.dropDatabaseObjects(catalog)

        then:
        def exception = thrown(LiquibaseException.class)
        exception.message == "Cannot drop database objects: expected \"my-db\" catalog, got \"another-db\""
    }
}
