package liquibase.ext.neo4j.database

import liquibase.database.DatabaseConnection
import liquibase.database.DatabaseFactory
import liquibase.ext.neo4j.DockerNeo4j
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Neo4jContainer
import spock.lang.Shared
import spock.lang.Specification

import java.time.ZoneId
import java.util.logging.LogManager

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion

class Neo4jDatabaseIT extends Specification {

    static {
        LogManager.getLogManager().reset()
    }

    private static final String PASSWORD = "sup3rs3cur3"

    private DatabaseConnection connection

    @Shared
    GenericContainer<Neo4jContainer> neo4jContainer = DockerNeo4j.container(
            PASSWORD,
            ZoneId.of("Europe/Paris")
    )

    def setupSpec() {
        neo4jContainer.start()
    }

    def setup() {
        connection = openConnection()
    }

    def cleanup() {
        connection.close()
    }

    def cleanupSpec() {
        neo4jContainer.stop()
    }

    def "retrieves database version upon connection"() {
        given:
        def database = new Neo4jDatabase()

        when:
        database.setConnection(connection)

        then:
        database.getNeo4jVersion().startsWith(neo4jVersion())
    }

    def "supports catalog if Neo4j version is 4+"() {
        given:
        def database = new Neo4jDatabase()

        when:
        database.setConnection(connection)

        then:
        database.supportsCatalogs() == neo4jVersion().startsWith("4")
    }

    def "detects server edition"() {
        given:
        def database = new Neo4jDatabase()

        when:
        database.setConnection(connection)

        then:
        database.isEnterprise() == enterpriseEdition()
    }

    private DatabaseConnection openConnection() {
        return DatabaseFactory.instance.openConnection(
                "jdbc:neo4j:" + neo4jContainer.getBoltUrl(),
                "neo4j",
                PASSWORD,
                null,
                null
        )
    }
}
