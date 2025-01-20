package liquibase.ext.neo4j.database

import liquibase.database.DatabaseConnection
import liquibase.database.DatabaseFactory
import liquibase.ext.neo4j.DockerNeo4j
import liquibase.ext.neo4j.Neo4jContainerSpec

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion

class Neo4jDatabaseIT extends Neo4jContainerSpec {

    private DatabaseConnection connection

    def setup() {
        connection = openConnection()
    }

    def cleanup() {
        connection.close()
    }

    def "retrieves database version upon connection"() {
        given:
        def database = new Neo4jDatabase()

        when:
        database.setConnection(connection)

        then:
        def configuredVersion = neo4jVersion()
        def actualVersion = database.getKernelVersion()
        actualVersion.major() == configuredVersion.major() &&
                (actualVersion.minor() == configuredVersion.minor() || configuredVersion.minor() == Integer.MAX_VALUE)
    }

    def "supports catalog if Neo4j version is 4+ and edition is enterprise"() {
        given:
        def database = new Neo4jDatabase()

        when:
        database.setConnection(connection)

        then:
        database.supportsCatalogs() == (neo4jVersion() >= KernelVersion.V4_0 && enterpriseEdition())
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
