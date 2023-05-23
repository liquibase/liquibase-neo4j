package liquibase.ext.neo4j.database

import liquibase.database.DatabaseConnection
import liquibase.database.DatabaseFactory
import liquibase.exception.DatabaseException
import liquibase.ext.neo4j.Neo4jContainerSpec
import liquibase.ext.neo4j.database.jdbc.Neo4jTransactionState
import liquibase.statement.core.RawParameterizedSqlStatement
import liquibase.structure.core.Catalog
import spock.lang.Requires

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


    @Requires({ !neo4jVersion().isCalver() })
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
        database.supportsCatalogs() == (neo4jVersion() >= KernelVersion.V4_0_0 && enterpriseEdition())
    }

    def "detects server edition"() {
        given:
        def database = new Neo4jDatabase()

        when:
        database.setConnection(connection)

        then:
        database.isEnterprise() == enterpriseEdition()
    }

    @Requires({ neo4jVersion() >= KernelVersion.V4_0_0 && enterpriseEdition() })
    def "runs statement in requested catalog and restores the previous catalog"() {
        given:
        queryRunner.recreateDatabase("app")
        def database = new Neo4jDatabase()
        database.setConnection(connection)

        when:
        def rows = database.run(
                new Catalog("app"),
                new RawParameterizedSqlStatement("CREATE (m:Movie {title: 'Heat'}) RETURN m.title AS title"))

        then:
        rows == [[title: "Heat"]]
        connection.underlyingConnection.catalog == "neo4j"
        queryRunner.getSingleRow("app", "MATCH (m:Movie {title: 'Heat'}) RETURN count(m) AS count")["count"] == 1L
        queryRunner.getSingleRow("MATCH (m:Movie {title: 'Heat'}) RETURN count(m) AS count")["count"] == 0L

        cleanup:
        database?.rollback()
        connection?.underlyingConnection?.setCatalog("neo4j")
        queryRunner?.dropDatabase("app")
    }

    @Requires({ neo4jVersion() >= KernelVersion.V4_0_0 && enterpriseEdition() })
    def "commits transaction before restoring the previous catalog"() {
        given:
        queryRunner.recreateDatabase("apptx")
        connection.underlyingConnection.setAutoCommit(false)
        def database = new Neo4jDatabase()
        database.setConnection(connection)

        when:
        def rows = database.run(
                new Catalog("apptx"),
                new RawParameterizedSqlStatement("CREATE (m:Movie {title: 'Thief'}) RETURN m.title AS title"))

        then:
        rows == [[title: "Thief"]]
        !connection.underlyingConnection.unwrap(Neo4jTransactionState.class).hasActiveTransaction()
        connection.underlyingConnection.catalog == "neo4j"
        queryRunner.getSingleRow("apptx", "MATCH (m:Movie {title: 'Thief'}) RETURN count(m) AS count")["count"] == 1L
        queryRunner.getSingleRow("MATCH (m:Movie {title: 'Thief'}) RETURN count(m) AS count")["count"] == 0L

        cleanup:
        database?.rollback()
        connection?.underlyingConnection?.setCatalog("neo4j")
        connection?.underlyingConnection?.setAutoCommit(true)
        queryRunner?.dropDatabase("apptx")
    }

    @Requires({ neo4jVersion() >= KernelVersion.V4_0_0 && enterpriseEdition() })
    def "rejects catalog switch when the current catalog has uncommitted work"() {
        given:
        queryRunner.recreateDatabase("app")
        def database = new Neo4jDatabase()
        database.setConnection(connection)
        database.run(new RawParameterizedSqlStatement("CREATE (m:Movie {title: 'Collateral'}) RETURN m.title AS title"))

        when:
        database.run(new Catalog("app"), new RawParameterizedSqlStatement("RETURN 1 AS value"))

        then:
        def failure = thrown(DatabaseException)
        failure.message == "Cannot run statement against Neo4j database catalog 'app' while the connection has an active transaction against catalog 'neo4j'"
        connection.underlyingConnection.catalog == "neo4j"
        queryRunner.getSingleRow("MATCH (m:Movie {title: 'Collateral'}) RETURN count(m) AS count")["count"] == 0L

        cleanup:
        database?.rollback()
        connection?.underlyingConnection?.setCatalog("neo4j")
        queryRunner?.dropDatabase("app")
    }

    @Requires({ neo4jVersion() >= KernelVersion.V4_0_0 && enterpriseEdition() })
    def "keeps the current catalog when it already matches the requested catalog"() {
        given:
        queryRunner.recreateDatabase("app")
        connection.underlyingConnection.setCatalog("app")
        def database = new Neo4jDatabase()
        database.setConnection(connection)

        when:
        def rows = database.run(
                new Catalog("app"),
                new RawParameterizedSqlStatement("CALL db.info() YIELD name RETURN name"))

        then:
        rows == [[name: "app"]]
        connection.underlyingConnection.catalog == "app"

        cleanup:
        database?.rollback()
        connection?.underlyingConnection?.setCatalog("neo4j")
        queryRunner?.dropDatabase("app")
    }

    @Requires({ neo4jVersion() >= KernelVersion.V4_0_0 && enterpriseEdition() })
    def "restores the previous catalog when statement execution fails after switching"() {
        given:
        queryRunner.recreateDatabase("audit")
        def database = new Neo4jDatabase()
        database.setConnection(connection)

        when:
        database.run(
                new Catalog("audit"),
                new RawParameterizedSqlStatement("THIS IS NOT CYPHER"))

        then:
        def failure = thrown(Exception)
        failure.message.contains("THIS IS NOT CYPHER")
        connection.underlyingConnection.catalog == "neo4j"

        cleanup:
        database?.rollback()
        connection?.underlyingConnection?.setCatalog("neo4j")
        queryRunner?.dropDatabase("audit")
    }

    @Requires({ neo4jVersion() >= KernelVersion.V4_0_0 && enterpriseEdition() })
    def "rolls back transaction before restoring the previous catalog after statement execution fails"() {
        given:
        queryRunner.recreateDatabase("audittx")
        connection.underlyingConnection.setAutoCommit(false)
        def database = new Neo4jDatabase()
        database.setConnection(connection)

        when:
        database.run(
                new Catalog("audittx"),
                new RawParameterizedSqlStatement("THIS IS NOT CYPHER"))

        then:
        def failure = thrown(Exception)
        failure.message.contains("THIS IS NOT CYPHER")
        !connection.underlyingConnection.unwrap(Neo4jTransactionState.class).hasActiveTransaction()
        connection.underlyingConnection.catalog == "neo4j"

        cleanup:
        database?.rollback()
        connection?.underlyingConnection?.setCatalog("neo4j")
        connection?.underlyingConnection?.setAutoCommit(true)
        queryRunner?.dropDatabase("audittx")
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
