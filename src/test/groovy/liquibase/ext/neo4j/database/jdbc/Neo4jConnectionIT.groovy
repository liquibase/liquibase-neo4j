package liquibase.ext.neo4j.database.jdbc

import liquibase.ext.neo4j.Neo4jContainerSpec
import org.neo4j.driver.SessionConfig
import spock.lang.Requires

import java.sql.SQLException

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.database.KernelVersion.V4_0_0

class Neo4jConnectionIT extends Neo4jContainerSpec {

    def "retrieves the current username"() {
        given:
        def connection = (Neo4jConnection) new Neo4jDriver().connect(jdbcUrl(), authenticationProperties())

        when:
        def username = connection.getMetaData().getUserName()

        then:
        username == "neo4j"

        cleanup:
        connection?.close()
    }

    def "commits the explicit transaction when switching to autocommit"() {
        when:
        def connection = (Neo4jConnection) new Neo4jDriver().connect(jdbcUrl(), authenticationProperties())
        connection.setAutoCommit(false)
        assert !connection.getAutoCommit()
        def statement = connection.prepareStatement("RETURN 42")
        statement.executeQuery()
        def transaction = connection.getTransaction()
        assert transaction.isOpen()

        and:
        connection.setAutoCommit(true)
        assert connection.getAutoCommit()

        then:
        !transaction.isOpen()

        cleanup:
        connection.close()
    }

    @Requires({ (neo4jVersion() >= V4_0_0) && enterpriseEdition() })
    def "retrieves the default database name as catalog"() {
        given:
        def connection = (Neo4jConnection) new Neo4jDriver().connect(jdbcUrl(), authenticationProperties())

        when:
        def catalog = connection.getCatalog()

        then:
        catalog == "neo4j"
    }

    @Requires({ (neo4jVersion() >= V4_0_0) && enterpriseEdition() })
    def "retrieves the catalog without closing the active transaction"() {
        given:
        def connection = (Neo4jConnection) new Neo4jDriver().connect(jdbcUrl(), authenticationProperties())
        connection.setAutoCommit(false)
        connection.createStatement().executeQuery("RETURN 42").close()
        def transaction = connection.getTransaction()
        assert transaction.isOpen()

        when:
        def catalog = connection.getCatalog()

        then:
        catalog == "neo4j"
        transaction.isOpen()

        cleanup:
        connection?.rollback()
        connection?.close()
    }

    @Requires({ (neo4jVersion() >= V4_0_0) && enterpriseEdition() })
    def "retrieves the current database name as catalog"() {
        given:
        queryRunner.recreateDatabase("foobar")
        def props = authenticationProperties()
        props.put("database", "foobar")
        def connection = (Neo4jConnection) new Neo4jDriver().connect(jdbcUrl(), props)

        when:
        def catalog = connection.getCatalog()

        then:
        catalog == "foobar"

        cleanup:
        connection?.close()
        queryRunner.dropDatabase("foobar")
    }

    @Requires({ (neo4jVersion() >= V4_0_0) && enterpriseEdition() })
    def "rejects catalog switch with an open transaction"() {
        given:
        queryRunner.recreateDatabase("catalogswitch")
        def connection = (Neo4jConnection) new Neo4jDriver().connect(jdbcUrl(), authenticationProperties())
        connection.setAutoCommit(false)
        connection.createStatement().executeQuery("RETURN 42").close()

        when:
        connection.setCatalog("catalogswitch")

        then:
        def exception = thrown(SQLException)
        exception.message == "Cannot switch catalog to 'catalogswitch' while a transaction is active"
        connection.catalog == "neo4j"
        connection.transaction.isOpen()

        cleanup:
        connection?.rollback()
        connection?.close()
        queryRunner.dropDatabase("catalogswitch")
    }
}
