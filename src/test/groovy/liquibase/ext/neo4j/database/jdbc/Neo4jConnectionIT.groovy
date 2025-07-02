package liquibase.ext.neo4j.database.jdbc

import liquibase.ext.neo4j.Neo4jContainerSpec
import liquibase.ext.neo4j.database.KernelVersion
import org.neo4j.driver.SessionConfig
import spock.lang.IgnoreIf
import spock.lang.Requires

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.DockerNeo4j.supportsMultiTenancy
import static liquibase.ext.neo4j.database.KernelVersion.V4_0_0
import static liquibase.ext.neo4j.database.KernelVersion.V4_4_0

class Neo4jConnectionIT extends Neo4jContainerSpec {

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
    def "retrieves the current database name as catalog"() {
        given:
        queryRunner.run("CREATE DATABASE \$name",
                        [name: "foobar"],
                        SessionConfig.builder().withDatabase("system").build())
        def props = authenticationProperties()
        props.put("database", "foobar")
        def connection = (Neo4jConnection) new Neo4jDriver().connect(jdbcUrl(), props)

        when:
        def catalog = connection.getCatalog()

        then:
        catalog == "foobar"
    }
}
