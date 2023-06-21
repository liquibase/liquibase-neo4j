package liquibase.ext.neo4j.database.jdbc

import liquibase.ext.neo4j.Neo4jContainerSpec

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
}
