package liquibase.ext.neo4j.database.jdbc


import liquibase.ext.neo4j.Neo4jContainerSpec
import spock.lang.Requires

import static liquibase.ext.neo4j.DockerNeo4j.supportsCypherCallInTransactions

class Neo4jStatementIT extends Neo4jContainerSpec {

    def "executes simple statements"() {
        given:
        def connection = new Neo4jDriver().connect(jdbcUrl(), authenticationProperties())
        def statement = connection.createStatement()

        when:
        def results = statement.executeQuery("RETURN 42")

        then:
        results.next()
        results.getLong("42") == 42

        cleanup:
        results.close()
        statement.close()
        connection.close()
    }

    def "can rollback simple statement, execute it again and then commit"() {
        given:
        def connection = new Neo4jDriver().connect(jdbcUrl(), authenticationProperties())
        def statement = connection.createStatement()

        when:
        def results = statement.executeQuery("MERGE (c:Count) ON CREATE SET c.count = 1 ON MATCH SET c.count = c.count+1 RETURN c.count AS count")
        results.close()
        connection.rollback()

        and:
        results = statement.executeQuery("MERGE (c:Count) ON CREATE SET c.count = 1 ON MATCH SET c.count = c.count+1 RETURN c.count AS count")

        then:
        results.next()
        results.getLong("count") == 1
        results.close()
        connection.commit()

        cleanup:
        statement.close()
        connection.close()
    }

    @Requires({ supportsCypherCallInTransactions() })
    def "executes auto-commit statements"() {
        given:
        def connection = new Neo4jDriver().connect(jdbcUrl(), authenticationProperties())
        connection.setAutoCommit(true)
        def statement = connection.createStatement()

        when:
        def results = statement.executeQuery("CALL {RETURN 42 as value} IN TRANSACTIONS RETURN value")

        then:
        results.next()
        results.getLong("value") == 42

        cleanup:
        results.close()
        statement.close()
        connection.close()
    }

    def "executes parameterized statements"() {
        given:
        def connection = new Neo4jDriver().connect(jdbcUrl(), authenticationProperties())
        def statement = connection.prepareStatement("RETURN \$1 AS truth")

        when:
        statement.setLong(1, 42)
        def results = statement.executeQuery()

        then:
        results.next()
        results.getLong("truth") == 42

        cleanup:
        results.close()
        statement.close()
        connection.close()
    }

    def "can rollback parameterized statement, execute it again and then commit"() {
        given:
        def connection = new Neo4jDriver().connect(jdbcUrl(), authenticationProperties())
        def statement = connection.prepareStatement("MERGE (c:Count) ON CREATE SET c.count = \$1 ON MATCH SET c.count = c.count+1 RETURN c.count AS count")

        when:
        statement.setLong(1, 36)
        def results = statement.executeQuery()
        results.close()
        connection.rollback()

        and:
        results = statement.executeQuery()

        then:
        results.next()
        results.getLong("count") == 36
        results.close()
        connection.commit()

        cleanup:
        statement.close()
        connection.close()
    }

    @Requires({ supportsCypherCallInTransactions() })
    def "executes autocommit parameterized statements"() {
        given:
        def connection = new Neo4jDriver().connect(jdbcUrl(), authenticationProperties())
        connection.setAutoCommit(true)
        def statement = connection.prepareStatement("CALL {RETURN \$1 as value} IN TRANSACTIONS RETURN value")

        when:
        statement.setLong(1, 42)
        def results = statement.executeQuery()

        then:
        results.next()
        results.getLong("value") == 42

        cleanup:
        results.close()
        statement.close()
        connection.close()
    }
}
