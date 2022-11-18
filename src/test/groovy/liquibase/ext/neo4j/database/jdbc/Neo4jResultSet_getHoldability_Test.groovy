package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Values
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException

class Neo4jResultSet_getHoldability_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets holdability for autocommit statement"() {
        when:
        statement.isAutocommit() >> true

        then:
        resultSet.getHoldability() == ResultSet.HOLD_CURSORS_OVER_COMMIT
    }

    def "gets holdability for regular statement"() {
        when:
        statement.isAutocommit() >> false

        then:
        resultSet.getHoldability() == ResultSet.CLOSE_CURSORS_AT_COMMIT
    }

    def "fails to get holdability if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getHoldability()

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
