package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException

import static java.sql.ResultSet.FETCH_FORWARD
import static java.sql.ResultSet.FETCH_REVERSE
import static java.sql.ResultSet.TYPE_FORWARD_ONLY

class Neo4jResultSet_setFetchDirection_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "accepts FETCH_FORWARD fetch direction"() {
        when:
        resultSet.setFetchDirection(FETCH_FORWARD)

        then:
        resultSet.getFetchDirection() == FETCH_FORWARD
    }

    def "fails setting a fetch direction different from FETCH_FORWARD since the type is ResultSet.TYPE_FORWARD_ONLY"() {
        when:
        resultSet.setFetchDirection(FETCH_REVERSE)

        then:
        resultSet.getType() == TYPE_FORWARD_ONLY
        def exception = thrown(SQLException)
        exception.message == "only ResultSet.FETCH_FORWARD is supported"
    }

    def "fails setting fetch direction if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.setFetchDirection(FETCH_FORWARD)

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }

    def "fails getting fetch direction if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getFetchDirection()

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
