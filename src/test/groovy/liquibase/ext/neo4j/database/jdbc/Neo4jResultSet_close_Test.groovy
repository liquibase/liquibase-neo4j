package liquibase.ext.neo4j.database.jdbc


import org.neo4j.driver.Result
import org.neo4j.driver.exceptions.NoSuchRecordException
import org.neo4j.driver.types.TypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException

class Neo4jResultSet_close_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, Mock(TypeSystem.class), result)
    }

    def "consumes result when closing"() {
        when:
        resultSet.close()

        then:
        resultSet.isClosed()
        1 * result.consume()
    }

    def "consumes result only once despite multiple close calls"() {
        when:
        5.times { resultSet.close() }

        then:
        resultSet.isClosed()
        1 * result.consume()
    }

    def "fails to close cursor if result consumption goes wrong"() {
        given:
        def driverException = new RuntimeException("oopsie")
        result.consume() >> { throw driverException }

        when:
        resultSet.close()

        then:
        !resultSet.isClosed()
        def exception = thrown(SQLException)
        exception.message == "cannot close result set"
        exception.cause == driverException
    }
}
