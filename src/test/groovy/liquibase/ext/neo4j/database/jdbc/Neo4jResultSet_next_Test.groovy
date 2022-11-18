package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.exceptions.NoSuchRecordException
import org.neo4j.driver.types.TypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException

class Neo4jResultSet_next_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, Mock(TypeSystem.class), result)
    }

    def "moves cursor with next until no more records are emitted"() {
        given:
        result.next() >>>
                [Mock(Record.class), Mock(Record.class)] >>
                { throw new NoSuchRecordException("") }

        when:
        def results = [resultSet.next(), resultSet.next(), resultSet.next(), resultSet.next()]

        then:
        results == [true, true, false, false]
    }

    def "fails to move cursor if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.next()

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }

    def "fails to move cursor if something went wrong"() {
        given:
        def driverException = new RuntimeException("oopsie")
        result.next() >> { throw driverException }

        when:
        resultSet.next()

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot move to next row"
        exception.cause == driverException
    }
}
