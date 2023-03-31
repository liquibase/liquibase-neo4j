package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Values
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Time
import java.time.LocalTime

class Neo4jResultSet_getTime_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets named time value"() {
        given:
        def row = Mock(Record.class)
        def localTime = LocalTime.of(12, 12, 0)
        row.get("foo") >> Values.value(localTime)
        row.get("bar") >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        resultSet.getTime("foo") == Time.valueOf(localTime)
        resultSet.getTime("bar") == null
    }

    def "gets indexed time value"() {
        given:
        def localTime = LocalTime.of(12, 12, 0)
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value(localTime)
        row.get(44 - 1) >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        resultSet.getTime(42) == Time.valueOf(localTime)
        resultSet.getTime(44) == null
    }

    def "fails to coerce named uncoercible value to time"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getTime("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column named foo to expected type'
    }

    def "fails to coerce indexed uncoercible value to time"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getTime(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column 42 to expected type'
    }

    def "fails getting named time value if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        def row = Mock(Record.class)
        row.get("foo") >> { throw driverException }
        result.next() >> row

        when:
        resultSet.next()

        and:
        resultSet.getString("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot get column "foo"'
        exception.cause == driverException
    }

    def "fails getting indexed time value if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        def row = Mock(Record.class)
        row.get(42 - 1) >> { throw driverException }
        result.next() >> row

        when:
        resultSet.next()

        and:
        resultSet.getTime(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot get column 42'
        exception.cause == driverException
    }

    def "fails to get named time value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getTime("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }

    def "fails to get indexed time value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getTime(42)

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
