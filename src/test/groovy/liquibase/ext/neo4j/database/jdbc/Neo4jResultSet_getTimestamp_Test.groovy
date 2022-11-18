package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Values
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneId

class Neo4jResultSet_getTimestamp_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets named timestamp value"() {
        given:
        def row = Mock(Record.class)
        def localDateTime = LocalDateTime.of(2021, 10, 5, 12, 12, 0)
        row.get("foo") >> Values.value(localDateTime)
        row.get("bar") >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()


        then:
        // have to rely on default system TZ since it may not resolve to the same from 1 machine to another
        def epoch = localDateTime.atZone(ZoneId.systemDefault()).toEpochSecond()
        resultSet.getTimestamp("foo") == new Timestamp(epoch * 1000)
        resultSet.getTimestamp("bar") == null
    }

    def "gets indexed timestamp value"() {
        given:
        def row = Mock(Record.class)
        def localDateTime = LocalDateTime.of(2021, 10, 5, 12, 12, 0)
        row.get(42 - 1) >> Values.value(localDateTime)
        row.get(44 - 1) >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        // have to rely on default system TZ since it may not resolve to the same from 1 machine to another
        def epoch = localDateTime.atZone(ZoneId.systemDefault()).toEpochSecond()
        resultSet.getTimestamp(42) == new Timestamp(epoch * 1000)
        resultSet.getTimestamp(44) == null
    }

    def "fails to coerce named uncoercible value to timestamp"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getTimestamp("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column named foo to expected type'
    }

    def "fails to coerce indexed uncoercible value to timestamp"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getTimestamp(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column 42 to expected type'
    }

    def "fails getting named timestamp value if access error occurred"() {
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

    def "fails getting indexed timestamp value if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        def row = Mock(Record.class)
        row.get(42 - 1) >> { throw driverException }
        result.next() >> row

        when:
        resultSet.next()

        and:
        resultSet.getTimestamp(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot get column 42'
        exception.cause == driverException
    }

    def "fails to get named timestamp value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getTimestamp("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }

    def "fails to get indexed timestamp value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getTimestamp(42)

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
