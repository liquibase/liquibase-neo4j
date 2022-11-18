package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Values
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.Date
import java.sql.ResultSet
import java.sql.SQLException
import java.time.LocalDate
import java.time.ZoneId

class Neo4jResultSet_getDate_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets named date value"() {
        given:
        def row = Mock(Record.class)
        def localDate = LocalDate.of(2002, 1, 1)
        row.get("foo") >> Values.value(localDate)
        row.get("bar") >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        // have to rely on default system TZ since it may not resolve to the same from 1 machine to another
        def epoch = localDate.atStartOfDay(ZoneId.systemDefault()).toEpochSecond()
        resultSet.getDate("foo") == new Date(epoch * 1000)
        resultSet.getDate("bar") == null
    }

    def "gets indexed date value"() {
        given:
        def row = Mock(Record.class)
        def localDate = LocalDate.of(2002, 1, 1)
        row.get(42 - 1) >> Values.value(localDate)
        row.get(44 - 1) >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        // have to rely on default system TZ since it may not resolve to the same from 1 machine to another
        def epoch = localDate.atStartOfDay(ZoneId.systemDefault()).toEpochSecond()
        resultSet.getDate(42) == new Date(epoch * 1000)
        resultSet.getDate(44) == null
    }

    def "fails to coerce named uncoercible value to date"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getDate("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column named foo to expected type'
    }

    def "fails to coerce indexed uncoercible value to date"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getDate(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column 42 to expected type'
    }

    def "fails getting named date value if access error occurred"() {
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

    def "fails getting indexed date value if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        def row = Mock(Record.class)
        row.get(42 - 1) >> { throw driverException }
        result.next() >> row

        when:
        resultSet.next()

        and:
        resultSet.getDate(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot get column 42'
        exception.cause == driverException
    }

    def "fails to get named date value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getDate("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }

    def "fails to get indexed date value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getDate(42)

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
