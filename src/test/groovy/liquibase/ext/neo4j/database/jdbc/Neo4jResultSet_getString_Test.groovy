package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Values
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException
import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

class Neo4jResultSet_getString_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets named string value"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value("bar")
        row.get("bar") >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        resultSet.getString("foo") == "bar"
        resultSet.getString("bar") == null
    }

    def "gets indexed string value"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value("bar")
        row.get(44 - 1) >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        resultSet.getString(42) == "bar"
        resultSet.getString(44) == null
    }

    def "gets named string value out of coercible value"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value(input)

        and:
        result.next() >> row
        resultSet.next()

        expect:
        resultSet.getString("foo") == expected

        where:
        input                                       | expected
        null                                        | null
        true                                        | "true"
        false                                       | "false"
        42                                          | "42"
        42.0f                                       | "42.0"
        LocalDate.of(1986, 4, 3)                    | "1986-04-03"
        OffsetTime.of(12, 12, 0, 0, ZoneOffset.UTC) | "12:12Z"
        LocalTime.of(12, 12)                        | "12:12"
        LocalDateTime.of(2019, 3, 4, 12, 12)        | "2019-03-04T12:12"
        Duration.of(12, ChronoUnit.SECONDS)         | "P0M0DT12S"
    }

    def "gets indexed string value out of coercible value"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value(input)

        and:
        result.next() >> row
        resultSet.next()

        expect:
        resultSet.getString(42) == expected

        where:
        input                                       | expected
        null                                        | null
        true                                        | "true"
        false                                       | "false"
        42                                          | "42"
        42.0f                                       | "42.0"
        LocalDate.of(1986, 4, 3)                    | "1986-04-03"
        OffsetTime.of(12, 12, 0, 0, ZoneOffset.UTC) | "12:12Z"
        LocalTime.of(12, 12)                        | "12:12"
        LocalDateTime.of(2019, 3, 4, 12, 12)        | "2019-03-04T12:12"
        Duration.of(12, ChronoUnit.SECONDS)         | "P0M0DT12S"
    }

    def "fails to coerce named uncoercible value to string"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getString("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column named foo to expected type'
    }

    def "fails to coerce indexed uncoercible value to string"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getString(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column 42 to expected type'
    }

    def "fails to get named string value if access error occurred"() {
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

    def "fails to get indexed string value if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        def row = Mock(Record.class)
        row.get(42 - 1) >> { throw driverException }
        result.next() >> row

        when:
        resultSet.next()

        and:
        resultSet.getString(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot get column 42'
        exception.cause == driverException
    }

    def "fails to get named string value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getString("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }

    def "fails to get indexed string value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getString(42)

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
