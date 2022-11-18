package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Values
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException
import java.time.*
import java.time.temporal.ChronoUnit

class Neo4jResultSet_getBoolean_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets named boolean value"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value(false)
        row.get("bar") >> Values.value(true)
        row.get("baz") >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        !resultSet.getBoolean("foo")
        resultSet.getBoolean("bar")
        !resultSet.getBoolean("baz")
    }

    def "gets indexed boolean value"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value(false)
        row.get(44 - 1) >> Values.value(true)
        row.get(46 - 1) >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        !resultSet.getBoolean(42)
        resultSet.getBoolean(44)
        !resultSet.getBoolean(46)
    }

    def "gets named boolean value out of coercible value"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value(input)

        and:
        result.next() >> row
        resultSet.next()

        expect:
        resultSet.getBoolean("foo") == expected

        where:
        input | expected
        0     | false
        1     | true
        "0"   | false
        "1"   | true
    }

    def "gets indexed boolean value out of coercible value"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value(input)

        and:
        result.next() >> row
        resultSet.next()

        expect:
        resultSet.getBoolean(42) == expected

        where:
        input | expected
        0     | false
        1     | true
        "0"   | false
        "1"   | true
    }

    def "fails to coerce named uncoercible value to boolean"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getBoolean("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column named foo to expected type'
    }

    def "fails to coerce indexed uncoercible value to boolean"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getBoolean(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column 42 to expected type'
    }

    def "fails getting named boolean value if access error occurred"() {
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

    def "fails getting indexed boolean value if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        def row = Mock(Record.class)
        row.get(42 - 1) >> { throw driverException }
        result.next() >> row

        when:
        resultSet.next()

        and:
        resultSet.getBoolean(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot get column 42'
        exception.cause == driverException
    }

    def "fails to get named boolean value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getBoolean("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }

    def "fails to get indexed boolean value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getBoolean(42)

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
