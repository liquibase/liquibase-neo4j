package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Values
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException

class Neo4jResultSet_getShort_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets named short value"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value(1)
        row.get("bar") >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        resultSet.getShort("foo") == (short) 1
        resultSet.getShort("bar") == (short) 0
    }

    def "gets indexed short value"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value(1)
        row.get(44 - 1) >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        resultSet.getShort(42) == (short) 1
        resultSet.getShort(44) == (short) 0
    }

    def "fails to coerce named out of range value to short"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value(Short.MAX_VALUE + 1)

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getShort("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column named foo to expected type: value is out of range'
    }

    def "fails to coerce indexed out of range value to short"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value(Short.MIN_VALUE - 1)

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getShort(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column 42 to expected type: value is out of range'
    }

    def "fails to coerce named uncoercible value to short"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getShort("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column named foo to expected type'
    }

    def "fails to coerce indexed uncoercible value to short"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value([1])

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getShort(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column 42 to expected type'
    }

    def "fails getting named short value if access error occurred"() {
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

    def "fails getting indexed short value if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        def row = Mock(Record.class)
        row.get(42 - 1) >> { throw driverException }
        result.next() >> row

        when:
        resultSet.next()

        and:
        resultSet.getShort(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot get column 42'
        exception.cause == driverException
    }

    def "fails to get named short value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getShort("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }

    def "fails to get indexed short value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getShort(42)

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
