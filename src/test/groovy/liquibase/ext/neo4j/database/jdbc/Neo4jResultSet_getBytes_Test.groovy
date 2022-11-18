package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Values
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException

class Neo4jResultSet_getBytes_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets named bytes value"() {
        given:
        def row = Mock(Record.class)
        byte[] value = [(byte) 0x1, (byte) 0x2]
        row.get("foo") >> Values.value(value)
        row.get("bar") >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        byte[] result = [(byte) 0x1, (byte) 0x2]
        resultSet.getBytes("foo") == result
        resultSet.getBytes("bar") == null
    }

    def "gets indexed bytes value"() {
        given:
        def row = Mock(Record.class)
        byte[] value = [(byte) 0x1, (byte) 0x2]
        row.get(42 - 1) >> Values.value(value)
        row.get(44 - 1) >> Values.NULL
        result.next() >> row

        when:
        resultSet.next()

        then:
        byte[] result = [(byte) 0x1, (byte) 0x2]
        resultSet.getBytes(42) == result
        resultSet.getBytes(44) == null
    }

    def "fails to coerce named uncoercible value to bytes"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value(true)

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getBytes("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column named foo to expected type'
    }

    def "fails to coerce indexed uncoercible value to bytes"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value(true)

        and:
        result.next() >> row
        resultSet.next()

        when:
        resultSet.getBytes(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not safely cast column 42 to expected type'
    }

    def "fails getting named bytes value if access error occurred"() {
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

    def "fails getting indexed bytes value if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        def row = Mock(Record.class)
        row.get(42 - 1) >> { throw driverException }
        result.next() >> row

        when:
        resultSet.next()

        and:
        resultSet.getBytes(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot get column 42'
        exception.cause == driverException
    }

    def "fails to get named bytes value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getBytes("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }

    def "fails to get indexed bytes value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getBytes(42)

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
