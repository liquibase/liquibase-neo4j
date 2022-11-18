package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException

class Neo4jResultSet_findColumn_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "finds column"() {
        given:
        result.keys() >> ["foo", "bar"]

        expect:
        resultSet.findColumn("foo") == 0
        resultSet.findColumn("bar") == 1
    }


    def "fails finding column if key does not exist"() {
        given:
        result.keys() >> ["foo", "bar"]

        when:
        resultSet.findColumn("baz")

        then:
        def exception = thrown(SQLException)
        exception.message == 'could not find column named baz, existing columns are: foo, bar'
    }


    def "fails finding column if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        result.keys() >> { throw driverException }

        when:
        resultSet.findColumn("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot access result keys'
        exception.cause == driverException
    }

    def "fails finding column if result set is already closed"() {
        given:
        result.keys() >> ["foo"]
        resultSet.close()

        when:
        resultSet.findColumn("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
