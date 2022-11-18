package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSetMetaData
import java.sql.SQLException

class Neo4jResultSet_getColumnCount_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSetMetaData resultSetMetadata

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSetMetadata = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets column count"() {
        when:
        result.keys() >> ["k", "i", "n", "g"]

        then:
        resultSetMetadata.getColumnCount() == 4
    }

    def "fails getting column count if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        result.keys() >> { throw driverException }

        when:
        resultSetMetadata.getColumnCount()

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot get column count'
        exception.cause == driverException
    }
}
