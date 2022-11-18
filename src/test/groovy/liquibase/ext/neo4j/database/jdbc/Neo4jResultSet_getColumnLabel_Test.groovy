package liquibase.ext.neo4j.database.jdbc


import org.neo4j.driver.Result
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSetMetaData
import java.sql.SQLException

class Neo4jResultSet_getColumnLabel_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSetMetaData resultSetMetadata

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSetMetadata = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets column label"() {
        when:
        result.keys() >> ["k", "o"]

        then:
        resultSetMetadata.getColumnLabel(1) == "k"
        resultSetMetadata.getColumnLabel(2) == "o"
    }

    def "fails getting column label if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        result.keys() >> { throw driverException }

        when:
        resultSetMetadata.getColumnLabel(1)

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot access result keys'
        exception.cause == driverException
    }

    def "fails getting column label if index is out of bound error occurred"() {
        given:
        result.keys() >> ['k']

        when:
        resultSetMetadata.getColumnLabel(2)

        then:
        def exception = thrown(SQLException)
        exception.message == 'invalid column index 2, index must be between 1 (incl.) and 1 (incl.)'
    }
}
