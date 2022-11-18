package liquibase.ext.neo4j.database.jdbc


import org.neo4j.driver.Result
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSetMetaData
import java.sql.SQLException

class Neo4jResultSet_getColumnName_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSetMetaData resultSetMetadata

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSetMetadata = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets column name"() {
        when:
        result.keys() >> ["k", "o"]

        then:
        resultSetMetadata.getColumnName(1) == "k"
        resultSetMetadata.getColumnName(2) == "o"
    }

    def "fails getting column name if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        result.keys() >> { throw driverException }

        when:
        resultSetMetadata.getColumnName(1)

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot access result keys'
        exception.cause == driverException
    }

    def "fails getting column name if index is out of bound error occurred"() {
        given:
        result.keys() >> ['k', 'o']

        when:
        resultSetMetadata.getColumnName(3)

        then:
        def exception = thrown(SQLException)
        exception.message == 'invalid column index 3, index must be between 1 (incl.) and 2 (incl.)'
    }
}
