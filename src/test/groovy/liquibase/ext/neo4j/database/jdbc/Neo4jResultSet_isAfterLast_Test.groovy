package liquibase.ext.neo4j.database.jdbc


import org.neo4j.driver.Result
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException

class Neo4jResultSet_isAfterLast_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "is after last when iteration is done"() {
        given:
        result.hasNext() >> false

        expect:
        resultSet.isAfterLast()
    }

    def "is not initially after last"() {
        given:
        result.hasNext() >> true

        expect:
        !resultSet.isAfterLast()
    }

    def "fails determining if result set is after last when it is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.isAfterLast()

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
