package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.internal.types.InternalTypeSystem
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException

class Neo4jResultSet_isBeforeFirst_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "is initially before first"() {
        expect:
        resultSet.isBeforeFirst()
    }

    def "is not before first once iteration has started"() {
        given:
        result.next() >> Mock(Record.class)

        when:
        resultSet.next()

        then:
        !resultSet.isBeforeFirst()
    }

    def "fails determining if result set is before first when it is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.isBeforeFirst()

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }
}
