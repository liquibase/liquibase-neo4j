package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Values
import org.neo4j.driver.types.TypeSystem
import spock.lang.Specification

import java.sql.ResultSet

class Neo4jResultSet_wasNull_Test extends Specification {

    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        this.result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, Mock(TypeSystem.class), this.result)
    }

    def "is null when last-accessed named column does not exist"() {
        given:
        def row = Mock(Record.class)
        row.get("foobar") >> Values.NULL
        this.result.next() >> row

        when:
        resultSet.next()
        resultSet.getString("foobar")

        and:
        def result = resultSet.wasNull()

        then:
        result
    }

    def "is null when last-accessed indexed column does not exist"() {
        given:
        def row = Mock(Record.class)
        row.get(42-1) >> Values.NULL // JDBC indices are 1-based, driver's are 0-based
        result.next() >> row

        when:
        resultSet.next()
        resultSet.getBoolean(42)

        and:
        def result = resultSet.wasNull()

        then:
        result
    }
}
