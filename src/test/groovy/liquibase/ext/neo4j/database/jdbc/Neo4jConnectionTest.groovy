package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Transaction
import spock.lang.Specification

import java.sql.Connection
import java.sql.SQLException

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT
import static java.sql.ResultSet.CONCUR_READ_ONLY
import static java.sql.ResultSet.CONCUR_UPDATABLE
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT
import static java.sql.ResultSet.TYPE_FORWARD_ONLY
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE
import static java.sql.ResultSet.TYPE_SCROLL_SENSITIVE
import static liquibase.ext.neo4j.database.jdbc.ResultSets.rsConcurrencyName
import static liquibase.ext.neo4j.database.jdbc.ResultSets.rsHoldabilityName
import static liquibase.ext.neo4j.database.jdbc.ResultSets.rsTypeName

class Neo4jConnectionTest extends Specification {

    Neo4jConnection connection

    def setup() {
        connection = new Neo4jConnection("jdbc:neo4j:neo4j://example.com", new Properties())
    }

    def "new connections are in autocommit mode by default"() {
        when:
        connection = new Neo4jConnection("jdbc:neo4j:neo4j://example.com", new Properties())

        then:
        connection.getAutoCommit()
    }

    def "creates a simple statement"() {
        when:
        def statement = connection.createStatement()

        then:
        statement != null
    }

    def "creates a prepared statement"() {
        when:
        def statement = connection.prepareStatement("RETURN 42")

        then:
        statement != null
    }

    def "creates a simple statement with supported result set parameters"() {
        when:
        connection.setAutoCommit(autocommit)
        def statement = connection.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, holdability)

        then:
        statement != null

        where:
        autocommit | holdability
        true       | HOLD_CURSORS_OVER_COMMIT
        false      | CLOSE_CURSORS_AT_COMMIT
    }

    def "creates a prepared statement with supported result set parameters"() {
        when:
        connection.setAutoCommit(autocommit)
        def statement = connection.prepareStatement("RETURN 42", TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, holdability)

        then:
        statement != null

        where:
        autocommit | holdability
        true       | HOLD_CURSORS_OVER_COMMIT
        false      | CLOSE_CURSORS_AT_COMMIT
    }

    def "fails creating a simple statement with unsupported parameters"() {
        when:
        connection.setAutoCommit(autocommit)
        connection.createStatement(type, concurrency, holdability)

        then:
        def exception = thrown(RuntimeException.class)
        exception.message.trim() == """
Expected the following result set parameters: 
 - type: ResultSet.TYPE_FORWARD_ONLY
 - concurrency: ResultSet.CONCUR_READ_ONLY
 - holdability: ResultSet.${rsHoldabilityName(autocommit ? HOLD_CURSORS_OVER_COMMIT : CLOSE_CURSORS_AT_COMMIT)}
but got:
 - type: ResultSet.${rsTypeName(type)}
 - concurrency: ResultSet.${rsConcurrencyName(concurrency)}
 - holdability: ResultSet.${rsHoldabilityName(holdability)}
""".trim()

        where:
        autocommit | type                    | concurrency      | holdability
        true       | TYPE_FORWARD_ONLY       | CONCUR_READ_ONLY | CLOSE_CURSORS_AT_COMMIT
        true       | TYPE_FORWARD_ONLY       | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        true       | TYPE_FORWARD_ONLY       | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
        true       | TYPE_SCROLL_INSENSITIVE | CONCUR_READ_ONLY | HOLD_CURSORS_OVER_COMMIT
        true       | TYPE_SCROLL_INSENSITIVE | CONCUR_READ_ONLY | CLOSE_CURSORS_AT_COMMIT
        true       | TYPE_SCROLL_INSENSITIVE | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        true       | TYPE_SCROLL_INSENSITIVE | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
        true       | TYPE_SCROLL_SENSITIVE   | CONCUR_READ_ONLY | HOLD_CURSORS_OVER_COMMIT
        true       | TYPE_SCROLL_SENSITIVE   | CONCUR_READ_ONLY | CLOSE_CURSORS_AT_COMMIT
        true       | TYPE_SCROLL_SENSITIVE   | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        true       | TYPE_SCROLL_SENSITIVE   | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
        false      | TYPE_FORWARD_ONLY       | CONCUR_READ_ONLY | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_FORWARD_ONLY       | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_FORWARD_ONLY       | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
        false      | TYPE_SCROLL_INSENSITIVE | CONCUR_READ_ONLY | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_SCROLL_INSENSITIVE | CONCUR_READ_ONLY | CLOSE_CURSORS_AT_COMMIT
        false      | TYPE_SCROLL_INSENSITIVE | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_SCROLL_INSENSITIVE | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
        false      | TYPE_SCROLL_SENSITIVE   | CONCUR_READ_ONLY | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_SCROLL_SENSITIVE   | CONCUR_READ_ONLY | CLOSE_CURSORS_AT_COMMIT
        false      | TYPE_SCROLL_SENSITIVE   | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_SCROLL_SENSITIVE   | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
    }

    def "fails creating a prepared statement with unsupported parameters"() {
        when:
        connection.setAutoCommit(autocommit)
        connection.createStatement(type, concurrency, holdability)

        then:
        def exception = thrown(RuntimeException.class)
        exception.message.trim() == """
Expected the following result set parameters: 
 - type: ResultSet.TYPE_FORWARD_ONLY
 - concurrency: ResultSet.CONCUR_READ_ONLY
 - holdability: ResultSet.${rsHoldabilityName(autocommit ? HOLD_CURSORS_OVER_COMMIT : CLOSE_CURSORS_AT_COMMIT)}
but got:
 - type: ResultSet.${rsTypeName(type)}
 - concurrency: ResultSet.${rsConcurrencyName(concurrency)}
 - holdability: ResultSet.${rsHoldabilityName(holdability)}
""".trim()

        where:
        autocommit | type                    | concurrency      | holdability
        true       | TYPE_FORWARD_ONLY       | CONCUR_READ_ONLY | CLOSE_CURSORS_AT_COMMIT
        true       | TYPE_FORWARD_ONLY       | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        true       | TYPE_FORWARD_ONLY       | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
        true       | TYPE_SCROLL_INSENSITIVE | CONCUR_READ_ONLY | HOLD_CURSORS_OVER_COMMIT
        true       | TYPE_SCROLL_INSENSITIVE | CONCUR_READ_ONLY | CLOSE_CURSORS_AT_COMMIT
        true       | TYPE_SCROLL_INSENSITIVE | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        true       | TYPE_SCROLL_INSENSITIVE | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
        true       | TYPE_SCROLL_SENSITIVE   | CONCUR_READ_ONLY | HOLD_CURSORS_OVER_COMMIT
        true       | TYPE_SCROLL_SENSITIVE   | CONCUR_READ_ONLY | CLOSE_CURSORS_AT_COMMIT
        true       | TYPE_SCROLL_SENSITIVE   | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        true       | TYPE_SCROLL_SENSITIVE   | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
        false      | TYPE_FORWARD_ONLY       | CONCUR_READ_ONLY | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_FORWARD_ONLY       | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_FORWARD_ONLY       | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
        false      | TYPE_SCROLL_INSENSITIVE | CONCUR_READ_ONLY | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_SCROLL_INSENSITIVE | CONCUR_READ_ONLY | CLOSE_CURSORS_AT_COMMIT
        false      | TYPE_SCROLL_INSENSITIVE | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_SCROLL_INSENSITIVE | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
        false      | TYPE_SCROLL_SENSITIVE   | CONCUR_READ_ONLY | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_SCROLL_SENSITIVE   | CONCUR_READ_ONLY | CLOSE_CURSORS_AT_COMMIT
        false      | TYPE_SCROLL_SENSITIVE   | CONCUR_UPDATABLE | HOLD_CURSORS_OVER_COMMIT
        false      | TYPE_SCROLL_SENSITIVE   | CONCUR_UPDATABLE | CLOSE_CURSORS_AT_COMMIT
    }

    def "fails to commit in autocommit mode"() {
        when:
        connection.commit()

        then:
        def exception = thrown(SQLException.class)
        exception.message == "the connection is in auto-commit mode. Explicit commit is prohibited"
    }

    def "fails to rollback in autocommit mode"() {
        when:
        connection.rollback()

        then:
        def exception = thrown(SQLException.class)
        exception.message == "the connection is in auto-commit mode. Explicit rollback is prohibited"
    }
}
