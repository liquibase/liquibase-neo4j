package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Values
import spock.lang.Specification

import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

class Neo4jStatementTest extends Specification {

    def "stores boolean parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1")

        when:
        statement.setBoolean(1, true)

        then:
        statement.getParameters() == ["1": true]
    }

    def "stores byte parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1")

        when:
        statement.setByte(1, (byte) 1)

        then:
        statement.getParameters() == ["1": (byte) 1]
    }

    def "stores short parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1")

        when:
        statement.setShort(1, (short) 1)

        then:
        statement.getParameters() == ["1": (short) 1]
    }

    def "stores int parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1")

        when:
        statement.setInt(1, 1)

        then:
        statement.getParameters() == ["1": 1]
    }

    def "stores long parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1")

        when:
        statement.setLong(1, 1L)

        then:
        statement.getParameters() == ["1": 1L]
    }

    def "stores float parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1")

        when:
        statement.setFloat(1, 1.0F)

        then:
        statement.getParameters() == ["1": 1.0F]
    }

    def "stores double parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1")

        when:
        statement.setDouble(1, 1.0D)

        then:
        statement.getParameters() == ["1": 1.0D]
    }

    def "stores big decimal parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1")

        when:
        statement.setBigDecimal(1, BigDecimal.TEN)

        then:
        statement.getParameters() == ["1": BigDecimal.TEN]
    }

    def "stores object parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1, \$2, \$3, \$4")

        when:
        statement.setObject(1, [1])
        statement.setObject(2, ["one": 2])
        statement.setObject(3, Values.point(7203, 3, 4))
        statement.setObject(4, Values.point(4979, 5, 6))

        then:
        statement.getParameters() == [
                "1": [1],
                "2": ["one": 2],
                "3": Values.point(7203, 3, 4),
                "4": Values.point(4979, 5, 6)
        ]
    }

    def "stores date parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1")

        when:
        statement.setDate(1, new Date(1))

        then:
        statement.getParameters() == ["1": LocalDate.ofEpochDay(0)]
    }

    def "stores time parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1")

        when:
        statement.setTime(1, new Time(1000))


        then:
        def localTime = LocalTime.ofSecondOfDay(1 + (int) (Offsets.currentUtcOffsetMillis() / 1000))
        statement.getParameters() == ["1": localTime]
    }

    def "stores timestamp parameters"() {
        given:
        def statement = new Neo4jStatement(Mock(Neo4jConnection.class), "RETURN \$1")

        when:
        statement.setTimestamp(1, new Timestamp(1000))

        then:
        def localTime = LocalTime.ofSecondOfDay(1 + (int) (Offsets.currentUtcOffsetMillis() / 1000))
        statement.getParameters() == ["1": LocalDateTime.of(LocalDate.ofEpochDay(0), localTime)]
    }
}
