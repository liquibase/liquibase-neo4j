package liquibase.ext.neo4j

import liquibase.ext.neo4j.lockservice.Neo4jLockServiceTest

import java.time.Duration
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.time.temporal.TemporalUnit

class DateUtils {

    static Date date(Integer year, Integer month, Integer day) {
        Date.from(LocalDate.of(year, month, day).atStartOfDay().atZone(Neo4jLockServiceTest.TIMEZONE).toInstant())
    }

    static Date date(ZonedDateTime time) {
        Date.from(time.toInstant())
    }

    static Date nowMinus(long amount, TemporalUnit unit) {
        return nowMinus(Duration.of(amount, unit))
    }

    static Date nowMinus(Duration duration) {
        Date.from((LocalDateTime.now() - duration).atZone(Neo4jLockServiceTest.TIMEZONE).toInstant())
    }

}
