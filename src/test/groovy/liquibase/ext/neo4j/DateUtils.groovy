package liquibase.ext.neo4j


import java.time.Duration
import java.time.LocalDate
import java.time.ZonedDateTime
import java.time.temporal.TemporalUnit

import static java.time.ZonedDateTime.now
import static liquibase.ext.neo4j.lockservice.Neo4jLockServiceIT.TIMEZONE

class DateUtils {

    static Date date(Integer year, Integer month, Integer day) {
        Date.from(LocalDate.of(year, month, day).atStartOfDay().atZone(TIMEZONE).toInstant())
    }

    static Date date(ZonedDateTime time) {
        Date.from(time.toInstant())
    }

    static Date nowMinus(long amount, TemporalUnit unit) {
        return nowMinus(Duration.of(amount, unit))
    }

    static Date nowMinus(Duration duration) {
        Date.from((now(TIMEZONE) - duration).toInstant())
    }

}
