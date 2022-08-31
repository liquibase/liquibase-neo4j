package liquibase.ext.neo4j.lockservice

import spock.lang.Specification

import java.time.Duration
import java.time.Instant

class ConditionCheckSchedulerTest extends Specification {

    Duration timeout

    ConditionCheckScheduler scheduler

    def alwaysFalse = { -> false }

    def alwaysTrue = { -> true }

    def alwaysFailing = { -> throw new Exception("nope") }

    def mustBeTrue = { it }

    def setup() {
        timeout = Duration.ofMillis(100)
        scheduler = new ConditionCheckScheduler(timeout)
    }

    def "returns default value when timeout period is over"() {
        given:
        def start = Instant.now()

        when:
        def result = scheduler.scheduleCheckWithFixedDelay(alwaysFalse, mustBeTrue, false, Duration.ofMillis(20))

        then:
        !result
        def value = Duration.between(start, Instant.now())
        value >= timeout
    }

    def "returns default value when timeout period is over even if supplier fails"() {
        given:
        def start = Instant.now()

        when:
        def result = scheduler.scheduleCheckWithFixedDelay(alwaysFailing, mustBeTrue, false, Duration.ofMillis(20))

        then:
        !result
        def value = Duration.between(start, Instant.now())
        value >= timeout
    }

    def "returns before timeout if condition is met"() {
        given:
        def start = Instant.now()

        when:
        def result = scheduler.scheduleCheckWithFixedDelay(alwaysTrue, mustBeTrue, false, Duration.ofMillis(20))

        then:
        result
        def value = Duration.between(start, Instant.now())
        value < timeout
    }

    def "fails if delay is larger or equal than timeout"() {
        given:
        def interval = timeout.plusDays(1)

        when:
        scheduler.scheduleCheckWithFixedDelay(alwaysTrue, mustBeTrue, false, interval)

        then:
        def exception = thrown(IllegalArgumentException.class)
        exception.message == "delay PT24H0.1S should be strictly less than the configured timeout PT0.1S"
    }

    def "fails and returns last seen exception"() {
        given:
        def interval = timeout.minusMillis(80)

        when:
        scheduler.scheduleCheckWithFixedDelay(() -> {throw new RuntimeException("oopsie")}, alwaysTrue, false, interval)

        then:
        def cause = scheduler.getLastException().getCause()
        cause instanceof RuntimeException
        cause.getMessage() == "oopsie"
    }
}
