package liquibase.ext.neo4j.lockservice;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

public class ConditionCheckScheduler {

    private final ScheduledExecutorService scheduledExecutorService;

    private final Duration timeout;

    public ConditionCheckScheduler(Duration timeout) {
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
        this.timeout = timeout;
    }

    public <T> T scheduleCheckWithFixedDelay(Callable<T> valueSupplier, Predicate<T> check, T defaultValue, Duration delay) {
        if (delay.compareTo(this.timeout) >= 0) {
            throw new IllegalArgumentException(String.format("delay %s should be strictly less than the configured timeout %s", delay, this.timeout));
        }

        Instant waitingStarted = Instant.now();
        Duration waitedFor;
        long delayInNanos = 0L;
        while ((waitedFor = Duration.between(waitingStarted, Instant.now())).compareTo(timeout) < 0) {
            try {
                T currentResult = scheduledExecutorService.schedule(valueSupplier, delayInNanos, TimeUnit.NANOSECONDS)
                        .get(timeout.minus(waitedFor).toNanos(), TimeUnit.NANOSECONDS);
                if (check.test(currentResult)) {
                    return currentResult;
                }
            } catch (ExecutionException | InterruptedException | TimeoutException ignored) {
            }
            delayInNanos = delay.toNanos();
        }
        return defaultValue;
    }
}