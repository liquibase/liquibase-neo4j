package liquibase.ext.neo4j.lockservice;

import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class Exceptions {

    public static Predicate<Exception> messageContaining(String message) {
        return (e) -> e.getMessage().toLowerCase(Locale.ENGLISH).contains(message.toLowerCase(Locale.ENGLISH));
    }

    public static void onThrow(ThrowingConsumer<Exception> action, ThrowingRunnable runner) {
        try {
            runner.run();
        } catch (Exception runnableException) {
            try {
                action.accept(runnableException);
            } catch (Exception consumerException) {
                runnableException.addSuppressed(consumerException);
                throw convertToRuntimeException(runnableException);
            }
        }
    }

    public static RuntimeException convertToRuntimeException(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new RuntimeException(e.getMessage(), e);
    }
}
