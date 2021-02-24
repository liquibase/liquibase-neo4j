package liquibase.ext.neo4j.lockservice;

import java.util.Locale;
import java.util.function.Predicate;

public class Exceptions {

    public static Predicate<Exception> messageContaining(String message) {
        return (e) -> e.getMessage().toLowerCase(Locale.ENGLISH).contains(message.toLowerCase(Locale.ENGLISH));
    }

    public static void ignoring(Predicate<Exception> predicate, ThrowingRunnable statement) {
        try {
            statement.run();
        } catch (Exception e) {
            if (!predicate.test(e)) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }
}
