package liquibase.ext.neo4j.database.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

public enum SupportedJdbcUrl implements Predicate<String> {

    IS_SUPPORTED_JDBC_URL;

    private static final String PREFIX = "jdbc:neo4j:";

    private static final Set<String> ACCEPTED_SCHEMES = new HashSet<>(Arrays.asList(
            "bolt", "bolt+s", "bolt+ssc", "bolt+routing", "neo4j", "neo4j+s", "neo4j+ssc"
    ));

    @Override
    public boolean test(String rawUri) {
        if (rawUri == null || !rawUri.startsWith(PREFIX)) {
            return false;
        }
        rawUri = normalizeUri(rawUri);
        if (rawUri.isEmpty()) {
            return false;
        }
        try {
            URI uri = new URI(rawUri);
            return ACCEPTED_SCHEMES.contains(uri.getScheme());
        } catch (URISyntaxException e) {
            return false;
        }
    }

    public static String normalizeUri(String rawUri) {
        return normalizeUri(rawUri, Function.identity());
    }

    public static String normalizeUri(String rawUri, Function<URI, URI> postProcess) {
        try {
            URI uri = new URI(rawUri.substring(PREFIX.length()));
            uri = postProcess.apply(uri);
            return uri.toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
