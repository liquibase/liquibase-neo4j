package liquibase.ext.neo4j.database.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

public enum SupportedUrlPredicate implements Predicate<String> {

    IS_SUPPORTED_URL;

    private static final Set<String> ACCEPTED_SCHEMES = new HashSet<>(Arrays.asList(
            "bolt", "bolt+s", "bolt+ssc", "bolt+routing", "neo4j", "neo4j+s", "neo4j+ssc"
    ));

    @Override
    public boolean test(String url) {
        if (url == null) {
            return false;
        }
        url = normalizeUri(url);
        if (url.isEmpty()) {
            return false;
        }
        try {
            URI uri = new URI(url);
            return ACCEPTED_SCHEMES.contains(uri.getScheme());
        } catch (URISyntaxException e) {
            return false;
        }
    }

    public static String normalizeUri(String url) {
        return url.trim().replaceFirst("^jdbc:neo4j:", "");
    }
}
