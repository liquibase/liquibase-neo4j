package liquibase.ext.neo4j.database.jdbc;

import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

class QueryStringParser {

    /**
     * Parses a given URI to extract its query string
     * <p>A value-less key (?key or &key) is considered equivalent to the same key with the value true (?key=true or
     * &key=true.</p>
     * <p>Both keys and values are URL-decoded</p>
     *
     * @param rawUri the raw URI to extract the query string from
     * @return a map of key and values
     */
    public static QueryString parseQueryString(String rawUri) {
        try {
            return new QueryString(parse(rawUri));
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(String.format("could not parse query string of URL %s", rawUri), e);
        }
    }

    private static Map<String, List<String>> parse(String rawUrl) throws Exception {
        URI uri = new URI(rawUrl);
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, List<String>> result = new HashMap<>();
        String[] parts = query.split("&");
        for (String rawKeyValue : parts) {
            String[] keyValue = rawKeyValue.split("=", 2);
            String key = URLDecoder.decode(keyValue[0], "UTF-8");
            String value = URLDecoder.decode(keyValue.length == 1 ? "true" : keyValue[1], "UTF-8");
            result.computeIfAbsent(key, (k) -> new ArrayList<>(1)).add(value);
        }
        return result;
    }

}
