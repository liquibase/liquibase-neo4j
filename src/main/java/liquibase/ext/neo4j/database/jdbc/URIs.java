package liquibase.ext.neo4j.database.jdbc;

import java.net.URI;
import java.net.URISyntaxException;

class URIs {
    public static URI stripQueryString(URI uri) {
        try {
            return new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, uri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("could not strip query string from URI: %s", uri), e);
        }
    }
}
