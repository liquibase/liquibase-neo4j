package liquibase.ext.neo4j;

import liquibase.Scope;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.ChangeLogParseException;
import liquibase.parser.ChangeLogParser;
import liquibase.parser.core.xml.XMLChangeLogSAXParser;
import liquibase.resource.ResourceAccessor;
import liquibase.serializer.ChangeLogSerializer;
import liquibase.serializer.core.json.JsonChangeLogSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;

/**
 * ChangeLogFormatConverter is a small utility to convert XML change log examples to other formats
 * This is manually invoked, mostly for documentation purposes
 */
public class ChangeLogFormatConverter {

    static class Serialization {
        private final ChangeLogSerializer serializer;
        private final String targetPath;

        public Serialization(ChangeLogSerializer serializer, String targetPath) {
            this.serializer = serializer;
            this.targetPath = targetPath;
        }

        void serialize(DatabaseChangeLog changeLog) throws IOException {
            try (OutputStream outputStream = Files.newOutputStream(Paths.get(targetPath))) {
                serializer.write(changeLog.getChangeSets(), outputStream);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new RuntimeException("expected 2 arguments (XML change log file classpath location and output format but got " + args.length);
        }
        String xmlChangeLogPath = args[0];
        String format = args[1];

        DatabaseChangeLog changeLog = parseChangeLog(xmlChangeLogPath);
        resolveSerializer(xmlChangeLogPath, format).serialize(changeLog);
    }

    private static DatabaseChangeLog parseChangeLog(String xmlChangeLogPath) throws IOException, ChangeLogParseException {
        ResourceAccessor resourceAccessor = Scope.getCurrentScope().getResourceAccessor();
        if (resourceAccessor.get(xmlChangeLogPath) instanceof ResourceAccessor.NotFoundResource) {
            throw new RuntimeException("could not find readable XML change log file in classpath: " + xmlChangeLogPath);
        }

        ChangeLogParser parser = new XMLChangeLogSAXParser();
        return parser.parse(xmlChangeLogPath, new ChangeLogParameters(), resourceAccessor);
    }

    private static Serialization resolveSerializer(String inputPath, String format) {
        switch (format.toLowerCase(Locale.ENGLISH)) {
            case "json":
                return new Serialization(new JsonChangeLogSerializer(), inputPath.replaceAll("\\.xml$", ".json"));
            case "yaml":
                return new Serialization(new JsonChangeLogSerializer(), inputPath.replaceAll("\\.xml$", ".yaml"));
        }
        throw new RuntimeException("unsupported output format: " + format);
    }
}
