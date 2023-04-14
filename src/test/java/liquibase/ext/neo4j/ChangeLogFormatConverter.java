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
import liquibase.serializer.core.yaml.YamlChangeLogSerializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Locale;

/**
 * ChangeLogFormatConverter is a small utility to convert XML change log examples to other formats
 * This is manually invoked, mostly for documentation purposes
 */
public class ChangeLogFormatConverter {

    static class Serialization {
        private final ChangeLogSerializer serializer;
        private final String format;

        public Serialization(ChangeLogSerializer serializer, String format) {
            this.serializer = serializer;
            this.format = format;
        }

        void serialize(DatabaseChangeLog changeLog) throws IOException {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            serializer.write(changeLog.getChangeSets(), output);
            System.out.format("%s output:\n\n", format);
            System.out.println(output.toString("UTF-8"));
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
        File file = new File(inputPath);
        String fileName = file.getName();
        switch (format.toLowerCase(Locale.ENGLISH)) {
            case "json":
                return new Serialization(new JsonChangeLogSerializer(), "json");
            case "yaml":
                return new Serialization(new YamlChangeLogSerializer(), "yaml");
        }
        throw new RuntimeException("unsupported output format: " + format);
    }
}
