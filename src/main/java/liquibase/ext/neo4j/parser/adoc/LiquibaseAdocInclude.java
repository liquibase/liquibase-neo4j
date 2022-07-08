package liquibase.ext.neo4j.parser.adoc;

import liquibase.resource.ResourceAccessor;
import org.asciidoctor.ast.Document;
import org.asciidoctor.extension.IncludeProcessor;
import org.asciidoctor.extension.PreprocessorReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.stream.Collectors;

public class LiquibaseAdocInclude extends IncludeProcessor {

    private final ResourceAccessor resourceAccessor;

    public LiquibaseAdocInclude(ResourceAccessor resourceAccessor) {
        this.resourceAccessor = resourceAccessor;
    }

    @Override
    public boolean handles(String target) {
        return true;
    }

    @Override
    public void process(Document document, PreprocessorReader reader, String target, Map<String, Object> attributes) {
        try {
            reader.pushInclude(
                readContents(resourceAccessor, target),
                    target,
                    target,
                    0,
                    attributes
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String readContents(ResourceAccessor resourceAccessor, String physicalChangeLogLocation) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAccessor.openStream(null, physicalChangeLogLocation)))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }
}
