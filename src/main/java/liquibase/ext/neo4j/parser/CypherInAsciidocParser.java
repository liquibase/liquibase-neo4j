package liquibase.ext.neo4j.parser;

import liquibase.change.core.RawSQLChange;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.ChangeLogParseException;
import liquibase.parser.ChangeLogParser;
import liquibase.parser.core.sql.SqlChangeLogParser;
import liquibase.resource.ResourceAccessor;
import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Options;
import org.asciidoctor.OptionsBuilder;
import org.asciidoctor.ast.Block;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.StructuralNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CypherInAsciidocParser implements ChangeLogParser {

    private final Asciidoctor asciidocParser;

    public CypherInAsciidocParser() {
        this.asciidocParser = Asciidoctor.Factory.create();
    }

    @Override
    public boolean supports(String changeLogFile, ResourceAccessor resourceAccessor) {
        return changeLogFile.endsWith(".asciidoc") || changeLogFile.endsWith(".adoc");
    }


    @Override
    public int getPriority() {
        return 1;
    }

    @Override
    public DatabaseChangeLog parse(String physicalChangeLogLocation, ChangeLogParameters changeLogParameters, ResourceAccessor resourceAccessor) throws ChangeLogParseException {
        DatabaseChangeLog changeLog = new DatabaseChangeLog();
        changeLog.setPhysicalFilePath(physicalChangeLogLocation);

        try {
            Document document = asciidocParser.load(readContents(resourceAccessor, physicalChangeLogLocation), Options.builder().build());
            Map<Object, Object> selectors = new HashMap<>(1);
            selectors.put("context", ":listing");
            List<StructuralNode> nodes = document.findBy(selectors);
            for (StructuralNode node : nodes) {
                Block block = (Block) node;
                RawSQLChange change = new RawSQLChange();
                change.setSql(String.join("\n", block.getLines()));
                ChangeSet changeSet = new ChangeSet(
                        (String) block.getAttribute("id"),
                        (String) block.getAttribute("author"),
                        (Boolean) block.getAttribute("runAlways", false),
                        (Boolean) block.getAttribute("runOnChange", false),
                        physicalChangeLogLocation,
                        (String) block.getAttribute("contexts", ""),
                        (String) block.getAttribute("dbms", ""),
                        changeLog
                );
                changeSet.addChange(change);
                changeLog.addChangeSet(changeSet);
            }
        } catch (IOException e) {
            throw new ChangeLogParseException(e);
        }

        return changeLog;
    }

    private static String readContents(ResourceAccessor resourceAccessor, String physicalChangeLogLocation) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resourceAccessor.openStream(null, physicalChangeLogLocation)))) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }
}
