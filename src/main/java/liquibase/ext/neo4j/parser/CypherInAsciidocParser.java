package liquibase.ext.neo4j.parser;

import liquibase.change.core.RawSQLChange;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.ChangeLogParseException;
import liquibase.ext.neo4j.parser.adoc.LiquibaseAdocInclude;
import liquibase.parser.ChangeLogParser;
import liquibase.resource.ResourceAccessor;
import org.asciidoctor.Asciidoctor;
import org.asciidoctor.Options;
import org.asciidoctor.SafeMode;
import org.asciidoctor.ast.Block;
import org.asciidoctor.ast.Document;
import org.asciidoctor.ast.StructuralNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class CypherInAsciidocParser implements ChangeLogParser {

    private final Asciidoctor asciidocParser;

    public CypherInAsciidocParser() {
        Asciidoctor asciidocParser = Asciidoctor.Factory.create();
        asciidocParser.javaExtensionRegistry().includeProcessor(new LiquibaseAdocInclude());
        this.asciidocParser = asciidocParser;
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
            String content = readContents(resourceAccessor, physicalChangeLogLocation);
            Document document = asciidocParser.load(
                    content,
                    Options.builder().safe(SafeMode.SAFE).build()
            );
            Map<Object, Object> selectors = new HashMap<>(2);
            selectors.put("context", ":listing");
            selectors.put("style", "source");
            List<StructuralNode> nodes = document.findBy(selectors);
            for (StructuralNode node : nodes) {
                Block block = (Block) node;
                if (!isCypherBlock(node)) {
                    continue;
                }

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

    private static boolean isCypherBlock(StructuralNode node) {
        Object language = node.getAttribute("language");
        if (language == null) {
            return false;
        }
        return "cypher".equals(((String) language).toLowerCase(Locale.ENGLISH));
    }
}
