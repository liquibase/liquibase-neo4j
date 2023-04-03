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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class AsciidocChangeLogParser implements ChangeLogParser {

    private final Asciidoctor asciidocParser;

    public AsciidocChangeLogParser() {
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
        changeLog.setChangeLogParameters(changeLogParameters);

        try {
            List<StructuralNode> nodes = findCodeListings(physicalChangeLogLocation, resourceAccessor);
            for (StructuralNode node : nodes) {
                Block block = (Block) node;
                if (!targetsLiquibase(node)) {
                    continue;
                }
                ChangeSet changeSet = convertToChangeSet(physicalChangeLogLocation, changeLog, block);
                changeLog.addChangeSet(changeSet);
            }
        } catch (IOException e) {
            throw new ChangeLogParseException(e);
        }
        return changeLog;
    }

    private List<StructuralNode> findCodeListings(String physicalChangeLogLocation, ResourceAccessor resourceAccessor) throws IOException {
        String content = readContents(resourceAccessor, physicalChangeLogLocation);
        Document document = asciidocParser.load(
                content,
                Options.builder().safe(SafeMode.SAFE).build()
        );
        Map<Object, Object> selectors = new HashMap<>(2);
        selectors.put("context", ":listing");
        selectors.put("style", "source");
        List<StructuralNode> nodes = document.findBy(selectors);
        return nodes;
    }

    private static String readContents(ResourceAccessor resourceAccessor, String physicalChangeLogLocation) throws IOException {
        try (InputStream stream = resourceAccessor.openStream(null, physicalChangeLogLocation)) {
            if (stream == null) {
                throw new IOException(String.format("could not find change log resource %s", physicalChangeLogLocation));
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        }
    }

    private static ChangeSet convertToChangeSet(String location, DatabaseChangeLog changeLog, Block block) throws ChangeLogParseException {
        validateLiquibaseBlock(block);
        ChangeSet changeSet = new ChangeSet(
                (String) block.getAttribute("id"),
                (String) block.getAttribute("author"),
                Boolean.parseBoolean((String) block.getAttribute("runAlways", "false")),
                Boolean.parseBoolean((String) block.getAttribute("runOnChange", "false")),
                location,
                (String) block.getAttribute("contexts", ""),
                (String) block.getAttribute("dbms", ""),
                Boolean.parseBoolean((String) block.getAttribute("runInTransaction", "true")),
                changeLog
        );
        changeSet.addChange(convertToChange(block.getSource()));
        return changeSet;
    }

    private static void validateLiquibaseBlock(Block block) throws ChangeLogParseException {
        if (block.getAttribute("id") == null) {
            throw new ChangeLogParseException("Liquibase code listing ID attribute must be set");
        }
        if (block.getAttribute("author") == null) {
            throw new ChangeLogParseException("Liquibase code listing author attribute must be set");
        }
        Object runOnChange = block.getAttribute("runOnChange");
        if (runOnChange != null && !isValidBooleanString(runOnChange)) {
            throw new ChangeLogParseException(
                    String.format("Liquibase code listing runOnChange attribute must be set to either \"true\" or \"false\", found: %s", runOnChange));
        }
        Object runAlways = block.getAttribute("runAlways");
        if (runAlways != null && !isValidBooleanString(runAlways)) {
            throw new ChangeLogParseException(
                    String.format("Liquibase code listing runAlways attribute must be set to either \"true\" or \"false\", found: %s", runAlways));
        }
        Object runInTransaction = block.getAttribute("runInTransaction");
        if (runInTransaction != null && !isValidBooleanString(runInTransaction)) {
            throw new ChangeLogParseException(
                    String.format("Liquibase code listing runInTransaction attribute must be set to either \"true\" or \"false\", found: %s", runInTransaction));
        }
    }

    private static boolean isValidBooleanString(Object rawValue) {
        if (!(rawValue instanceof String)) {
            return false;
        }
        String value = (String) rawValue;
        return value.toLowerCase(Locale.ENGLISH).equals("true")
                || value.toLowerCase(Locale.ENGLISH).equals("false");
    }

    private static RawSQLChange convertToChange(String source) {
        RawSQLChange change = new RawSQLChange();
        change.setSql(source);
        return change;
    }

    private static boolean targetsLiquibase(StructuralNode node) {
        Object target = node.getAttribute("target");
        if (target == null) {
            return false;
        }
        return "liquibase".equals(((String) target).toLowerCase(Locale.ENGLISH));
    }
}
