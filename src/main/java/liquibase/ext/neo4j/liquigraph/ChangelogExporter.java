package liquibase.ext.neo4j.liquigraph;

import liquibase.change.core.RawSQLChange;
import liquibase.changelog.ChangeSet;
import liquibase.serializer.ChangeLogSerializer;
import liquibase.serializer.ChangeLogSerializerFactory;
import org.jetbrains.annotations.NotNull;
import org.liquigraph.core.io.xml.ChangelogParser;
import org.liquigraph.core.io.xml.ChangelogPreprocessor;
import org.liquigraph.core.io.xml.ClassLoaderChangelogLoader;
import org.liquigraph.core.io.xml.ImportResolver;
import org.liquigraph.core.io.xml.XmlSchemaValidator;
import org.liquigraph.core.model.Changeset;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Poc for exporting Liquigraph changelog to Liquibase
 *  TODO ==> Post a condition + Check Liquigraph in classpath....
 */
public class ChangelogExporter {

    public static void main(String[] args) throws IOException {
        ChangeLogSerializerFactory changeLogSerializerFactory = ChangeLogSerializerFactory.getInstance();

        ChangeLogSerializer changeLogSerializer = changeLogSerializerFactory.getSerializer("xml");

        Collection<Changeset> liquigraphChangeSets = getLiquigraphChangeSets();

        List<ChangeSet> changeSets = convertChangeSets(liquigraphChangeSets);
        changeLogSerializer.write(changeSets, System.out);
    }

    @NotNull
    private static Collection<Changeset> getLiquigraphChangeSets() {
        ChangelogParser changelogParser = new ChangelogParser(new XmlSchemaValidator(), new ChangelogPreprocessor(new ImportResolver()));
        return changelogParser.parse(ClassLoaderChangelogLoader.currentThreadContextClassLoader(), "liquigraph-changelog.xml");
    }

    @NotNull
    private static List<ChangeSet> convertChangeSets(Collection<Changeset> liquigraphChangeSets) {
        return liquigraphChangeSets.stream().map(ChangelogExporter::from).collect(Collectors.toList());
    }

    @NotNull
    private static ChangeSet from(Changeset changeset) {
        ChangeSet liquibaseChangeSet = new ChangeSet(changeset.getId(), changeset.getAuthor(), changeset.isRunAlways(),
                                                    changeset.isRunOnChange(), null, String.join(",",
                                                    changeset.getExecutionsContexts()),null, null);
        changeset.getQueries().stream().map(RawSQLChange::new).forEach(liquibaseChangeSet::addChange);
        return liquibaseChangeSet;
    }

}
