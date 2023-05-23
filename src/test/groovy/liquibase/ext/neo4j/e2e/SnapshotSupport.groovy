package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.command.core.helpers.PreCompareCommandStep
import liquibase.command.core.helpers.ReferenceDbUrlConnectionCommandStep
import liquibase.diff.DiffResult
import liquibase.ext.neo4j.structure.Label
import liquibase.snapshot.DatabaseSnapshot
import liquibase.structure.DatabaseObject
import liquibase.structure.core.Catalog
import org.yaml.snakeyaml.Yaml

import java.nio.charset.StandardCharsets

class SnapshotSupport {

    static CommandScope referenceCommand(String[] commandName, String referenceUrl, String password) {
        return new CommandScope(commandName)
                .addArgumentValue(ReferenceDbUrlConnectionCommandStep.REFERENCE_URL_ARG, referenceUrl)
                .addArgumentValue(ReferenceDbUrlConnectionCommandStep.REFERENCE_USERNAME_ARG, "neo4j")
                .addArgumentValue(ReferenceDbUrlConnectionCommandStep.REFERENCE_PASSWORD_ARG, password)
    }

    static CommandScope targetCommand(String[] commandName, String jdbcUrl, String password) {
        return new CommandScope(commandName)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, jdbcUrl)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, password)
    }

    static CommandScope compareCommand(String[] commandName, String jdbcUrl, String password, String targetDatabase, String referenceDatabase, String diffTypes = "labels") {
        return targetCommand(commandName, jdbcUrl, password)
                .addArgumentValue(ReferenceDbUrlConnectionCommandStep.REFERENCE_URL_ARG, jdbcUrl)
                .addArgumentValue(ReferenceDbUrlConnectionCommandStep.REFERENCE_USERNAME_ARG, "neo4j")
                .addArgumentValue(ReferenceDbUrlConnectionCommandStep.REFERENCE_PASSWORD_ARG, password)
                .addArgumentValue(PreCompareCommandStep.SCHEMAS_ARG, targetDatabase)
                .addArgumentValue(PreCompareCommandStep.REFERENCE_SCHEMAS_ARG, referenceDatabase)
                .addArgumentValue(PreCompareCommandStep.DIFF_TYPES_ARG, diffTypes)
    }

    static Map<String, ?> snapshotReport(String report) {
        return new Yaml().load(snapshotPayload(report))["snapshot"] as Map<String, ?>
    }

    static Map<String, ?> snapshotObjects(String report) {
        return snapshotReport(report)["objects"] as Map<String, ?>
    }

    static def execute(CommandScope command) {
        def buffer = new ByteArrayOutputStream()
        def stream = new PrintStream(buffer)
        command.setOutput(stream)
        def results = command.execute()
        stream.flush()
        return [results: results, output: buffer.toString(StandardCharsets.UTF_8)]
    }

    static String jdbcUrlFor(String jdbcUrl, String database) {
        return "${jdbcUrl}?database=${database}"
    }

    static List<String> catalogNames(DatabaseSnapshot snapshot) {
        return snapshot.get(Catalog.class).collect { it.name }.findAll { it != null }.sort()
    }

    static List<String> labelNames(DatabaseSnapshot snapshot) {
        return snapshot.get(Label.class).collect { it.name }.sort()
    }

    static List<String> labelNames(Map<String, ?> objects) {
        return (objects[Label.name] ?: []).collect { it["label"]["value"] }.sort()
    }

    static List<String> missingLabelNames(DiffResult diffResult) {
        return diffResult.getMissingObjects(Label.class).collect { it.name }.sort()
    }

    static List<String> unexpectedLabelNames(DiffResult diffResult) {
        return diffResult.getUnexpectedObjects(Label.class).collect { it.name }.sort()
    }

    static List<String> changedLabelNames(DiffResult diffResult) {
        return diffResult.getChangedObjects(Label.class).keySet().collect { it.name }.sort()
    }

    static Map<String, List<String>> labelDiff(DiffResult diffResult) {
        return [
                missing   : missingLabelNames(diffResult),
                unexpected: unexpectedLabelNames(diffResult),
                changed   : changedLabelNames(diffResult)
        ]
    }

    static List<String> missingObjectTypeNames(DiffResult diffResult) {
        return objectTypeNames(diffResult.missingObjects)
    }

    static List<String> unexpectedObjectTypeNames(DiffResult diffResult) {
        return objectTypeNames(diffResult.unexpectedObjects)
    }

    static List<String> changedObjectTypeNames(DiffResult diffResult) {
        return objectTypeNames(diffResult.changedObjects.keySet())
    }

    static Map<String, List<String>> objectTypeDiff(DiffResult diffResult) {
        return [
                missing   : missingObjectTypeNames(diffResult),
                unexpected: unexpectedObjectTypeNames(diffResult),
                changed   : changedObjectTypeNames(diffResult)
        ]
    }

    static List<Map<String, String>> schemaComparisons(DiffResult diffResult) {
        return diffResult.compareControl.schemaComparisons.collect {
            [
                    referenceCatalog : it.referenceSchema.catalogName,
                    referenceSchema  : it.referenceSchema.schemaName,
                    comparisonCatalog: it.comparisonSchema.catalogName,
                    comparisonSchema : it.comparisonSchema.schemaName
            ]
        }
    }

    private static List<String> objectTypeNames(Collection<? extends DatabaseObject> objects) {
        return objects.collect { it.class.name }.unique().sort()
    }

    private static String snapshotPayload(String report) {
        int jsonIndex = report.indexOf("{\n")
        if (jsonIndex >= 0) {
            String candidate = report.substring(jsonIndex)
            if (candidate.trim().startsWith("{") && candidate.contains("\"snapshot\"")) {
                return candidate
            }
        }
        jsonIndex = report.indexOf("{\"snapshot\"")
        if (jsonIndex >= 0) {
            return report.substring(jsonIndex)
        }
        int yamlIndex = report.indexOf("snapshot:")
        if (yamlIndex >= 0) {
            return report.substring(yamlIndex)
        }
        return report
    }
}
