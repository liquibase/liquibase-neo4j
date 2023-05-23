package liquibase.ext.neo4j.command;

import liquibase.command.CommandOverride;
import liquibase.command.CommandResultsBuilder;
import liquibase.command.CommandScope;
import liquibase.command.core.DiffCommandStep;
import liquibase.command.core.helpers.PreCompareCommandStep;
import liquibase.command.providers.ReferenceDatabase;
import liquibase.database.Database;
import liquibase.diff.DiffResult;
import liquibase.diff.compare.CompareControl;
import liquibase.exception.DatabaseException;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.snapshot.InvalidExampleException;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Catalog;

import java.util.LinkedHashSet;
import java.util.Set;

@CommandOverride(override = DiffCommandStep.class)
public class Neo4jDiffCommandStep extends DiffCommandStep {

    @Override
    public DiffResult createDiffResult(CommandResultsBuilder resultsBuilder)
            throws DatabaseException, InvalidExampleException {
        removeCatalogsFromDefaultNeo4jDiff(resultsBuilder);
        return super.createDiffResult(resultsBuilder);
    }

    private static void removeCatalogsFromDefaultNeo4jDiff(CommandResultsBuilder resultsBuilder) {
        CommandScope commandScope = resultsBuilder.getCommandScope();

        if (!isNeo4jToNeo4jDiff(commandScope)) {
            return;
        }
        if (hasExplicitTypeSelection(commandScope)) {
            return;
        }

        CompareControl compareControl = (CompareControl) resultsBuilder.getResult(
                PreCompareCommandStep.COMPARE_CONTROL_RESULT.getName()
        );
        if (compareControl == null || !compareControl.getComparedTypes().contains(Catalog.class)) {
            return;
        }

        Set<Class<? extends DatabaseObject>> comparedTypes =
                new LinkedHashSet<>(compareControl.getComparedTypes());
        comparedTypes.remove(Catalog.class);

        if (comparedTypes.isEmpty()) {
            return;
        }

        CompareControl replacement = new CompareControl(
                compareControl.getSchemaComparisons(),
                comparedTypes
        );
        resultsBuilder.addResult(PreCompareCommandStep.COMPARE_CONTROL_RESULT, replacement);
    }

    private static boolean isNeo4jToNeo4jDiff(CommandScope commandScope) {
        Database targetDatabase = (Database) commandScope.getDependency(Database.class);
        Database referenceDatabase = (Database) commandScope.getDependency(ReferenceDatabase.class);

        return targetDatabase instanceof Neo4jDatabase
               && referenceDatabase instanceof Neo4jDatabase;
    }

    private static boolean hasExplicitTypeSelection(CommandScope commandScope) {
        if (commandScope.getArgumentValue(PreCompareCommandStep.COMPARE_CONTROL_ARG) != null) {
            return true;
        }
        if (commandScope.getArgumentValue(PreCompareCommandStep.SNAPSHOT_TYPES_ARG) != null) {
            return true;
        }

        return commandScope.getConfiguredValue(PreCompareCommandStep.DIFF_TYPES_ARG).found();
    }
}
