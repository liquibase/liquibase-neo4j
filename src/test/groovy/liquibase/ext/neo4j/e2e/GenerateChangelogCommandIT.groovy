package liquibase.ext.neo4j.e2e

import liquibase.command.core.DiffCommandStep
import liquibase.command.core.GenerateChangelogCommandStep
import liquibase.command.core.helpers.PreCompareCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec
import liquibase.ext.neo4j.structure.Label
import org.neo4j.driver.SessionConfig
import spock.lang.Requires

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.database.KernelVersion.V4_0_0
import static liquibase.ext.neo4j.e2e.SnapshotSupport.catalogNames
import static liquibase.ext.neo4j.e2e.SnapshotSupport.execute
import static liquibase.ext.neo4j.e2e.SnapshotSupport.labelDiff
import static liquibase.ext.neo4j.e2e.SnapshotSupport.labelNames
import static liquibase.ext.neo4j.e2e.SnapshotSupport.schemaComparisons
import static liquibase.ext.neo4j.e2e.SnapshotSupport.targetCommand

class GenerateChangelogCommandIT extends Neo4jContainerSpec {

    @Requires({ neo4jVersion() >= V4_0_0 && enterpriseEdition() })
    def "generateChangelog uses schemas as Neo4j database names"() {
        given:
        def selectedDatabase = "generatechangelogcommand"
        queryRunner.recreateDatabase(selectedDatabase)
        queryRunner.run("CREATE (:`Invoice`)", [:], SessionConfig.forDatabase("neo4j"))
        queryRunner.run("CREATE (:`Person`)", [:], SessionConfig.forDatabase(selectedDatabase))
        queryRunner.run("CREATE (:`Movie`)", [:], SessionConfig.forDatabase(selectedDatabase))

        when:
        def execution = execute(targetCommand(GenerateChangelogCommandStep.COMMAND_NAME, jdbcUrl(), PASSWORD)
                .addArgumentValue(PreCompareCommandStep.SCHEMAS_ARG, selectedDatabase)
                .addArgumentValue(PreCompareCommandStep.DIFF_TYPES_ARG, "labels")
                .addArgumentValue(GenerateChangelogCommandStep.AUTHOR_ARG, "liquibase"))
        def diffResult = execution.results.getResult(DiffCommandStep.DIFF_RESULT)
        def generatedChangeLog = new groovy.xml.XmlSlurper(false, false).parseText(execution.output)

        then: "--schemas selects the Neo4j database snapshotted by generateChangelog"
        diffResult.comparedTypes == ([Label] as Set)
        schemaComparisons(diffResult) == [[
                referenceCatalog : selectedDatabase,
                referenceSchema  : selectedDatabase,
                comparisonCatalog: selectedDatabase,
                comparisonSchema : selectedDatabase
        ]]
        catalogNames(diffResult.referenceSnapshot) == [selectedDatabase]
        labelNames(diffResult.referenceSnapshot) == ["Movie", "Person"]
        labelNames(diffResult.comparisonSnapshot) == []

        and: "generateChangelog represents existing objects as missing from an empty comparison snapshot"
        labelDiff(diffResult) == [
                missing   : ["Movie", "Person"],
                unexpected: [],
                changed   : []
        ]
        !diffResult.areEqual()

        and: "the command serializes a Liquibase changelog document, but label diffs do not produce changesets"
        generatedChangeLog.name() == "databaseChangeLog"
        generatedChangeLog.changeSet.size() == 0

        and: "labels from the connection's default database are ignored"
        !execution.output.contains("Invoice")

        cleanup:
        queryRunner?.dropDatabase(selectedDatabase)
    }
}
