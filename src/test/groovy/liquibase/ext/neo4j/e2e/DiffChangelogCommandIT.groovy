package liquibase.ext.neo4j.e2e

import liquibase.command.core.DiffChangelogCommandStep
import liquibase.command.core.DiffCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec
import liquibase.ext.neo4j.structure.Label
import org.neo4j.driver.SessionConfig
import spock.lang.Requires

import java.nio.file.Files

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.database.KernelVersion.V4_4_0
import static liquibase.ext.neo4j.e2e.SnapshotSupport.catalogNames
import static liquibase.ext.neo4j.e2e.SnapshotSupport.compareCommand
import static liquibase.ext.neo4j.e2e.SnapshotSupport.execute
import static liquibase.ext.neo4j.e2e.SnapshotSupport.labelDiff
import static liquibase.ext.neo4j.e2e.SnapshotSupport.labelNames
import static liquibase.ext.neo4j.e2e.SnapshotSupport.objectTypeDiff
import static liquibase.ext.neo4j.e2e.SnapshotSupport.schemaComparisons

class DiffChangelogCommandIT extends Neo4jContainerSpec {

    @Requires({ neo4jVersion() >= V4_4_0 && enterpriseEdition() })
    def "diffChangelog uses schemas and referenceSchemas as Neo4j database names"() {
        given:
        def referenceDatabase = "reference"
        def targetDatabase = "target"
        def changeLogDirectory = Files.createTempDirectory("liquibase-neo4j-diff-changelog")
        def changeLogFile = changeLogDirectory.resolve("diff-changelog.xml")
        queryRunner.recreateDatabase(referenceDatabase)
        queryRunner.recreateDatabase(targetDatabase)
        queryRunner.run("CREATE (:`Session`)", [:], SessionConfig.forDatabase("neo4j"))
        queryRunner.run("CREATE (:`Customer`)", [:], SessionConfig.forDatabase(referenceDatabase))
        queryRunner.run("CREATE (:`Invoice`)", [:], SessionConfig.forDatabase(referenceDatabase))
        queryRunner.run("CREATE (:`Customer`)", [:], SessionConfig.forDatabase(targetDatabase))
        queryRunner.run("CREATE (:`Payment`)", [:], SessionConfig.forDatabase(targetDatabase))

        when:
        def execution = execute(compareCommand(DiffChangelogCommandStep.COMMAND_NAME, jdbcUrl(), PASSWORD, targetDatabase, referenceDatabase)
                .addArgumentValue(DiffChangelogCommandStep.CHANGELOG_FILE_ARG, changeLogFile.toString())
                .addArgumentValue(DiffChangelogCommandStep.AUTHOR_ARG, "liquibase"))
        def diffResult = execution.results.getResult(DiffCommandStep.DIFF_RESULT)

        then: "--referenceSchemas is the source side, --schemas is the target/comparison side"
        diffResult.comparedTypes == ([Label] as Set)
        schemaComparisons(diffResult) == [[
                referenceCatalog : referenceDatabase,
                referenceSchema  : referenceDatabase,
                comparisonCatalog: targetDatabase,
                comparisonSchema : targetDatabase
        ]]
        catalogNames(diffResult.referenceSnapshot) == [referenceDatabase]
        catalogNames(diffResult.comparisonSnapshot) == [targetDatabase]
        labelNames(diffResult.referenceSnapshot) == ["Customer", "Invoice"]
        labelNames(diffResult.comparisonSnapshot) == ["Customer", "Payment"]

        and: "diffChangelog computes the same missing/unexpected/changed buckets as diff"
        objectTypeDiff(diffResult) == [
                missing   : [Label.name],
                unexpected: [Label.name],
                changed   : []
        ]
        labelDiff(diffResult) == [
                missing   : ["Invoice"],
                unexpected: ["Payment"],
                changed   : []
        ]
        !diffResult.areEqual()

        and: "diffChangelog writes changelog output, not the TXT diff report"
        execution.output.empty

        and: "Neo4j labels are snapshot/diff objects here, but no label ChangeGenerator writes changesets"
        !Files.exists(changeLogFile)

        cleanup:
        if (changeLogFile != null) {
            Files.deleteIfExists(changeLogFile)
        }
        if (changeLogDirectory != null) {
            Files.deleteIfExists(changeLogDirectory)
        }
        queryRunner?.dropDatabase(referenceDatabase)
        queryRunner?.dropDatabase(targetDatabase)
    }
}
