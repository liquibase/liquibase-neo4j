package liquibase.ext.neo4j.e2e

import liquibase.command.core.DiffCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec
import liquibase.ext.neo4j.structure.GraphType
import liquibase.ext.neo4j.structure.Label
import org.neo4j.driver.SessionConfig
import spock.lang.Requires

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.database.KernelVersion.V4_4_0
import static liquibase.ext.neo4j.database.KernelVersion.V2026_02_0
import static liquibase.ext.neo4j.e2e.SnapshotSupport.catalogNames
import static liquibase.ext.neo4j.e2e.SnapshotSupport.compareCommand
import static liquibase.ext.neo4j.e2e.SnapshotSupport.execute
import static liquibase.ext.neo4j.e2e.SnapshotSupport.labelDiff
import static liquibase.ext.neo4j.e2e.SnapshotSupport.labelNames
import static liquibase.ext.neo4j.e2e.SnapshotSupport.objectTypeDiff
import static liquibase.ext.neo4j.e2e.SnapshotSupport.schemaComparisons

class DiffCommandIT extends Neo4jContainerSpec {

    @Requires({ neo4jVersion() >= V4_4_0 && enterpriseEdition() })
    def "diff uses schemas and referenceSchemas as Neo4j database names"() {
        given:
        def referenceDatabase = "reference"
        def targetDatabase = "target"
        queryRunner.recreateDatabase(referenceDatabase)
        queryRunner.recreateDatabase(targetDatabase)
        queryRunner.run("CREATE (:`Invoice`)", [:], SessionConfig.forDatabase("neo4j"))
        queryRunner.run("CREATE (:`Person`)", [:], SessionConfig.forDatabase(referenceDatabase))
        queryRunner.run("CREATE (:`Movie`)", [:], SessionConfig.forDatabase(referenceDatabase))
        queryRunner.run("CREATE (:`Person`)", [:], SessionConfig.forDatabase(targetDatabase))
        queryRunner.run("CREATE (:`Order`)", [:], SessionConfig.forDatabase(targetDatabase))

        when:
        def execution = execute(compareCommand(DiffCommandStep.COMMAND_NAME, jdbcUrl(), PASSWORD, targetDatabase, referenceDatabase))
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
        labelNames(diffResult.referenceSnapshot) == ["Movie", "Person"]
        labelNames(diffResult.comparisonSnapshot) == ["Order", "Person"]

        and: "the DiffResult uses the same buckets printed by the text diff report"
        objectTypeDiff(diffResult) == [
                missing   : [Label.name],
                unexpected: [Label.name],
                changed   : []
        ]
        labelDiff(diffResult) == [
                missing   : ["Movie"],
                unexpected: ["Order"],
                changed   : []
        ]
        !diffResult.areEqual()

        and: "the diff command's default TXT output shows the same missing/unexpected/changed label sections"
        execution.output.contains("Compared Schemas: ${referenceDatabase} -> ${targetDatabase}")
        execution.output.contains("Missing Label(s): \n     Movie")
        execution.output.contains("Unexpected Label(s): \n     Order")
        execution.output.contains("Changed Label(s): NONE")
        !execution.output.contains("Person")
        !execution.output.contains("Invoice")

        cleanup:
        queryRunner?.dropDatabase(referenceDatabase)
        queryRunner?.dropDatabase(targetDatabase)
    }

    @Requires({ neo4jVersion() >= V2026_02_0 && enterpriseEdition() })
    def "diff compares graph types across mapped Neo4j database names"() {
        given:
        def referenceDatabase = "referencegraphtype"
        def targetDatabase = "targetgraphtype"
        def graphType = """
            CYPHER 25 ALTER CURRENT GRAPH TYPE SET {
                (:Person => {identityKey :: STRING}),
                CONSTRAINT graph_person_key FOR (n:Person =>) REQUIRE (n.identityKey) IS KEY
            }
        """
        queryRunner.recreateDatabase(referenceDatabase)
        queryRunner.recreateDatabase(targetDatabase)
        queryRunner.run(graphType, [:], SessionConfig.forDatabase(referenceDatabase))
        queryRunner.run(graphType, [:], SessionConfig.forDatabase(targetDatabase))

        when:
        def execution = execute(compareCommand(DiffCommandStep.COMMAND_NAME, jdbcUrl(), PASSWORD, targetDatabase, referenceDatabase, "graphTypes"))
        def diffResult = execution.results.getResult(DiffCommandStep.DIFF_RESULT)

        then:
        diffResult.comparedTypes == ([GraphType] as Set)
        diffResult.getMissingObjects(GraphType).empty
        diffResult.getUnexpectedObjects(GraphType).empty
        diffResult.getChangedObjects(GraphType).isEmpty()
        diffResult.areEqual()

        and:
        execution.output.contains("Missing Graph Type(s): NONE")
        execution.output.contains("Unexpected Graph Type(s): NONE")
        execution.output.contains("Changed Graph Type(s): NONE")

        cleanup:
        queryRunner?.dropDatabase(referenceDatabase)
        queryRunner?.dropDatabase(targetDatabase)
    }
}
