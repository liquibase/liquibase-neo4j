package liquibase.ext.neo4j.e2e

import liquibase.command.core.SnapshotReferenceCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec
import liquibase.ext.neo4j.structure.Label
import org.neo4j.driver.SessionConfig
import spock.lang.Requires

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.database.KernelVersion.V4_4_0
import static liquibase.ext.neo4j.e2e.SnapshotSupport.execute
import static liquibase.ext.neo4j.e2e.SnapshotSupport.jdbcUrlFor
import static liquibase.ext.neo4j.e2e.SnapshotSupport.referenceCommand

class SnapshotReferenceCommandIT extends Neo4jContainerSpec {

    @Requires({ neo4jVersion() >= V4_4_0 && enterpriseEdition() })
    def "snapshotReference snapshots the selected reference database"() {
        given:
        def selectedReferenceDatabase = "snapshotreferencecommand"
        queryRunner.recreateDatabase(selectedReferenceDatabase)
        queryRunner.run("CREATE (:`Invoice`)", [:], SessionConfig.forDatabase("neo4j"))
        queryRunner.run("CREATE (:`Product`)", [:], SessionConfig.forDatabase(selectedReferenceDatabase))
        queryRunner.run("CREATE (:`Supplier`)", [:], SessionConfig.forDatabase(selectedReferenceDatabase))

        when:
        def execution = execute(referenceCommand(
                SnapshotReferenceCommandStep.COMMAND_NAME,
                jdbcUrlFor(jdbcUrl(), selectedReferenceDatabase),
                PASSWORD))

        then: "snapshotReference writes the readable text snapshot to command output"
        execution.output.startsWith("Database snapshot for jdbc:neo4j:")
        execution.output.contains("Database type: Neo4j")
        execution.output.contains("Included types:")
        !execution.output.trim().startsWith("{")

        and: "the reference URL database is reported as the snapshot catalog"
        execution.output.contains("Catalog: ${selectedReferenceDatabase}")
        !execution.output.contains("Catalog: neo4j")

        and: "the output lists labels from the selected reference database only"
        execution.output.contains("${Label.name}:")
        execution.output.contains("Product")
        execution.output.contains("Supplier")
        !execution.output.contains("Invoice")

        cleanup:
        queryRunner?.dropDatabase(selectedReferenceDatabase)
    }
}
