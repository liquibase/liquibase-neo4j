package liquibase.ext.neo4j.e2e

import liquibase.command.core.SnapshotCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec
import org.neo4j.driver.SessionConfig
import spock.lang.Requires

import static liquibase.ext.neo4j.DockerNeo4j.enterpriseEdition
import static liquibase.ext.neo4j.DockerNeo4j.neo4jVersion
import static liquibase.ext.neo4j.database.KernelVersion.V4_0_0
import static liquibase.ext.neo4j.e2e.SnapshotSupport.catalogNames
import static liquibase.ext.neo4j.e2e.SnapshotSupport.execute
import static liquibase.ext.neo4j.e2e.SnapshotSupport.labelNames
import static liquibase.ext.neo4j.e2e.SnapshotSupport.snapshotObjects
import static liquibase.ext.neo4j.e2e.SnapshotSupport.snapshotReport
import static liquibase.ext.neo4j.e2e.SnapshotSupport.targetCommand

class SnapshotCommandIT extends Neo4jContainerSpec {

    @Requires({ neo4jVersion() >= V4_0_0 && enterpriseEdition() })
    def "snapshot uses schemas as Neo4j database names"() {
        given:
        def selectedDatabase = "snapshotcommand"
        queryRunner.recreateDatabase(selectedDatabase)
        queryRunner.run("CREATE (:`Invoice`)", [:], SessionConfig.forDatabase("neo4j"))
        queryRunner.run("CREATE (:`Person`)", [:], SessionConfig.forDatabase(selectedDatabase))
        queryRunner.run("CREATE (:`Movie`)", [:], SessionConfig.forDatabase(selectedDatabase))

        when:
        def execution = execute(targetCommand(SnapshotCommandStep.COMMAND_NAME, jdbcUrl(), PASSWORD)
                .addArgumentValue(SnapshotCommandStep.SNAPSHOT_FORMAT_ARG, "json")
                .addArgumentValue(SnapshotCommandStep.SCHEMAS_ARG, selectedDatabase))
        def snapshot = execution.results.getResult("snapshot")
        def serializedSnapshot = snapshotReport(execution.output)
        def serializedObjects = snapshotObjects(execution.output)

        then: "--schemas selects the Neo4j database to snapshot"
        execution.results.getResult("statusCode") == 0
        catalogNames(snapshot) == [selectedDatabase]
        labelNames(snapshot) == ["Movie", "Person"]

        and: "the JSON snapshot output contains the same selected database labels"
        serializedSnapshot["database"]["shortName"] == "neo4j"
        labelNames(serializedObjects) == ["Movie", "Person"]

        and: "labels from the connection's default database are ignored"
        !labelNames(snapshot).contains("Invoice")
        !labelNames(serializedObjects).contains("Invoice")

        cleanup:
        queryRunner?.dropDatabase(selectedDatabase)
    }
}
