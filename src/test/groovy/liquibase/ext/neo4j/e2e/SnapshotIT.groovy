package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.SnapshotCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class SnapshotIT extends Neo4jContainerSpec {

    def setup() {
        queryRunner.run("CREATE BTREE INDEX FOR (f:Foo) ON f.bar")
        queryRunner.run("CREATE (f:Foo {bar: 'qix'})-[:FIGHTERS]->()")
        System.out = stdout
        System.err = stderr
    }

    def "records snapshots"() {
        given:
        def command = new CommandScope(SnapshotCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .setOutput(System.out)

        when:
        command.execute()

        then:
        true
    }
}
