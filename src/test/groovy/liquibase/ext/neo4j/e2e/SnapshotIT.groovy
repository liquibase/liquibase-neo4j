package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.SnapshotCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class SnapshotIT extends Neo4jContainerSpec {

    def setup() {
        queryRunner.run("CREATE BTREE INDEX node_btree_index FOR (f:Foo) ON f.bar")
        queryRunner.run("CREATE RANGE INDEX node_range_index FOR (f:Foo) ON f.baz")
        queryRunner.run("CREATE POINT INDEX node_point_index FOR (f:Foo) ON (f.location)")
        queryRunner.run("CREATE TEXT INDEX node_text_index FOR (f:Foo) ON (f.description)")
        queryRunner.run("CREATE FULLTEXT INDEX node_full_text_index FOR (f:Foo|Bar) ON EACH [f.bar, f.description]")
        queryRunner.run("CREATE (f:Foo {" +
                "bar: 'qix', " +
                "baz: 42, " +
                "location: point({latitude: toFloat('37.89'), longitude: toFloat('41.12')}), " +
                "description: 'lorem ipsum'" +
                "})-[:FIGHTERS]->()")
        System.out = stdout
        System.err = stderr
    }

    // TODO: 4.x test for btree
    // TODO: 5.x test for vector
    def "records snapshots"() {
        given:
        def command = new CommandScope(SnapshotCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(SnapshotCommandStep.SNAPSHOT_FORMAT_ARG, "json")
                .setOutput(System.out)

        when:
        command.execute()

        then:
        true
    }
}
