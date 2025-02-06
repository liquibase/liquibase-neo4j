package liquibase.ext.neo4j.e2e

import liquibase.ChecksumVersion
import liquibase.Scope
import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import liquibase.ext.neo4j.Neo4jContainerSpec

class CheckSumUpgradeIT extends Neo4jContainerSpec {

    def "upgrades check sums upon subsequent run"() {
        given:
        def changeLogFormat = format
            runMigrations(changeLogFormat)
            hackyDowngradeCheckSums(format)
            assert allStoredCheckSumsFollowVersion(ChecksumVersion.V8)

        when:
        runMigrations(format)

        then:
        allStoredCheckSumsFollowVersion(ChecksumVersion.latest())

        where:
        format << ["json", "sql", "xml", "yaml"]
    }


    private void runMigrations(String format) {
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, "jdbc:neo4j:${neo4jContainer.getBoltUrl()}".toString())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, "neo4j")
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, PASSWORD)
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/basic/changeLog.${format}".toString())
        command.execute()
    }

    private boolean allStoredCheckSumsFollowVersion(ChecksumVersion version) {
        def rows = queryRunner.getRows("MATCH (changeSet:__LiquibaseChangeSet) RETURN changeSet.checkSum AS checkSum")
        return rows.size() > 0 && rows.every { ((String) it["checkSum"]).startsWith(version.version + ":")}
    }

    private hackyDowngradeCheckSums(String format) {
        // Checksums generated using Liquibase 4.21.1
        String replacement = "8:2713d4d60749a4b55b25f25657bd93ce"
        if (format == "sql") {
            replacement = "8:7c1c2967734b91e5422e52c05d4dd511"
        }
        // note: it relies on the internal structure of check sums (version:actualChecksum)
        queryRunner.run("""
                        | MATCH (changeSet:__LiquibaseChangeSet)
                        | WITH changeSet, changeSet.checkSum AS original, \$search + ':' AS search, \$replacement + ':' AS replacement
                        | WITH changeSet, original, search, replacement, split(original, search) AS parts
                        | WITH changeSet, 
                        |      CASE size(parts)
                        |         WHEN 1 THEN original
                        |         ELSE replacement
                        |      END AS newCheckSum
                        | SET changeSet.checkSum = newCheckSum""".stripMargin(),
                [
                        search: String.valueOf(ChecksumVersion.latest().version),
                        replacement: replacement
                ])
    }

}
