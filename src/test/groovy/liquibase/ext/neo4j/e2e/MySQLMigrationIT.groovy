package liquibase.ext.neo4j.e2e

import liquibase.command.CommandScope
import liquibase.command.core.UpdateCommandStep
import liquibase.command.core.helpers.DatabaseChangelogCommandStep
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep
import org.testcontainers.containers.MySQLContainer
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager

import static liquibase.ext.neo4j.Neo4jContainerSpec.tempFilePrintStream

class MySQLMigrationIT extends Specification {

    @Shared
    def mysql = new MySQLContainer<>("mysql:8")

    @Shared
    Connection connection

    def setupSpec() {
        System.out = tempFilePrintStream()
        System.err = tempFilePrintStream()
        mysql.start()
        connection = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())
    }

    def cleanupSpec() {
        connection.close()
        mysql.stop()
    }

    def "runs migrations for MySQL without interference from the Neo4j plugin"() {
        when:
        execute()

        then:
        verifyExecution()
    }

    def "runs migrations for MySQL twice without interference from the Neo4j plugin"() {
        when:
        2.times {
            execute()
        }

        then:
        verifyExecution()
    }

    private void execute() {
        def command = new CommandScope(UpdateCommandStep.COMMAND_NAME)
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG, mysql.getJdbcUrl())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.USERNAME_ARG, mysql.getUsername())
                .addArgumentValue(DbUrlConnectionArgumentsCommandStep.PASSWORD_ARG, mysql.getPassword())
                .addArgumentValue(DatabaseChangelogCommandStep.CHANGELOG_FILE_ARG, "/e2e/mysql/changeLog.xml")
                .setOutput(System.out)
        command.execute()
    }

    private boolean verifyExecution() {
        connection.createStatement()
                .withCloseable {
                    it.executeQuery("SELECT name FROM first_names ORDER BY name ASC")
                            .withCloseable {
                                assert it.next()
                                assert it.getString("name") == "Florent"
                                assert it.next()
                                assert it.getString("name") == "Marouane"
                                assert it.next()
                                assert it.getString("name") == "Nathan"
                                assert it.next()
                                assert it.getString("name") == "Robert"
                                assert !it.next()
                            }
                    return true
                }
    }
}
