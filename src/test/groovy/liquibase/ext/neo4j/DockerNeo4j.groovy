package liquibase.ext.neo4j

import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.utility.DockerImageName

import java.time.ZoneId

import static java.lang.Boolean.parseBoolean

class DockerNeo4j {

    static container(String password, ZoneId timezone, String tag = dockerTag()) {
        def container = new Neo4jContainer<>(DockerImageName.parse("neo4j").withTag(tag))
                .withAdminPassword(password)
                .withNeo4jConfig("db.temporal.timezone", timezone.id)
        if (!enterpriseEdition()) {
            return container
        }
        return container.withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    }

    static String neo4jVersion() {
        return System.getenv().getOrDefault("NEO4J_VERSION", "3.5")
    }

    static boolean enterpriseEdition() {
        return parseBoolean(System.getenv().getOrDefault("ENTERPRISE", "false"))
    }

    private static String dockerTag() {
        return "${neo4jVersion()}${enterpriseEdition() ? "-enterprise" : ""}"
    }
}
