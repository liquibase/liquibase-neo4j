package liquibase.ext.neo4j

import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.utility.DockerImageName

import java.time.ZoneId

import static java.lang.Boolean.parseBoolean

class DockerNeo4j {

    static Neo4jContainer container(String password, ZoneId timezone, String tag = dockerTag()) {
        def container = new Neo4jContainer<>(DockerImageName.parse("neo4j").withTag(tag))
                .withAdminPassword(password)
                .withNeo4jConfig("db.temporal.timezone", timezone.id)
        if (!enterpriseEdition()) {
            return container
        }
        return container.withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    }

    static boolean supportsCypherCallInTransactions() {
        def version = neo4jVersion()
        return version.startsWith("4.4") || Integer.parseInt(version.substring(0, 1)) >= 5
    }

    static boolean supportsShowConstraintsYieldSyntax() {
        return supportsShowIndexesYieldSyntax()
    }

    static boolean supportsShowIndexesYieldSyntax() {
        def version = neo4jVersion()
        def major = Integer.parseInt(version.substring(0, 1))
        def minor = version.contains(".") ? Integer.parseInt(version.substring(2, 3)) : Integer.MAX_VALUE
        // SHOW INDEXES|CONSTRAINTS is supported since Neo4j 4.2
        // SHOW INDEXES|CONSTRAINTS YIELD xxx RETURN xxx is supported since Neo4j 4.3
        return major >= 5 || major == 4 && minor >= 3
    }

    static String dockerTag() {
        return "${neo4jVersion()}${enterpriseEdition() ? "-enterprise" : ""}"
    }

    static boolean enterpriseEdition() {
        return parseBoolean(System.getenv().getOrDefault("ENTERPRISE", "false"))
    }

    static String neo4jVersion() {
        return System.getenv().getOrDefault("NEO4J_VERSION", "4.4")
    }
}
