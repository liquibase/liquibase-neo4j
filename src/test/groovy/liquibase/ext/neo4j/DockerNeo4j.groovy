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
        return version.startsWith("4.4") || parseMajor(version) >= 5
    }

    static boolean supportsMultiTenancy() {
        def version = neo4jVersion()
        return parseMajor(version) >= 4 && enterpriseEdition()
    }

    static boolean supportsShowConstraintsYieldSyntax() {
        return supportsShowIndexesYieldSyntax()
    }

    static boolean supportsShowIndexesYieldSyntax() {
        def version = neo4jVersion()
        def major = parseMajor(version)
        int minor = parseMinor(version)
        // SHOW INDEXES|CONSTRAINTS is supported since Neo4j 4.2
        // SHOW INDEXES|CONSTRAINTS YIELD xxx RETURN xxx is supported since Neo4j 4.3
        return major >= 5 || major == 4 && minor >= 3
    }

    static String dockerTag() {
        return "${neo4jVersion()}${enterpriseEdition() ? "-enterprise" : ""}"
    }

    static boolean supportsTypeConstraints() {
        if (!enterpriseEdition()) {
            return false
        }
        def version = neo4jVersion()
        def major = parseMajor(version)
        def minor = parseMinor(version)
        // type constraints added in 5.9 (with list types in 5.10 and union types in 5.11)
        return (major == 5 && minor >= 11) || major > 5
    }

    static boolean enterpriseEdition() {
        return parseBoolean(System.getenv().getOrDefault("ENTERPRISE", "false"))
    }

    static String neo4jVersion() {
        return System.getenv().getOrDefault("NEO4J_VERSION", "4.4")
    }

    private static int parseMajor(String version) {
        def nextDot = version.indexOf(".")
        if (nextDot >= 0) {
            return Integer.parseInt(version.substring(0, nextDot), 10)
        }
        return Integer.parseInt(version, 10)
    }

    private static int parseMinor(String version) {
        def rest = version.substring(2)
        if (rest.isEmpty()) {
            throw new RuntimeException("Could not extract minor version from Neo4j Docker version ${version}")
        }
        def nextDot = rest.indexOf(".")
        if (nextDot >= 0) {
            return Integer.parseInt(rest.substring(0, nextDot), 10)
        }
        def nextDash = rest.indexOf("-")
        if (nextDash >= 0) {
            return Integer.parseInt(rest.substring(0, nextDash), 10)
        }
        return Integer.parseInt(rest, 10)
    }
}
