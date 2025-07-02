package liquibase.ext.neo4j

import liquibase.ext.neo4j.database.KernelVersion
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

    static String dockerTag() {
        return "${padMinor(neo4jVersion())}${enterpriseEdition() ? "-enterprise" : ""}"
    }

    static boolean enterpriseEdition() {
        return parseBoolean(System.getenv().getOrDefault("ENTERPRISE", "true"))
    }

    static KernelVersion neo4jVersion() {
        return KernelVersion.parse(System.getenv().getOrDefault("NEO4J_VERSION", "4.4"))
    }

    private static padMinor(KernelVersion v) {
        if (v.isCalver() && v.minor() < 10) {
            if (v.patch() != Integer.MAX_VALUE) {
                return "${v.major()}.0${v.minor()}.${v.patch()}"
            }
            return "${v.major()}.0${v.minor()}"
        }
        return v.versionString()
    }
}
