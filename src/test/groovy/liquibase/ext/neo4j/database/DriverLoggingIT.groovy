package liquibase.ext.neo4j.database

import liquibase.database.DatabaseConnection
import liquibase.database.DatabaseFactory
import liquibase.ext.neo4j.Neo4jContainerSpec

class DriverLoggingIT extends Neo4jContainerSpec {

    def "enables driver console logging"() {
        given:
        def buffer = new ByteArrayOutputStream()
        System.err = new PrintStream(buffer)
        def database = new Neo4jDatabase()

        when:
        database.setConnection(openConnection(new Tuple2<>("driver.logging.console.level", "FINE")))

        then:
        !buffer.toString().isEmpty()
    }

    private DatabaseConnection openConnection(Tuple2<String, String>... queryString) {
        return DatabaseFactory.instance.openConnection(
                "jdbc:neo4j:" + neo4jContainer.getBoltUrl() + serializeQueryString(queryString),
                "neo4j",
                PASSWORD,
                null,
                null
        )
    }

    private String serializeQueryString(Tuple2<String, String>[] queryString) {
        if (queryString.size() == 0) {
            return ""
        }
        def builder = new StringBuilder("?")
        for (final def element in queryString) {
            builder.append(element.getV1())
            builder.append("=")
            builder.append(element.getV2())
            builder.append("&")
        }
        return builder.substring(0, builder.length()-1)
    }
}
