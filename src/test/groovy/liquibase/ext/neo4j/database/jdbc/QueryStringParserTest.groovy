package liquibase.ext.neo4j.database.jdbc

import spock.lang.Specification

import static liquibase.ext.neo4j.database.jdbc.QueryStringParser.parseQueryString

class QueryStringParserTest extends Specification {

    def "parses query string"() {
        expect:
        parseQueryString(uri) == new QueryString(parameters)

        where:
        uri                                                                   | parameters
        "neo4j://example.com"                                                 | [:]
        "neo4j://example.com?"                                                | [:]
        "neo4j://example.com?foo=bar"                                         | [foo: ["bar"]]
        "neo4j://example.com?foo=bar&bar=fighters"                            | [foo: ["bar"], bar: ["fighters"]]
        "neo4j://example.com?foo=bar&foo=fighters"                            | [foo: ["bar", "fighters"]]
        "neo4j://example.com?foo=je%20suis%20Groot"                           | [foo: ["je suis Groot"]]
        "neo4j://example.com?foo=je%20suis%20Groot%20%26%20toi%20%3F&bar=qix" | [foo: ["je suis Groot & toi ?"], bar: ["qix"]]
        "neo4j://example.com?je%20suis%26=Groot"                              | ["je suis&": ["Groot"]]
        "neo4j://example.com?booleanFlag"                                     | ["booleanFlag": ["true"]]
    }
}
