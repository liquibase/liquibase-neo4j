package liquibase.ext.neo4j.database.jdbc

import org.neo4j.driver.Record
import org.neo4j.driver.Result
import org.neo4j.driver.Values
import org.neo4j.driver.internal.InternalPath
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.internal.value.NodeValue
import org.neo4j.driver.internal.value.PathValue
import org.neo4j.driver.internal.value.RelationshipValue
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Path
import org.neo4j.driver.types.Relationship
import spock.lang.Specification

import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Timestamp
import java.time.ZoneOffset
import java.time.ZonedDateTime

class Neo4jResultSet_getObject_Test extends Specification {
    Neo4jStatement statement
    Result result
    ResultSet resultSet

    def setup() {
        statement = Mock(Neo4jStatement.class)
        result = Mock(Result.class)
        resultSet = new Neo4jResultSet(statement, InternalTypeSystem.TYPE_SYSTEM, result)
    }

    def "gets named object value"() {
        given:
        def row = Mock(Record.class)
        row.get("foo") >> Values.value(input)
        row.get("bar") >> Values.NULL
        result.next() >> row

        and:
        resultSet.next()

        expect:
        resultSet.getObject("foo") == output
        resultSet.getObject("bar") == null

        where:
        input                                                                 | output
        true                                                                  | true
        false                                                                 | false
        42                                                                    | 42
        42L                                                                   | 42L
        1.0f                                                                  | 1.0f
        1.0d                                                                  | 1.0d
        [1]                                                                   | [1]
        [k: "v"]                                                              | [k: "v"]
        Values.point(7203, 42, 43)                                            | [srid: 7203, crs: "cartesian", x: 42, y: 43]
        Values.point(9157, 42, 43, 44)                                        | [srid: 9157, crs: "cartesian-3d", x: 42, y: 43, z: 44]
        Values.point(4326, 42, 43)                                            | [srid: 4326, crs: "wgs-84", x: 42.0, y: 43.0, longitude: 42, latitude: 43]
        Values.point(4979, 42, 43, 44)                                        | [srid: 4979, crs: "wgs-84-3d", x: 42.0, y: 43.0, z: 44, longitude: 42, latitude: 43, height: 44]
        [[1], true, [almostpi: 2d]]                                           | [[1], true, [almostpi: 2d]]
        [key: [true, false, Values.point(7203, 42, 43)]]                      | [key: [true, false, [srid: 7203, crs: "cartesian", x: 42, y: 43]]]
        nodeValue(42L, ["Label1", "Label2"], [prop1: true, prop2: "yep"])                    | [_id: 42L, _labels: ["Label1", "Label2"], _properties: [prop1: true, prop2: "yep"]]
        nodeValueV5(42L, "elementId", ["Label1", "Label2"], [prop1: true, prop2: "yep"])     | [_id: 42L, _elementId: "elementId", _labels: ["Label1", "Label2"], _properties: [prop1: true, prop2: "yep"]]
        relationshipValue(42L, "type", [prop1: true, prop2: "yep"], 43L, 44L)                | [_id: 42L, _type: "type", _properties: [prop1: true, prop2: "yep"], "_startId": 43L, "_endId": 44L]
        relationshipValueV5(42L, "elementId", "type", [prop1: true, prop2: "yep"], 43L, 44L) | [_id: 42L, _elementId: "elementId", _type: "type", _properties: [prop1: true, prop2: "yep"], "_startId": 43L, "_endId": 44L]
        pathValue(nodeValue(42L, ["S"], [k1: "v1"]),
                relationshipValue(43L, "L", [k2: "v2"], 42L, 44L),
                nodeValue(44L, ["E"], [k3: "v3"]))                            | [[_id: 42L, _labels: ["S"], _properties: [k1: "v1"]],
                                                                                 [_id: 43L, _type: "L", _properties: [k2: "v2"], "_startId": 42L, "_endId": 44L],
                                                                                 [_id: 44L, _labels: ["E"], _properties: [k3: "v3"]]]
        ZonedDateTime.of(2000, 1, 1, 12, 12, 0, 0, ZoneOffset.UTC)            | new Timestamp(946728720000)
    }

    def "gets indexed object value"() {
        given:
        def row = Mock(Record.class)
        row.get(42 - 1) >> Values.value(input)
        row.get(44 - 1) >> Values.NULL
        result.next() >> row

        and:
        resultSet.next()

        expect:
        resultSet.getObject(42) == output
        resultSet.getObject(44) == null

        where:
        input                                                                                | output
        true                                                                                 | true
        false                                                                                | false
        42                                                                                   | 42
        42L                                                                                  | 42L
        1.0f                                                                                 | 1.0f
        1.0d                                                                                 | 1.0d
        [1]                                                                                  | [1]
        [k: "v"]                                                                             | [k: "v"]
        Values.point(7203, 42, 43)                                                           | [srid: 7203, crs: "cartesian", x: 42, y: 43]
        Values.point(9157, 42, 43, 44)                                                       | [srid: 9157, crs: "cartesian-3d", x: 42, y: 43, z: 44]
        Values.point(4326, 42, 43)                                                           | [srid: 4326, crs: "wgs-84", x: 42.0, y: 43.0, longitude: 42, latitude: 43]
        Values.point(4979, 42, 43, 44)                                                       | [srid: 4979, crs: "wgs-84-3d", x: 42.0, y: 43.0, z: 44, longitude: 42, latitude: 43, height: 44]
        [[1], true, [almostpi: 2d]]                                                          | [[1], true, [almostpi: 2d]]
        [key: [true, false, Values.point(7203, 42, 43)]]                                     | [key: [true, false, [srid: 7203, crs: "cartesian", x: 42, y: 43]]]
        nodeValue(42L, ["Label1", "Label2"], [prop1: true, prop2: "yep"])                    | [_id: 42L, _labels: ["Label1", "Label2"], _properties: [prop1: true, prop2: "yep"]]
        nodeValueV5(42L, "elementId", ["Label1", "Label2"], [prop1: true, prop2: "yep"])     | [_id: 42L, _elementId: "elementId", _labels: ["Label1", "Label2"], _properties: [prop1: true, prop2: "yep"]]
        relationshipValue(42L, "type", [prop1: true, prop2: "yep"], 43L, 44L)                | [_id: 42L, _type: "type", _properties: [prop1: true, prop2: "yep"], "_startId": 43L, "_endId": 44L]
        relationshipValueV5(42L, "elementId", "type", [prop1: true, prop2: "yep"], 43L, 44L) | [_id: 42L, _elementId: "elementId", _type: "type", _properties: [prop1: true, prop2: "yep"], "_startId": 43L, "_endId": 44L]
        pathValue(nodeValue(42L, ["S"], [k1: "v1"]),
                relationshipValue(43L, "L", [k2: "v2"], 42L, 44L),
                nodeValue(44L, ["E"], [k3: "v3"]))                                           | [[_id: 42L, _labels: ["S"], _properties: [k1: "v1"]],
                                                                                                [_id: 43L, _type: "L", _properties: [k2: "v2"], "_startId": 42L, "_endId": 44L],
                                                                                                [_id: 44L, _labels: ["E"], _properties: [k3: "v3"]]]
        ZonedDateTime.of(2000, 1, 1, 12, 12, 0, 0, ZoneOffset.UTC)                           | new Timestamp(946728720000)
    }

    def "fails getting named object value if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        def row = Mock(Record.class)
        row.get("foo") >> { throw driverException }
        result.next() >> row

        when:
        resultSet.next()

        and:
        resultSet.getString("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot get column "foo"'
        exception.cause == driverException
    }

    def "fails getting indexed object value if access error occurred"() {
        given:
        def driverException = new RuntimeException("oopsie")
        def row = Mock(Record.class)
        row.get(42 - 1) >> { throw driverException }
        result.next() >> row

        when:
        resultSet.next()

        and:
        resultSet.getObject(42)

        then:
        def exception = thrown(SQLException)
        exception.message == 'cannot get column 42'
        exception.cause == driverException
    }

    def "fails to get named object value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getObject("foo")

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }

    def "fails to get indexed object value if result set is already closed"() {
        given:
        resultSet.close()

        when:
        resultSet.getObject(42)

        then:
        def exception = thrown(SQLException)
        exception.message == "cannot perform operation: result set is closed"
    }

    NodeValue nodeValue(long id, List<String> labels, Map<String, Object> properties) {
        def node = Mock(Node.class)
        node.id() >> id
        node.labels() >> labels
        node.asMap() >> properties
        return new NodeValue(node)
    }

    // emulates Nodes with v5 driver (elementId would actually belong to the Entity interface)
    interface NodeV5 extends Node {
        String elementId()
    }

    NodeValue nodeValueV5(long id, String elementId, List<String> labels, Map<String, Object> properties) {
        def node = Mock(NodeV5.class)
        node.id() >> id
        node.elementId() >> elementId
        node.labels() >> labels
        node.asMap() >> properties
        return new NodeValue(node)
    }

    RelationshipValue relationshipValue(long id, String type, Map<String, Object> properties, long startNodeId, long endNodeId) {
        def relationship = Mock(Relationship.class)
        relationship.id() >> id
        relationship.type() >> type
        relationship.asMap() >> properties
        relationship.startNodeId() >> startNodeId
        relationship.endNodeId() >> endNodeId
        return new RelationshipValue(relationship)
    }

    // emulates Nodes with v5 driver (elementId would actually belong to the Entity interface)
    interface RelationshipV5 extends Relationship {
        String elementId()
    }

    RelationshipValue relationshipValueV5(long id, String elementId, String type, Map<String, Object> properties, long startNodeId, long endNodeId) {
        def relationship = Mock(RelationshipV5.class)
        relationship.id() >> id
        relationship.elementId() >> elementId
        relationship.type() >> type
        relationship.asMap() >> properties
        relationship.startNodeId() >> startNodeId
        relationship.endNodeId() >> endNodeId
        return new RelationshipValue(relationship)
    }

    PathValue pathValue(NodeValue start, Object... rest) {
        if (rest.length % 2 != 0) {
            throw new RuntimeException("extra arguments should be an alternating list of rels and nodes")
        }
        def startNode = start.asNode()
        def path = Mock(Path.class)
        path.start() >> startNode
        if (rest.length == 0) {
            path.end() >> startNode
            path.iterator() >> Collections.EMPTY_LIST.iterator()
            return new PathValue(path)
        }
        def segments = new ArrayList<Path.Segment>(rest.length.intdiv(2).intValue())
        for (def i = 0; i < rest.length; i += 2) {
            def relationship = ((RelationshipValue) rest[i]).asRelationship()
            def endNode = ((NodeValue) rest[i + 1]).asNode()
            segments.add(new InternalPath.SelfContainedSegment(startNode, relationship, endNode))
            startNode = endNode
        }
        path.end() >> ((NodeValue) rest.last()).asNode()
        path.iterator() >> segments.iterator()
        return new PathValue(path)
    }
}
