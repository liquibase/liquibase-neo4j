package liquibase.ext.neo4j.snapshot;

import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.ext.neo4j.structure.GraphType;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.core.Catalog;

import java.util.Optional;

import static liquibase.ext.neo4j.database.KernelVersion.V2026_02_0;

class GraphTypeSupport {

    public static Optional<GraphType> load(Catalog catalog, Neo4jDatabase neo4j) throws DatabaseException {
        if (!supportsGraphTypes(neo4j)) {
            return Optional.empty();
        }
        try {
            var specification = (String) neo4j.run(catalog, new RawParameterizedSqlStatement(
                            "CYPHER 25 SHOW CURRENT GRAPH TYPE YIELD specification RETURN specification"))
                    .get(0)
                    .get("specification");
            return Optional.of(new GraphType(catalog, specification));
        } catch (LiquibaseException e) {
            throw new DatabaseException("Could not retrieve Neo4j graph type", e);
        }
    }

    public static boolean supportsGraphTypes(Neo4jDatabase neo4j) {
        return neo4j.isEnterprise() && neo4j.getKernelVersion().compareTo(V2026_02_0) >= 0;
    }
}
