--liquibase formatted cypher
--changeset fbiville:count-movies runAlways:true logicalFilePath:changelog.cypher
MATCH (m:Movie) WITH count(m) AS count MERGE (c:Count) SET c.value = count
--rollback MATCH (c:Count) DETACH DELETE c
