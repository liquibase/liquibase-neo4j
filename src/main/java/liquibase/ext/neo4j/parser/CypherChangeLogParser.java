package liquibase.ext.neo4j.parser;

import liquibase.parser.core.sql.SqlChangeLogParser;
import liquibase.resource.ResourceAccessor;

public class CypherChangeLogParser extends SqlChangeLogParser {

  @Override
  public boolean supports(String changeLogFile, ResourceAccessor resourceAccessor) {
    return changeLogFile.endsWith(".cypher");
  }
}
