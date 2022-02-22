package liquibase.ext.neo4j.parser;

import liquibase.parser.core.formattedsql.FormattedSqlChangeLogParser;

public class FormattedCypherChangeLogParser extends FormattedSqlChangeLogParser {

  @Override
  protected boolean supportExtension(String changelogFile){
    return changelogFile.endsWith(".cypher");
  }
}
