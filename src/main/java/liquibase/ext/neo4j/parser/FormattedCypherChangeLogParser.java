package liquibase.ext.neo4j.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import liquibase.Scope;
import liquibase.parser.core.formattedsql.FormattedSqlChangeLogParser;
import liquibase.resource.ResourceAccessor;
import liquibase.util.StreamUtil;

public class FormattedCypherChangeLogParser extends FormattedSqlChangeLogParser {

  @Override
  public boolean supports(String changeLogFile, ResourceAccessor resourceAccessor) {
    BufferedReader reader = null;
    try {
      if (changeLogFile.endsWith(".cypher")) {
        InputStream fileStream = openChangeLogFile(changeLogFile, resourceAccessor);
        if (fileStream == null) {
          return false;
        }
        reader = new BufferedReader(StreamUtil.readStreamWithReader(fileStream, null));

        String firstLine = reader.readLine();

        while (firstLine.trim().isEmpty() && reader.ready()) {
          firstLine = reader.readLine();
        }

        return (firstLine != null) && firstLine.matches("\\-\\-\\s*liquibase formatted.*");
      } else {
        return false;
      }
    } catch (IOException e) {
      Scope.getCurrentScope().getLog(getClass()).fine("Exception reading " + changeLogFile, e);
      return false;
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          Scope.getCurrentScope().getLog(getClass()).fine("Exception closing " + changeLogFile, e);
        }
      }
    }
  }
}
