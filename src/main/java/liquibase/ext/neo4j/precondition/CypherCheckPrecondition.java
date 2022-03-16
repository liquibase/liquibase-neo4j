package liquibase.ext.neo4j.precondition;

import liquibase.precondition.core.SqlPrecondition;

public class CypherCheckPrecondition extends SqlPrecondition {

  @Override
  public String getName() {
    return "cypherCheck";
  }
}
