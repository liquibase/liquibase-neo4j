package liquibase.ext.neo4j.generator;

import liquibase.change.ChangeMetaData;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.core.DeleteStatement;

public class DeleteGenerator extends AbstractSqlGenerator<DeleteStatement> {

  @Override
  public ValidationErrors validate(DeleteStatement deleteStatement, Database database,
                                   SqlGeneratorChain<DeleteStatement> sqlGeneratorChain) {
    return null;
  }

  @Override
  public Sql[] generateSql(DeleteStatement deleteStatement, Database database,
                           SqlGeneratorChain<DeleteStatement> sqlGeneratorChain) {
    String label = deleteStatement.getTableName();
    String cypher = String.format("MATCH (n: %s)%nDELETE n", label);
    return new Sql[]{new UnparsedSql(cypher)};
  }

  @Override
  public int getPriority() {
    return ChangeMetaData.PRIORITY_DEFAULT;
  }
}
