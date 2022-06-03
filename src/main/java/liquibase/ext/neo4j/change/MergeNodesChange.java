package liquibase.ext.neo4j.change;

import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.statement.SqlStatement;

import java.util.ArrayList;
import java.util.List;

@DatabaseChange(name = "mergeNodes", priority = ChangeMetaData.PRIORITY_DEFAULT, description = "TODO")
public class MergeNodesChange extends AbstractChange {

    private String fragment;
    private String outputVariable;
    private List<MergeNodeProperty> properties = new ArrayList<>();

    @Override
    public String getConfirmationMessage() {
        return String.format("nodes %s have been merged", fragment);
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[0];
    }

    @Override
    public boolean generateStatementsVolatile(Database database) {
        return true;
    }

    public String getFragment() {
        return fragment;
    }

    public void setFragment(String fragment) {
        this.fragment = fragment;
    }

    public String getOutputVariable() {
        return outputVariable;
    }

    public void setOutputVariable(String outputVariable) {
        this.outputVariable = outputVariable;
    }

    public List<MergeNodeProperty> getProperties() {
        return properties;
    }

    public void setProperties(List<MergeNodeProperty> properties) {
        this.properties = properties;
    }
}
