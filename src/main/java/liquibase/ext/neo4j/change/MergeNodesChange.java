package liquibase.ext.neo4j.change;

import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.ext.neo4j.change.refactoring.MergePattern;
import liquibase.ext.neo4j.change.refactoring.NodeMerger;
import liquibase.ext.neo4j.change.refactoring.PropertyMergePolicy;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.statement.SqlStatement;

import java.util.ArrayList;
import java.util.List;

@DatabaseChange(name = "mergeNodes", priority = ChangeMetaData.PRIORITY_DEFAULT, description = "TODO")
public class MergeNodesChange extends AbstractChange {

    private String fragment;
    private String outputVariable;
    private List<PropertyMergePolicy> propertyPolicies = new ArrayList<>();

    @Override
    public String getConfirmationMessage() {
        return String.format("nodes %s have been merged", fragment);
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        try {
            MergePattern pattern = MergePattern.of(fragment, outputVariable);
            return new NodeMerger((Neo4jDatabase) database).merge(pattern, propertyPolicies);
        } catch (LiquibaseException e) {
            throw new RuntimeException(e);
        }
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

    public List<PropertyMergePolicy> getPropertyPolicies() {
        return propertyPolicies;
    }

    public void setPropertyPolicies(List<PropertyMergePolicy> propertyPolicies) {
        this.propertyPolicies = propertyPolicies;
    }
}
