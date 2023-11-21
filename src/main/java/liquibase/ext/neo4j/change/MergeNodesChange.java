package liquibase.ext.neo4j.change;

import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.change.refactoring.MatchPattern;
import liquibase.ext.neo4j.change.refactoring.NodeMerger;
import liquibase.ext.neo4j.change.refactoring.PropertyMergePolicy;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.statement.SqlStatement;

import java.util.ArrayList;
import java.util.List;

import static liquibase.ext.neo4j.change.Sequences.isNullOrEmpty;

@DatabaseChange(name = "mergeNodes", priority = ChangeMetaData.PRIORITY_DEFAULT, description =
        "The 'mergeNodes' tag allows you to merge nodes matching a given pattern described by the tag 'fragment' attribute." +
                " \n" +
                "It is especially useful after loading data from external, non-graph data sources.\n" +
                "\n" +
                "'mergeNodes' leaves the graph unchanged if fewer than two nodes match the specific pattern.\n" +
                "\n" +
                "'mergeNodes' will target the first node of the sequence and \"collapse\" all other nodes into it.\n" +
                "It is highly advised the 'fragment' attribute defines an explicit order clause. For instance, favor \n" +
                "'(p:Person) WITH p ORDER BY p.name ASC' over just '(p:Person)'. The latter fragment does not specify any explicit order\n" +
                "so the result of 'mergeNodes' with the same data set on two different servers could be different.\n" +
                "\n" +
                "'mergeNodes' also defines the 'outputVariable' attribute. This attribute denotes the variable used in the pattern for \n" +
                "the nodes to merge. If the fragment is \n" +
                "'(m:Movie)<-[:DIRECTED_BY]-(d:Director {name: 'John Woo'}) WITH m" +
                " ORDER BY m.title', the output variable is most likely \n" +
                "'m', so that all the nodes of the movies directed by 'John Woo' are merged " +
                "into a single node.\n" +
                "'outputVariable' must always " +
                "refer to a variable defined in 'fragment'.\n" +
                "\n" +
                "When merging nodes, property conflicts may happen. To prevent that, 'mergeNodes' accepts one to many 'propertyPolicy'\n" +
                "tags. Each policy consists of a 'nameMatcher' attribute which is a regular expression for property names.\n" +
                "'mergeStrategy" +
                "' defines what to do when combining properties. The strategy is either 'KEEP_ALL' where all values from \n" +
                "matched nodes under the same property name are combined into a single array, or 'KEEP_FIRST' where only the first set \n" +
                "value is kept (this is not necessarily the first node's value since it may not define that particular property), or \n" +
                "'KEEP_LAST' where only the last set value is kept (likewise, " +
                "this is not necessarily the last node's value).\n" +
                "All matched nodes' property names must have a matching policy or the change set execution will fail.")
public class MergeNodesChange extends AbstractChange {

    private String fragment;
    private String outputVariable;
    private List<PropertyMergePolicy> propertyPolicies = new ArrayList<>();

    @Override
    public ValidationErrors validate(Database database) {
        if (Sequences.isNullOrBlank(fragment)) {
            return new ValidationErrors(this)
                    .addError("missing Cypher fragment");
        }
        if (Sequences.isNullOrBlank(outputVariable)) {
            return new ValidationErrors(this)
                    .addError("missing Cypher output variable");
        }
        if (isNullOrEmpty(propertyPolicies)) {
            return new ValidationErrors(this)
                    .addError("missing property merge policy");
        }
        for (PropertyMergePolicy policy : propertyPolicies) {
            if (policy == null) {
                return new ValidationErrors(this)
                        .addError("property merge policy cannot be null");
            }
            ValidationErrors errors = policy.validate();
            if (errors.hasErrors()) {
                return errors;
            }
        }
        return super.validate(database);
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof Neo4jDatabase;
    }

    @Override
    public String getConfirmationMessage() {
        return String.format("nodes %s have been merged", fragment);
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        try {
            MatchPattern pattern = MatchPattern.of(fragment, outputVariable);
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
