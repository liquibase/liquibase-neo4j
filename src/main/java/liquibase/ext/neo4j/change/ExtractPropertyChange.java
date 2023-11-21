package liquibase.ext.neo4j.change;

import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.neo4j.change.refactoring.ExtractedNodes;
import liquibase.ext.neo4j.change.refactoring.ExtractedRelationships;
import liquibase.ext.neo4j.change.refactoring.MatchPattern;
import liquibase.ext.neo4j.change.refactoring.NodeExtraction;
import liquibase.ext.neo4j.change.refactoring.PropertyExtraction;
import liquibase.ext.neo4j.change.refactoring.PropertyExtractor;
import liquibase.ext.neo4j.change.refactoring.RelationshipDirection;
import liquibase.ext.neo4j.change.refactoring.RelationshipExtraction;
import liquibase.ext.neo4j.database.Neo4jDatabase;
import liquibase.statement.SqlStatement;

import static liquibase.ext.neo4j.change.refactoring.PropertyExtractor.NODE_VARIABLE;

@DatabaseChange(name = "extractProperty", priority = ChangeMetaData.PRIORITY_DEFAULT, description =
        "The 'extractProperty' tag allows you to extract properties from nodes matching the pattern described by the 'fromNodes' attribute.\n" +
                " \n" +
                "Properties are extracted to their own nodes, with the specified target label and property name. The property is removed\n" +
                "from the source nodes.\n" +
                "\n" +
                "These extracted nodes can be created or merged as controlled by the 'merge' attribute. The default behavior is to create nodes. \n" +
                "\n" +
                "Each resulting node can optionally be linked with the corresponding source node the property has been extracted from.\n" +
                "By default, no relationships are created. " +
                "The direction and type of the relationship must be explicitly provided for the linking to happen. \n" +
                "\n" +
                "The relationships between the source nodes and the extracted ones can be created or merged, as controlled by their own 'merge' attribute.\n" +
                "If relationships are specified, the default behavior is to create relationships.")
public class ExtractPropertyChange extends AbstractChange {

    private String property;
    private String fromNodes;
    private String nodesNamed;
    private ExtractedNodes toNodes;

    @Override
    public String getConfirmationMessage() {
        return String.format("property \"%s\" of nodes matching \"%s\" has been extracted", property, fromNodes);
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof Neo4jDatabase;
    }

    @Override
    public ValidationErrors validate(Database database) {
        if (Sequences.isNullOrBlank(property)) {
            return new ValidationErrors(this)
                    .addError("missing property name");
        }
        if (Sequences.isNullOrBlank(fromNodes)) {
            return new ValidationErrors(this)
                    .addError("missing Cypher fragment");
        }
        if (Sequences.isNullOrBlank(nodesNamed)) {
            return new ValidationErrors(this)
                    .addError("missing Cypher output variable");
        }
        if (NODE_VARIABLE.equals(nodesNamed)) {
            return new ValidationErrors(this)
                    .addError(String.format("Cypher output variable \"%s\" is reserved, please use another name", NODE_VARIABLE));
        }
        if (toNodes == null) {
            return new ValidationErrors(this)
                    .addError("missing node extraction description");
        }
        String label = toNodes.getWithLabel();
        if (Sequences.isNullOrBlank(label)) {
            return new ValidationErrors(this)
                    .addError("missing label in node extraction description");
        }
        String targetPropertyName = toNodes.getWithProperty();
        if (Sequences.isNullOrBlank(targetPropertyName)) {
            return new ValidationErrors(this)
                    .addError("missing target property name in node extraction description");
        }
        ExtractedRelationships extractedRelationships = toNodes.getLinkedFromSource();
        if (extractedRelationships != null) {
            String relationshipsType = extractedRelationships.getWithType();
            if (Sequences.isNullOrBlank(relationshipsType)) {
                return new ValidationErrors(this)
                        .addError("missing relationship type in node extraction description");
            }
            if (extractedRelationships.getWithDirection() == null) {
                return new ValidationErrors(this)
                        .addError("missing relationship direction in node extraction description");
            }
            boolean mergeNodes = toNodes.isMerge();
            boolean mergeRelationships = extractedRelationships.isMerge();
            if (!mergeNodes && mergeRelationships) {
                return new ValidationErrors(this)
                        .addWarning("creating nodes imply creating relationships - enable node merge or disable relation merge to suppress this warning");
            }
        }
        return super.validate(database);
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new PropertyExtractor().extract(PropertyExtraction.of(
                MatchPattern.of(fromNodes, nodesNamed),
                extractedNodes(),
                extractedRelationships()
        ));
    }

    private NodeExtraction extractedNodes() {
        String label = toNodes.getWithLabel();
        String targetPropertyName = toNodes.getWithProperty();
        if (toNodes.isMerge()) {
            return NodeExtraction.merging(label, property, targetPropertyName);
        }
        return NodeExtraction.creating(label, property, targetPropertyName);
    }

    private RelationshipExtraction extractedRelationships() {
        ExtractedRelationships extractedRelationships = toNodes.getLinkedFromSource();
        if (extractedRelationships == null) {
            return null;
        }
        String type = extractedRelationships.getWithType();
        RelationshipDirection direction = extractedRelationships.getWithDirection();
        if (extractedRelationships.isMerge()) {
            return RelationshipExtraction.merging(type, direction);
        }
        return RelationshipExtraction.creating(type, direction);
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public String getFromNodes() {
        return fromNodes;
    }

    public void setFromNodes(String fromNodes) {
        this.fromNodes = fromNodes;
    }

    public String getNodesNamed() {
        return nodesNamed;
    }

    public void setNodesNamed(String nodesNamed) {
        this.nodesNamed = nodesNamed;
    }

    public ExtractedNodes getToNodes() {
        return toNodes;
    }

    public void setToNodes(ExtractedNodes toNodes) {
        this.toNodes = toNodes;
    }
}
