package liquibase.ext.neo4j.change.refactoring;

import liquibase.serializer.AbstractLiquibaseSerializable;

import java.util.Objects;

public class ExtractedNodes extends AbstractLiquibaseSerializable  {
    private String withLabel;
    private String withProperty;
    private boolean merge;
    private ExtractedRelationships linkedFromSource;

    @Override
    public String getSerializedObjectName() {
        return "extractedNodes";
    }

    @Override
    public String getSerializedObjectNamespace() {
        return GENERIC_CHANGELOG_EXTENSION_NAMESPACE;
    }

    public String getWithLabel() {
        return withLabel;
    }

    public void setWithLabel(String withLabel) {
        this.withLabel = withLabel;
    }

    public String getWithProperty() {
        return withProperty;
    }

    public void setWithProperty(String withProperty) {
        this.withProperty = withProperty;
    }

    public boolean isMerge() {
        return merge;
    }

    public void setMerge(boolean merge) {
        this.merge = merge;
    }

    public ExtractedRelationships getLinkedFromSource() {
        return linkedFromSource;
    }

    public void setLinkedFromSource(ExtractedRelationships linkedFromSource) {
        this.linkedFromSource = linkedFromSource;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtractedNodes that = (ExtractedNodes) o;
        return merge == that.merge && Objects.equals(withLabel, that.withLabel) && Objects.equals(withProperty, that.withProperty) && Objects.equals(linkedFromSource, that.linkedFromSource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(withLabel, withProperty, merge, linkedFromSource);
    }

    @Override
    public String toString() {
        return "ExtractedNodes{" +
                "withLabel='" + withLabel + '\'' +
                ", withProperty='" + withProperty + '\'' +
                ", merge=" + merge +
                ", linkedFromSource=" + linkedFromSource +
                '}';
    }
}
