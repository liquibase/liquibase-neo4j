package liquibase.ext.neo4j.change.refactoring;

public final class MergePattern {
    private final String cypherFragment;
    private final String outputVariable;

    public static MergePattern of(String cypherFragment, String variable) {
        return new MergePattern(cypherFragment, variable);
    }

    private MergePattern(String cypherFragment, String outputVariable) {
        this.cypherFragment = cypherFragment;
        this.outputVariable = outputVariable;
    }

    public String cypherFragment() {
        return cypherFragment;
    }

    public String outputVariable() {
        return outputVariable;
    }
}
