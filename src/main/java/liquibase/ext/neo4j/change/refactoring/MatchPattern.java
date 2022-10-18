package liquibase.ext.neo4j.change.refactoring;

public final class MatchPattern {
    private final String cypherFragment;
    private final String outputVariable;

    public static MatchPattern of(String cypherFragment, String variable) {
        return new MatchPattern(cypherFragment, variable);
    }

    private MatchPattern(String cypherFragment, String outputVariable) {
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
