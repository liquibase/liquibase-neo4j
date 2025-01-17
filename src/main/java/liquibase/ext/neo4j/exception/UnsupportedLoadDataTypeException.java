package liquibase.ext.neo4j.exception;

import liquibase.exception.UnexpectedLiquibaseException;

@SuppressWarnings("serial")
public class UnsupportedLoadDataTypeException extends UnexpectedLiquibaseException {
    public UnsupportedLoadDataTypeException(String message, Object... args) {
        super(String.format(message, args));
    }
}
