package liquibase.ext.neo4j.changelog;

import liquibase.ContextExpression;
import liquibase.Labels;
import liquibase.change.CheckSum;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;

import java.util.Date;

public class LiquigraphRanChangeSet extends RanChangeSet {

    public LiquigraphRanChangeSet(String changeLog, String id, String author, CheckSum checkSum, Date dateExecuted, String tag, ChangeSet.ExecType execType, String description, String comments, ContextExpression contextExpression, Labels labels, String deploymentId, String storedChangeLog) {
        super(changeLog, id, author, checkSum, dateExecuted, tag, execType, description, comments, contextExpression, labels, deploymentId, storedChangeLog);
    }

    @Override
    public boolean isSameAs(ChangeSet changeSet) {
        return this.getId().equalsIgnoreCase(changeSet.getId()) && this.getAuthor().equalsIgnoreCase(changeSet.getAuthor());
    }
}
