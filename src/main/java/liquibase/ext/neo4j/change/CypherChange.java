package liquibase.ext.neo4j.change;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.RawSQLChange;

@DatabaseChange(name = "cypher", priority = ChangeMetaData.PRIORITY_DEFAULT, description =
        "The 'cypher' tag allows you to specify whatever cypher you want. It is useful for complex changes " +
                "that aren't supported through Liquibase's automated refactoring tags and to work around bugs and " +
                "limitations " +
                "of Liquibase. The Cypher contained in the cypher tag can be multi-line.\n" +
                "\n" +
                "The 'cypher' tag can also support multiline statements in the same file. Statements can either be split " +
                "using a ; at the end of the last line of the Cypher or a 'GO' on its own on the line between the statements " +
                "can be used. Multiline Cypher statements are also supported and only a ; or GO statement will finish a " +
                "statement, a new line is not enough. Files containing a single statement do not need to use a ; or GO.\n" +
                "\n" +
                "The cypher change can also contain comments of either of the following formats:\n" +
                "\n" +
                "A multiline comment that starts with /* and ends with */.\n" +
                "A single line comment starting with <space>--<space> and finishing at the end of the line.\n" +
                "Note: By default it will attempt to split statements on a ';' or 'go' at the end of lines. Because of " +
                "this, if you have a comment or some other non-statement ending ';' or 'go', don't have it at the end of a " +
                "line or you will get invalid Cypher.")
public class CypherChange extends RawSQLChange {

}
