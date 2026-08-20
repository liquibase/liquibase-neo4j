package liquibase.ext.neo4j.structure;

class GraphTypeTruncator {

    public static String truncate(String spec) {
        var processedSpec = spec.replace("\n", "");
        if (processedSpec.length() <= 50) {
            return processedSpec;
        }
        var shortenedSpec = processedSpec.substring(0, 50);
        return "%s [...]%s".formatted(shortenedSpec, closingSymbols(shortenedSpec));
    }

    private static String closingSymbols(String shortenedSpec) {
        var symbols = shortenedSpec.codePoints()
                .mapToObj(Character::toString)
                .toList();

        var builder = new StringBuilder();
        for (String symbol : symbols) {
            switch (symbol) {
                case "{": {
                    builder.append("} ");
                    break;
                }
                case "(": {
                    builder.append(") ");
                    break;
                }
                case "}", ")": {
                    builder.deleteCharAt(builder.length()-1);
                    break;
                }
            }
        }
        return builder.reverse().toString();
    }
}
