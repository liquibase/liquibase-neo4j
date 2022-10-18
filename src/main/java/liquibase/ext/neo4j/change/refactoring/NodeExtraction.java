package liquibase.ext.neo4j.change.refactoring;

public class NodeExtraction {
   private final String label;
   private final String sourcePropertyName;
   private final String targetPropertyName;
   private final boolean merge;

   public static NodeExtraction merging(String label, String sourcePropertyName, String targetPropertyName) {
      return new NodeExtraction(label, sourcePropertyName, targetPropertyName, true);
   }

   public static NodeExtraction creating(String label, String sourcePropertyName, String targetPropertyName) {
      return new NodeExtraction(label, sourcePropertyName, targetPropertyName, false);
   }

   private NodeExtraction(String label, String sourcePropertyName, String targetPropertyName, boolean merge) {
      this.label = label;
      this.sourcePropertyName = sourcePropertyName;
      this.targetPropertyName = targetPropertyName;
      this.merge = merge;
   }

   public String label() {
      return label;
   }

   public String sourcePropertyName() {
      return sourcePropertyName;
   }

   public String targetPropertyName() {
      return targetPropertyName;
   }

   public boolean isMerge() {
      return merge;
   }
}
