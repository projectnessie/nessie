


> Written with [StackEdit](https://stackedit.io/).
> 

## Proposal to extend the View Definition
**Background**
The current  interface for a SqlView  which is a first class object in Nessie specifies a Sql text and a Dialect  and schema :

    public interface SqlView extends Contents {  
      
      public static enum Dialect {  
        HIVE,  
        SPARK,  
        DREMIO,  
        PRESTO  
      }  
      
      String getSqlText();  
      
      Dialect getDialect();  
      
      // Schema getSchema();  
    }

While thinking of integrating Dremio with Nessie, we found we needed several more fields to fully define a view along with  a schema  (written up in a separate proposal) 

**Proposal for additional fields**
While a View is ultimately describe by just the SQL query , these  additional fields needed to describe a view (or what we refer to as a Virtual Dataset in the context of Dremio) for the DREMIO dialect . This is the addition that we  propose we add as an implementation of  the existing SqlView interface. 
   

    public class DremioView  implements SqlView {
    ...    
      String datasetPath; // distinct path to the view 
    
      long datasetId; //unique ID *generic*
    
      String sqlText; // Sql view text
    
      ViewSchema schema; //Schema describing the view columns.
    
      long previousDatasetId; // pointer to the previous definition  used to link previous edits to the view 
    
      List<Origin> fieldOrigins; //the original table or view columns this view is defined on (useful for join recommendations during view compilation)
    
      String context; // the context this view was defined in - i.e the home space, folder.
    
      long lastModTimestamp; // Last time this view was modified   *generic*
     
      long userId ; // the user that last modified this view definition *generic*
    
      List<String> dependentHash ; // a list of dependent hashes this view was defined on to avoid GC on those hashes.
    
      List<string> tags; // List of tag strings of a fixed size (100) used to describe the view
    
    }

    class Origin {
      private List<String> table;  
      private String columnName;  
      private Boolean derived;
    }

**Extending the interface**
Since several of these fields are generic to other views, we could possibly move some of them into an abstract View class (marked with `*generic*` in the definition above)  - a middle layer that would be common to most view definitions. Other option is to leave SqlView as simple as it is now and  leave each "Dialect" to define it's own extended definition of it's view . 









