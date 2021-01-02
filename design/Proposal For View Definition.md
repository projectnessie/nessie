



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
While a View is ultimately describe by just the SQL query , these  additional fields needed to describe a view (or what we refer to as a Virtual Dataset in the context of Dremio) for the DREMIO dialect . 

   
**Generic Vs Implementation specific View Definition**
Some fields are  generic enough that  we could create a  definition common to view definitions of any dialect. Each "Dialect" can  then define its own extended fields needed for maintaining metadata for its internal implementation of a view (shown below for Dremio) .

**Generic fields**

    message PSqlView {
    
    required string sqlText = 1 ; // SQL string  that defines the view
    
    string dialect = 2 ; // Describes the type of system this view is defined on - eg hive, presto, dremio etc. 
    
    optional ViewSchema schema = 3; // Details on the fields/column of the view, the datatypes, precision and scale. (See definition of ViewSchema)
    
    repeated string context = 4; // Defines the folder, space the view is defined on. Represented as list of strings which are keys  to container objects in Nessie.Eg ["@myspace","myfolder","mysubfolder"] or ["mycatalog", "myschema"]
       
    }
    

**Dremio specific  fields** 

    message PDremioView {
    
     required int64 datasetId  = 1; // A unique ID , generated when the view  is created to identify this dataset that could be used to associate/link  this view to other system objects specific to Dremio  -eg reflections.
        
     optional int64 previousDatasetId = 2 ; //  To keep track of the previous edits to  the view until it is saved. For a saved view this could be NULL.  This is to mark/track system ephemeral commits.
       
     repeated FieldOrigin fieldOrigins = 4; // Describes the origin or resolved field for the  underlying table or view this view is being defined on and if the field is a derived expression.  This information is obtained after the SQL query text has been compiled. 
     
     repeated string tags = 5; // List of tags to describe the view. Can be NULL if no tags are defined. Tags are short strings which can be used to mark or classify views. Tags can be added via a REST API (via UI)  to existing view definitions or  "Alter VDS" command. 
        
     }

 Resolved field definition - i.e which table and column it was defined on or derived from . 

    message FieldOrigin {
    
    required string name = 1; // Name of the field . Can be a rename of an underlying column of the table  or an expression on the underlying base table columns  like : basetable.column1+basetable.column2.
    
    repeated Origin origins = 2; //  Underlying column or columns this field is defined on. 
    
    required boolean derived = 3 ; //  True if the field is derived from the underlying tables column 
        
    }
Underlying base table and column information

   

    message Origin {
        
     repeated string table = 1; // Underlying base table name 
        
     required string columnName = 2; // Column name from the underlying table.  
    
    }
