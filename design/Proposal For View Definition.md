



## Proposal to extend the View Definition and Schema
**Background**

This is the current  interface for a SqlView which is a first class object in Nessie specifies a Sql text and a Dialect  and schema :

    public interface SqlView extends Contents {  
      
      public static enum Dialect {  
        HIVE,  
        SPARK,  
        DREMIO,  
        PRESTO  
      }  
      
      String getSqlText();  
      
      Dialect getDialect();  
      
      Schema getSchema();  
    }

**Enhancements**

While thinking of integrating Dremio with Nessie, we found we needed a few more fields to fully define a view along with  a schema .  Given all the feedback from the draft version and design goals of Nessie, the fields must meet the following criteria :
1. The core set of  fields must be well defined and understood by  any supported engine subsystem  

3. Interoperability being a goal, irrespective of the dialect, we must be able to execute a view that has a valid SQL query  text in any of the subsystems. One rule to enforce in the first version is not to make any assumptions about the default context of a table or underlying view so all references must be absolute.
4. If the schema for a view is not defined,  it should not be treated as a missing required field. The view text must be able t be  compiled and executed.  Optionally we must be able to generate and update  the schema and the updated View contents (with the newly generated schema)  can  be  committed to Nessie 
5. If a view schema is out of date or inconsistent,  it is detected at planning time . The SQL text  takes precedence and the view will get compiled and schema can optionally get updated as a new commit in Nessie. 
6. If any specialized fields are added within the specific  engine subsystems properties, they could be  fields needed specifically for that one engine and  not required or relevant for successful execution of the view in other engines. Some examples are fields needed to be cached for performance. 
 

**Future enhancements** 

 Given the goal of interoperability of views, as a future goal, the view SQL text may be using builtin functions or UDFs that need translation between systems. So a separate module could provide a view translation and rewrite layer to translate to specific engines. This layer would understand the supported engines and the differences in syntax and native APIs  for each of  these engines. With the first version we will not assume any translation of the SQL text. 

**Proposal for additional fields to current SqlView object **
While a View is ultimately described by just the SQL query , there are   additional fields needed to describe a view . 
We  could create a  core set of fields  common to view definitions of any dialect. Each "Dialect" can  then define its own extended fields needed for maintaining metadata for its internal implementation of a view (shown below for Dremio for the tags field) .

**Generic fields**

   **SQL View **
   
    message PSqlView {
        
       string sqlText = 1 ; // SQL string  that defines the view. All table and view references must be fully qualified
       
       string resolvedSql = 2; //  ? 
        
       message PDialect {
         oneof DialectType {
           PHiveViewProperties hive = 1;
           PSparkViewProperties spark = 2;
           PDremioViewProperties dremio = 3;
           PPrestoViewProperties presto = 4;
          }
        }
        
       PDialect dialect = 3 ; // Describes the type of system this view is defined on - eg hive, presto, dremio etc. The sepcific properties can be set and used by each underlying system. (Should there be a PGeneric dialect too as default ? ) 
    
       Schema viewSchema = 4; // Optional. Details on the fields/column of the view, the datatypes, precision and scale. (See definition of ViewSchema) This field gets generated when the view sql string is compiled . The output of the compilation is saved for future execution of the view.
    
       repeated string viewContext = 5; // Defines the namspace or schema where this view is contained as a list of strings. Systems that have a hierarchical namespace  can define the path. Some examples : ["mycatalog", "myschema"] or ["@myspace","myfolder","mysubfolder"]  
        
       PWiki comment = 6; // Comment text field  that describes the view in plain text or markdown
    }
    
**Wiki object type.**
  This is a  new object  type for Nessie to store plain text or wiki markdown.  This object type could be generally used to store wiki content for any objects in Nessie. 
  
    message PWiki {
    
      enum PFormat { 
        PLAIN_TEXT = 0; 
        GIT_MARKDOWN = 1;
      }
      
      Ptype format = 1;
      
      string text = 2; 
     
    }

**View Schema** 
The View Schema is a list of field descriptors.  The schema is optional in the sense that it  can be derived from the SQL text . For performance , it should be saved. During compilation if the fields from the compiled relational expression of the SQL string does not match the saved schema, the Schema will  be updated and possibly committed as a new version. 
 
    message Schema {
     
      repeated FieldSchema = 1;
        
    }

For the format of FieldSchema, below are 2 existing options that can be leveraged. Both are widely used and would suit the  purpose of storing the view schema. 

 - Option 1
 
This is the hive definition of the FieldSchema . This is widely used , built for SQL use and well adopted. It uses Thrift for serialization which can be converted to a protobuf before storing. 
[Hive Field Schema](https://github.com/microsoft/hive-metastore-http-client/blob/master/thrift/hive_metastore.thrift#L39)
[Hive Type Factory](https://github.com/apache/hive/blob/d5ea2f3bb81cd992ce2cf6ad1da23fc4db67c471/serde/src/java/org/apache/hadoop/hive/serde2/typeinfo/TypeInfoFactory.java#L42)
 - Option 2 
 
This is the Arrow definition of the FieldSchema. Arrow is also a widely adopted generic format , not built specifically for SQL. It uses a FlatBuffer format. 
https://github.com/apache/arrow/blob/master/format/Schema.fbs
Note ** Arrow will need to be enhanced  to include precision for string datatype



**Dremio specific  fields** 
We propose to store tags  - a concept specific to Dremio to assign labels to a view . They are short 128 bytes strings.
In the future, if there are any fields needed to be stored for performance or features specific to Dremio, they will get added to this custom field.
 

    message PDremioViewProperties {    
          
      repeated string tags = 1; // List of tags to describe the view. Can be NULL if no tags are defined. Tags are short strings which can be used to mark or classify views. Tags can be added via a REST API (via UI)  to existing view definitions or  "Alter VDS" command. 
             
    }






