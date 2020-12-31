



## Schema definition for a View
This is proposed  specification for a  schema definition of a view.  This is based on what a Dremio view schema would need to save along with the SQL text string describing the view. The goal is this schema could be used generally as well. 
Described is an outline of the classes for the types and attributes for the view fields. 

(This schema is a data member of the DremioView definition )

A ViewSchema is a list of ViewFields and maintains a versionId to handle future version changes if any.

    public final class ViewSchema implements Externalizable, Message<ViewSchema>, Schema<ViewSchema>  {      
        private Integer versionId;  
        private List<ViewField> field;
     }
A ViewField describes the attributes of the field - Simple, Struct, List, Map types are covered.

    public final class ViewField implements  Externalizable, Message<ViewField>, Schema<ViewField> {    
        public static final class SimpleFieldType implements Externalizable, Message<SimpleFieldType>, Schema<SimpleFieldType>  {  
          private String name;  //name of the field
          private SqlTypeName type;  // enum type  maps to  the field (from Calcite [SqlTypeName.](https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/type/SqlTypeName.html)
          private Integer precision;  //precision of the type
          private Integer scale;  // scale of the type
          private String startUnit;  //Interval type start unit- valid only for interval types
          private String endUnit;  //Interval type end unit valid only for interval types
          private Integer fractionalSecondPrecision;  // fractional precision (range 0 to 6)
          private Boolean isNullable; // is field nullable or not
       } 
       
         public enum TypeFamily {  
           SIMPLE, LIST,  STRUCT, MAP 
         }  
     
         public static final class StructType {     
           private String fieldName;  
           private ViewField.SimpleFieldType fieldType; 
         }
          
         public static final class ListType {      
           private ViewField.SimpleFieldType listElement;
         }
        
         public static final class MapType {
          private ViewField.SimpleFieldType keyType;  
          private ViewField.SimpleFieldType valueType;  
          private Boolean isNullableValue;
         }
   
       int versionId ; // version of the viewfield
       TypeFamily type; // type family it belongs to
       SimpleFieldType simpleField; // non null for simple field else null.
       ListType listField;// non null for a List field else null
       StructType structField;// non null for struct field else null;
       MapType mapField;// non null for map field else null
     }

