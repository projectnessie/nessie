# Commit Kernel

Nessie's production commit kernel is optimized to provide high commit 
throughput against a distributed key value store that provides record-level ACID 
guarantees. Today, this kernel is built on top of DynamoDB. The commit kernel is the 
heart of Nessie's oeprations and enables it to provide lightweight creation of new 
tags and branches, merges, rebases all with a very high concurrent commit rate. 

## Data Structures

Nessie's commit kernel works with two tables: the refs table and the objects table. 

Refs Table
: The refs table will have objects equal to the current number of active tags and branches. 
  This will generally be small (10s-1000s). All commits run through this table and thus 
  the writes and reads of this table should be provisioned based on the amount of read 
  and write operations expected per second. Since the dataset is small, sharding will 
  be unlikely to happen on this table. Scans are regularly done on this table.

Objects Table
: The objects table stores data structures associated with commits and will be a small 
multiple of the number of objects and version tracked in Nessie. Assume approximately 
`number objects` x `active versions` x `4` (write multiplier) to get a rough sense 
of the number of 4kb objects that Nessie will be storing in the objects table. Scans 
are not done on this table except in the case of rare Garbage Collection operations. 


Space Consumption: The data stored in DynamoDB is designed to largely fit within DynamoDBs 4kb read size 
unit (and 4x the write size unit). This true for objects in both the refs table and 
the objects table.

At a high level, Nessie's commit kernel breaks keyspace into 
a 3 level tree and each operation restates some portion of that tree. Each level of 
the tree is identified by sha256 20 byte hash value, which is used to as a storage 
key in DynamoDB. Commit operations are atomic and interact with branch objects using 
a data structure akin to a 151-way striped locked. This data structure is updated using 
patch conditional operations against DynamoDB. To maintain linear history, the 
branch object also maintains a commit log that works with the striped lock to make 
history clear and consistent. 


## Performance

The design target for the Nessie commit kernel was to support 100,000 tables with each 
table mutating every 5 minutes. This equates to ~300 operations/second. Tests have 
shown the Nessie commit kernel on DynamoDB to be able to exceed that by a large margin. We 
will publish a more comprehensive benchmark soon.