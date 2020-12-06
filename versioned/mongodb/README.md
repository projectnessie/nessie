# MongoDB Sharding Configuration Steps

These instructions indicate how MongoDB should be set up to allow sharding of the collections used by Nessie,
and assume that MongoDB has already been installed and configured with shards. If not, see
https://docs.mongodb.com/manual/tutorial/deploy-shard-cluster/.

## Configure sharding

Open the mongo shell, connected to your sharded Mongo instance, and run the following commands:
1. `sh.enableSharding("<database>")`
2. `sh.shardCollection("<database>.l1", {"id":"hashed"} )`
3. `sh.shardCollection("<database>.l2", {"id":"hashed"} )`
4. `sh.shardCollection("<database>.l3", {"id":"hashed"} )`
5. `sh.shardCollection("<database>.ref", {"id":"hashed"} )`
6. `sh.shardCollection("<database>.value", {"id":"hashed"} )`
7. `sh.shardCollection("<database>.key_fragment", {"id":"hashed"} )`
8. `sh.shardCollection("<database>.commit_metadata", {"id":"hashed"} )`

where `<database>` is the database that you would like Nessie to use, which also needs to be set
in the Nessie configuration.

**Note:** The collection names can also be changed via Nessie configuration, and if so the above
commands must be changed to reflect the configuration that is used.