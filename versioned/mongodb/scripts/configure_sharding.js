/*
Copyright (C) 2020 Dremio

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

print("Running configure_sharding.js");

use databaseName
sh.addShard(replicaSetname+"/localhost:27018")
sh.enableSharding(databaseName)
sh.shardCollection(databaseName+".nessie_l1", {"id":"hashed"} )
sh.shardCollection(databaseName+".nessie_l2", {"id":"hashed"} )
sh.shardCollection(databaseName+".nessie_l3", {"id":"hashed"} )
sh.shardCollection(databaseName+".nessie_ref", {"id":"hashed"} )
sh.shardCollection(databaseName+".nessie_value", {"id":"hashed"} )
sh.shardCollection(databaseName+".nessie_key_fragment", {"id":"hashed"} )
sh.shardCollection(databaseName+".nessie_commit_metadata", {"id":"hashed"} )
