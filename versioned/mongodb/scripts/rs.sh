#!/bin/bash
RS_INDEX=$1
RS_NAME="rs"$1
mkdir $RS_NAME
PORT=$((27017+$RS_INDEX))
cat <<EOF > $RS_NAME.conf
systemLog:
   destination: file
   path: "var/log/mongodb/mongod.log"
   logAppend: true
storage:
   dbPath: "$RS_NAME"
   journal:
      enabled: true
processManagement:
   fork: true
net:
   bindIp: localhost
   port: $PORT
setParameter:
   enableLocalhostAuthBypass: true
replication:
   oplogSizeMB: 10
   replSetName: "rs0"
sharding:
  clusterRole: shardsvr
EOF
mongod --config $RS_NAME.conf
