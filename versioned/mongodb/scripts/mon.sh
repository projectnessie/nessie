#!/bin/bash
RS_NAME="mon"
mkdir $RS_NAME
PORT=$((27037))
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
sharding:
  configDB: configsvr/localhost:27027,localhost:27028,localhost:27029
EOF
mongos --config $RS_NAME.conf
