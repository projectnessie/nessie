# List existing mongo process
pgrep mongo
# Create replica set 0
. rs.sh 0
# Create replica set 1
. rs.sh 1
# Create replica set 2
. rs.sh 2
pgrep mongo
# Connect to available host
# mongo --host localhost --port 27017
# then run: rs.initiate({_id:"rs0", members: [{_id:0, host:"localhost:27017", priority:100}, {_id:1, host:"localhost:27018", priority:50}, {_id:2, host:"localhost:27019", arbiterOnly:true}]})