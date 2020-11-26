# List existing mongo process
pgrep mongo
# Kill existing process if has.
pkill mongo
# Clear old folder if has.
rm -rf rs0 rs1 rs2 cfg0 cfg1 cfg2
# Create log folder.
mkdir -p ./var/log/mongodb
# Create config replica set 0
. cfg.sh 0
# Create config replica set 1
. cfg.sh 1
# Create config replica set 2
. cfg.sh 2
# Connect to available host
# mongo --host locahost --port 27027
# then run: rs.initiate({_id:"rs0", configsvr: true, members: [{_id:0, host:"localhost:27027", priority:100}, {_id:1, host:"localhost:27028", priority:50}, {_id:2, host:"localhost:27029", arbiterOnly:true}]})