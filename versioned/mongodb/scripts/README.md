# MongoDB Sharding Configuration Steps

This directory contains scripts to shard an existing MongoDB instance.

# Initial set up - Ensure MongoDb is installed

1. These instructions assume that MongoDB has already been installed.

# Configure sharding

1. In a terminal window set to this directory run:
1. sh configure_sharding.sh

This will configure sharding using default ports for:
Mongos             : 27017
Shared Replica Sets: 27018
Config Replica Sets: 27019

This script assumes the mongod and mongos are running on localhost.
This script assumes the replica set is named: "rs0". The bash script need to be updated to change this name.

