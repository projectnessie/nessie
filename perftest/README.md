# Nessie Perf test

## Local test

To Run:

* set up [docker compose] (https://docs.docker.com/compose/install/)
* execute service `docker-compose up -d --scale nessie=5`. This will start:
  - 5 concurrent nessie servers
  - [grafana](http://localhost:3000) - for plotting prometheus metrics
  - [prometheus](http://localhost:9090) - collecting load, dynamo usage etc from server
  - [nginx](http://localhost:19131) load balancer to round robin the 5 servers
  - [jaeger](http://localhost:16686) - trace executions
  - cadvisor - docker image and host metrics
  - localstack - for dynamodb and future AWS services
* build project (`mvn clean install`)  
* enter `perftest` directory and execute `mvn exec:java jmeter:jmeter -Dnessie.jmeter.users=10 -Dnessie.jmeter.queries=100 -Dnessie.jmeter.dbsize=10000 -Dnessie.jmeter.path=http://localhost:19131/api/v1`
* have a look at `target/jmeter/results` for results and check grafana and jaeger for timings


## AWS test

To Run:

These instructions are for running nessie on an EC2 instance and using dynamodb. To only use dynamo db go directly to item 3

1. create tables in Dynamo (NessieGitRefDatabase, NessieGitObjectDatabase)
1. start up an EC2 instance. Should be big but not huge. Open ports (3000, 9090, 19131, 16686)
1. ensure EC2 instance has r/w on Dynamo via IAM role
1. edit docker compopse to change the `nessie` env variables for dynamo region and [endpoint](https://docs.aws.amazon.com/general/latest/gr/ddb.html).
    - NESSIE_DB_PROPS_REGION=us-west-2
    - NESSIE_DB_PROPS_ENDPOINT=https://dynamodb.us-west-2.amazonaws.com
1. set up [docker compose] (https://docs.docker.com/compose/install/)
1. execute service `docker-compose up -d --scale nessie=5 --scale localstack=0`. This will start:
    - 5 concurrent nessie servers
    - [grafana](http://localhost:3000) - for plotting prometheus metrics
    - [prometheus](http://localhost:9090) - collecting load, dynamo usage etc from server
    - [nginx](http://localhost:19131) load balancer to round robin the 5 servers
    - [jaeger](http://localhost:16686) - trace executions
    - cadvisor - docker image and host metrics
1. build project (`mvn clean install`)  
1. enter `perftest` directory and execute `mvn exec:java jmeter:jmeter -Dnessie.jmeter.users=10 -Dnessie.jmeter.queries=100 -Dnessie.jmeter.dbsize=10000 -Dnessie.jmeter.path=http://<ec2_host>:19131/api/v1 -Dnessie.dynamo.region=us-west-2 -Dnessie.dynamo.endpoint=https://dynamodb.us-west-2.amazonaws.com`
1. have a look at `target/jmeter/results` for results and check grafana and jaeger for timings

## ECS test

**todo** docker compose should work with ECS, will test and add instructions here
