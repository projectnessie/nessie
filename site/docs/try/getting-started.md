# Nessie Server

## Deployment options

There are several ways to deploy and run a Nessie server:
* Java server
* Jersey REST service
* docker image
* Lambda service

### Java server

The code itself can be run simply by running `mvn clean install` to build the project and then `mvn exec:exec -pl
:nessie-distribution` to run it. Configuration is set in `distribution/src/main/resources/config.yaml`. By default the
process is started on port `19120` and uses the DynamoDB for a backend and the testing only basic user service for
authentication.

### Jersey REST Service

The `RestServerV1` class located in `server/src/main/java/com/dremio/nessie/server/` can be added directly into an
existing Jersey server and integrated into any other application. A `config.yaml` must be on the classpath for the
server to start up correctly.

### Docker image

The `mvn clean install` step will build a docker image (using Googles [Jib](https://github.com/GoogleContainerTools/jib)
) in `distribution/target/jib-image.jar`. This can be loaded into docker via `docker load -i <path to jib.jar>`. The
image can be started via `docker run -p 19210:19120 nessie`. Environment variables can be passed here to set config
options. Environment variables are specified with the `NESSIE_` prefix and `_` separated all caps equivalent of the
`config.yaml` options.

!!! warning
    **TODO:** allow for mounting a local config.yaml to docker image

!!! note
    **TODO:** push docker image to dockerhub


### Lambda

You should have a `sam` executable on your path. Installation instructions
[here](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html).
You must also have access to an aws account and permissions to create IAM roles, DynamoDb tables, lambda functions and
an API Gateway. You must also have an S3 bucket for storing lambda code in.

#### Quarkus

The Lambda image uses [Quarkus](https://quarkus.io) to generate a graal native image. This greatly reduces cold start
times on AWS Lambda and simplifies deployment. You must have download a GraalVM and set `GRAAL_HOME` to build the native
image. This has been tested on graalvm version `20.1.0`.

#### Run SAM

Either run `mvn clean install -P native,serverless-deploy -DawsRegion=us-west-2 -Ds3Bucket=nessie-test-deploy` 
which will execute `sam package` and create `serverless/target/sam.yaml` or run
`sam package --template-file serverless/sam.yaml --s3-bucket nessie-test-deploy --output-template-file out.yaml`. 
This will create and upload the lambda package to AWS.

Either run `mvn deploy -DawsRegion=us-west-2 -DstackName=nessie-test-sam` or 
`sam deploy --template-file out.yaml --stack-name nessie-test-sam --capabilities CAPABILITY_NAMED_IAM`
to deploy the stack. This will deploy:
1. lambda function version of Nessie
2. API gateway protected by IAM roles
3. Required dynamodb tables
4. Required IAM Role for lambda function
5. Two IAM policies: `NessieUserRole` and `NessieAdminRole`. 
6. Two IAM groups: `NessieUserGroup` and `NessieAdminGroup`. Users should be added to these groups 
  depending on whether they have read only or read/write access to Nessie.


#### Apply Policies to users

The admin and user groups must be added to users in AWS. The admin role gives full access to Nessie:
 * create, delete branches
 * perform commits
 * merge branches
 * read tables and branches
 * list tables and branches

The user role allows:
 * list tables and branches
 * read tables and branches

#### Access Nessie

The `sam` deployment will create a url https://<gatewayid>.execute-api.<region>.amazonaws.com/Prod/. 
This is the standard enpoint for all actions on Nessie. The nessie client must be configured to use the AWS auth module.

* for iceberg and delta lake: set `nessie.auth.type=aws`
* for python: set `auth: aws` in `config.yaml`

This will look for credentials in the standard places (env variables, system params, etc) and use them 
to authenticate against the gateway. If the user credentials don't have the correct IAM role you will get 
401 errors. Ensure the user has been grated one of the above policies.
