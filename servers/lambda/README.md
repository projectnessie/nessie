# Deploy to AWS

## Preliminaries

You should have a `sam` executable on your path. Installation instructions
[here](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html).
You must also have access to an aws account and permissions to create IAM roles, DynamoDb tables, lambda functions and
an API Gateway. You must also have an S3 bucket for storing lambda code in.

## Run SAM

Either run `mvn clean install -DawsRegion=us-west-2 -Ds3Bucket=nessie-test-deploy` 
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
5. Two IAM policies: `NessieUserRole` and `NessieAdminRole`. Users should have these policies associated
  with them depending on whether they have read only or read/write access to Nessie.
  
  
## Apply Policies to users

The admin and user policies must be added to users in AWS. The admin role gives full access to Nessie:
 * create, delete branches
 * perform commits
 * merge branches
 * read tables and branches
 * list tables and branches
 
The user role allows:
 * list tables and branches
 * read tables and branches
 
## Access Nessie

The `sam` deployment will create a url https://<gatewayid>.execute-api.<region>.amazonaws.com/Prod/api/v1/. 
This is the standard enpoint for all actions on Nessie. The nessie client must be configured to use the AWS auth module.

* for iceberg and delta lake: set `nessie.auth.type=aws`
* for python: set `auth: aws` in `config.yaml`

This will look for credentials in the standard places (env variables, system params, etc) and use them 
to authenticate against the gateway. If the user credentials don't have the correct IAM role you will get 
401 errors. Ensure the user has been grated one of the above policies.
