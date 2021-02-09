#
# Copyright (C) 2020 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

## THIS IS NOT A SCRIPT THAT CAN BE BLINDLY EXECUTED!
## THIS IS JUST AN ILLUSTRATION!

###############################################################################################
##
## These aws-cli invocations just briefly/roughly illustrate the needed permissions
##
## You need a AWS IAM group, for example called dynamodb-full, that has full access
## permissions to DynamoDB (read/write/create-tables).
##
## You also need a user in AWS IAM for which an AWS Access Key can be created. The user's called
## "nessie-perftest" in this example.

IAM_USER=nessie-perftest
IAM_GROUP=dynamodb-full

aws iam create-user --user-name ${IAM_USER}
aws iam add-user-to-group --group-name ${IAM_GROUP} --user-name ${IAM_USER}
aws iam create-access-key --user ${IAM_USER}

###############################################################################################

# Setup the credentials for the Nessie instance being run using the values returned by "create-access-key"
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx

###############################################################################################

# When done with perf-testing, delete the access key
aws iam delete-access-key --access-key-id ${AWS_ACCESS_KEY} --user-name ${IAM_USER}
