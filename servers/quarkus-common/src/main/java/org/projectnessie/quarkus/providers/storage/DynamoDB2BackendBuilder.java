/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.quarkus.providers.storage;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.DYNAMODB2;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.projectnessie.quarkus.config.QuarkusDynamoDBConfig;
import org.projectnessie.quarkus.providers.versionstore.StoreType;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.dynamodb2.DynamoDB2BackendConfig;
import org.projectnessie.versioned.storage.dynamodb2.DynamoDB2BackendFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@StoreType(DYNAMODB2)
@Dependent
public class DynamoDB2BackendBuilder implements BackendBuilder {

  @Inject DynamoDbClient client;

  @Inject QuarkusDynamoDBConfig dynamoDBConfig;

  @Override
  public Backend buildBackend() {
    DynamoDB2BackendFactory factory = new DynamoDB2BackendFactory();
    DynamoDB2BackendConfig c =
        DynamoDB2BackendConfig.builder()
            .client(client)
            .tablePrefix(dynamoDBConfig.tablePrefix())
            .build();
    return factory.buildBackend(c);
  }
}
