/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.persist.dynamodb;

import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tests.extension.AbstractTestConnectionProviderSource;

/**
 * DynamoDB test connection-provider source using an external DynamoDB instance/cluster.
 *
 * <p>See {@link DynamoClientConfig} for configuration options.
 */
public class DynamoTestConnectionProviderSource
    extends AbstractTestConnectionProviderSource<DynamoClientConfig> {

  @Override
  public boolean isCompatibleWith(
      DatabaseAdapterConfig adapterConfig, DatabaseAdapterFactory<?, ?, ?> databaseAdapterFactory) {
    return adapterConfig instanceof NonTransactionalDatabaseAdapterConfig
        && databaseAdapterFactory instanceof DynamoDatabaseAdapterFactory;
  }

  @Override
  public DynamoClientConfig createDefaultConnectionProviderConfig() {
    return ImmutableDefaultDynamoClientConfig.builder().build();
  }

  @Override
  public DynamoDatabaseClient createConnectionProvider() {
    return new DynamoDatabaseClient();
  }
}
