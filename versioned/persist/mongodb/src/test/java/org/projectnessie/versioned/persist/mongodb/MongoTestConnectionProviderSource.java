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
package org.projectnessie.versioned.persist.mongodb;

import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tests.extension.AbstractTestConnectionProviderSource;

/**
 * MongoDB test connection-provider source using an external MongoDB instance/cluster.
 *
 * <p>See {@link MongoClientConfig} for configuration options.
 */
public class MongoTestConnectionProviderSource
    extends AbstractTestConnectionProviderSource<MongoClientConfig> {

  @Override
  public boolean isCompatibleWith(
      DatabaseAdapterConfig adapterConfig, DatabaseAdapterFactory<?, ?, ?> databaseAdapterFactory) {
    return adapterConfig instanceof NonTransactionalDatabaseAdapterConfig
        && databaseAdapterFactory instanceof MongoDatabaseAdapterFactory;
  }

  @Override
  public MongoClientConfig createDefaultConnectionProviderConfig() {
    return ImmutableMongoClientConfig.builder().build();
  }

  @Override
  public MongoDatabaseClient createConnectionProvider() {
    return new MongoDatabaseClient();
  }
}
