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
package org.projectnessie.quarkus.config;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tx.TxDatabaseAdapterConfig;

/**
 * This is a superset of all database adapter configuration interfaces to be implemented by Quarkus.
 *
 * <p>All adapter configuration properties are assumed to be optional or have default values.
 * Therefore, combining all of them in one Quarkus configuration object should not cause any errors
 * even when only a sub-set of the values is defined in runtime.
 *
 * <p>This interface overrides all getters to assign explicit Quarkus configuration names and
 * default values to them.
 */
@StaticInitSafe
@ConfigMapping(prefix = "nessie.version.store.advanced")
@RegisterForReflection(targets = RepoIdConverter.class)
public interface QuarkusVersionStoreAdvancedConfig
    extends NonTransactionalDatabaseAdapterConfig, TxDatabaseAdapterConfig {

  @WithName("repository-id")
  @WithDefault(DEFAULT_REPOSITORY_ID)
  // Use RepoIdConverter for the "key-prefix" property because it can be an empty string,
  // but the default converter will turn empty strings into `null`.
  @WithConverter(RepoIdConverter.class)
  @Override
  String getRepositoryId();

  @WithName("parent-per-commit")
  @WithDefault("" + DEFAULT_PARENTS_PER_COMMIT)
  @Override
  int getParentsPerCommit();

  @WithName("key-list-distance")
  @WithDefault("" + DEFAULT_KEY_LIST_DISTANCE)
  @Override
  int getKeyListDistance();

  @WithName("max-key-list-size")
  @WithDefault("" + DEFAULT_MAX_KEY_LIST_SIZE)
  @Override
  int getMaxKeyListSize();

  @WithName("commit-timeout")
  @WithDefault("" + DEFAULT_COMMIT_TIMEOUT)
  @Override
  long getCommitTimeout();

  @WithName("commit-retries")
  @WithDefault("" + DEFAULT_COMMIT_RETRIES)
  @Override
  int getCommitRetries();

  @WithName("retry.initial-sleep.millis-lower")
  @WithDefault("" + DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_LOWER)
  @Override
  long getRetryInitialSleepMillisLower();

  @WithName("retry.initial-sleep.millis-upper")
  @WithDefault("" + DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_UPPER)
  @Override
  long getRetryInitialSleepMillisUpper();

  @WithName("retry.max-sleep.millis")
  @WithDefault("" + DEFAULT_RETRY_MAX_SLEEP_MILLIS)
  @Override
  long getRetryMaxSleepMillis();

  @WithName("nontx.parents-per-global-commit")
  @WithDefault("" + DEFAULT_PARENTS_PER_GLOBAL_COMMIT)
  @Override
  int getParentsPerGlobalCommit();

  @WithName("tx.batch-size")
  @WithDefault("" + DEFAULT_BATCH_SIZE)
  @Override
  int getBatchSize();

  @WithName("parent-per-reflog-entry")
  @WithDefault("" + DEFAULT_PARENTS_PER_REFLOG_ENTRY)
  @Override
  int getParentsPerRefLogEntry();

  @WithName("tx.jdbc.catalog")
  @WithDefault("")
  @WithConverter(RepoIdConverter.class)
  String getJdbcCatalog();

  @WithName("tx.jdbc.schema")
  @WithDefault("")
  @WithConverter(RepoIdConverter.class)
  String getJdbcSchema();
}
