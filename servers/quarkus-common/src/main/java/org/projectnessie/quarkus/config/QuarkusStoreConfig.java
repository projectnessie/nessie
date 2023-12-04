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

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import org.projectnessie.versioned.storage.common.config.StoreConfig;

@StaticInitSafe
@ConfigMapping(prefix = "nessie.version.store.persist")
public interface QuarkusStoreConfig extends StoreConfig {

  @WithName(CONFIG_REPOSITORY_ID)
  @WithDefault(DEFAULT_REPOSITORY_ID)
  // Use RepoIdConverter for the "key-prefix" property because it can be an empty string,
  // but the default converter will turn empty strings into `null`.
  @WithConverter(RepoIdConverter.class)
  @Override
  String repositoryId();

  @WithName(CONFIG_COMMIT_RETRIES)
  @WithDefault("" + DEFAULT_COMMIT_RETRIES)
  @Override
  int commitRetries();

  @WithName(CONFIG_COMMIT_TIMEOUT_MILLIS)
  @WithDefault("" + DEFAULT_COMMIT_TIMEOUT_MILLIS)
  @Override
  long commitTimeoutMillis();

  @WithName(CONFIG_RETRY_INITIAL_SLEEP_MILLIS_LOWER)
  @WithDefault("" + DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_LOWER)
  @Override
  long retryInitialSleepMillisLower();

  @WithName(CONFIG_RETRY_INITIAL_SLEEP_MILLIS_UPPER)
  @WithDefault("" + DEFAULT_RETRY_INITIAL_SLEEP_MILLIS_UPPER)
  @Override
  long retryInitialSleepMillisUpper();

  @WithName(CONFIG_RETRY_MAX_SLEEP_MILLIS)
  @WithDefault("" + DEFAULT_RETRY_MAX_SLEEP_MILLIS)
  @Override
  long retryMaxSleepMillis();

  @WithName(CONFIG_PARENTS_PER_COMMIT)
  @WithDefault("" + DEFAULT_PARENTS_PER_COMMIT)
  @Override
  int parentsPerCommit();

  @WithName(CONFIG_MAX_SERIALIZED_INDEX_SIZE)
  @WithDefault("" + DEFAULT_MAX_SERIALIZED_INDEX_SIZE)
  @Override
  int maxSerializedIndexSize();

  @WithName(CONFIG_MAX_INCREMENTAL_INDEX_SIZE)
  @WithDefault("" + DEFAULT_MAX_INCREMENTAL_INDEX_SIZE)
  @Override
  int maxIncrementalIndexSize();

  @WithName(CONFIG_MAX_REFERENCE_STRIPES_PER_COMMIT)
  @WithDefault("" + DEFAULT_MAX_REFERENCE_STRIPES_PER_COMMIT)
  @Override
  int maxReferenceStripesPerCommit();

  @WithName(CONFIG_ASSUMED_WALL_CLOCK_DRIFT_MICROS)
  @WithDefault("" + DEFAULT_ASSUMED_WALL_CLOCK_DRIFT_MICROS)
  @Override
  long assumedWallClockDriftMicros();

  @WithName(CONFIG_NAMESPACE_VALIDATION)
  @WithDefault("" + DEFAULT_NAMESPACE_VALIDATION)
  @Override
  boolean validateNamespaces();

  @WithName(CONFIG_PREVIOUS_HEAD_COUNT)
  @WithDefault("" + DEFAULT_PREVIOUS_HEAD_COUNT)
  @Override
  int referencePreviousHeadCount();

  @WithName(CONFIG_PREVIOUS_HEAD_TIME_SPAN_SECONDS)
  @WithDefault("" + DEFAULT_PREVIOUS_HEAD_TIME_SPAN_SECONDS)
  @Override
  long referencePreviousHeadTimeSpanSeconds();

  String CONFIG_CACHE_CAPACITY_MB = "cache-capacity-mb";

  @WithName(CONFIG_CACHE_CAPACITY_MB)
  OptionalInt cacheCapacityMB();

  String CONFIG_CACHE_CAPACITY_FRACTION_MIN_SIZE_MB = "cache-capacity-fraction-min-size-mb";

  @WithName(CONFIG_CACHE_CAPACITY_FRACTION_MIN_SIZE_MB)
  OptionalInt cacheCapacityFractionMinSizeMb();

  String CONFIG_CACHE_CAPACITY_FRACTION_OF_HEAP = "cache-capacity-fraction-of-heap";

  @WithName(CONFIG_CACHE_CAPACITY_FRACTION_OF_HEAP)
  OptionalDouble cacheCapacityFractionOfHeap();

  String CONFIG_CACHE_CAPACITY_FRACTION_ADJUST_MB = "cache-capacity-fraction-adjust-mb";

  @WithName(CONFIG_CACHE_CAPACITY_FRACTION_ADJUST_MB)
  OptionalInt cacheCapacityFractionAdjustMB();
}
