/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.cleanup;

import static org.projectnessie.versioned.storage.common.logic.InternalRef.REF_REFS;
import static org.projectnessie.versioned.storage.common.logic.InternalRef.REF_REPO;
import static org.projectnessie.versioned.transfer.related.CompositeTransferRelatedObjects.createCompositeTransferRelatedObjects;

import java.util.List;
import java.util.function.IntFunction;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.transfer.related.TransferRelatedObjects;

/**
 * Technically and implementation oriented parameters for Nessie's backend database cleanup,
 * considered for internal use only.
 *
 * <p>Any API or functionality that exposes Nessie's backend database cleanup must provide a
 * functionally oriented way for configuration and generate a {@link CleanupParams} from it.
 */
@NessieImmutable
public interface CleanupParams {
  // Following defaults result in a serialized bloom filter size of about 3000000 bytes.
  long DEFAULT_EXPECTED_OBJ_COUNT = 1_000_000L;
  double DEFAULT_FALSE_POSITIVE_PROBABILITY = 0.00001d;
  double DEFAULT_ALLOWED_FALSE_POSITIVE_PROBABILITY = 0.0001d;
  boolean DEFAULT_ALLOW_DUPLICATE_COMMIT_TRAVERSALS = false;
  int DEFAULT_PENDING_OBJS_BATCH_SIZE = 20;
  int DEFAULT_RECENT_OBJ_IDS_FILTER_SIZE = 100_000;

  static ImmutableCleanupParams.Builder builder() {
    return ImmutableCleanupParams.builder();
  }

  /**
   * Number of expected {@link Obj}s, defaults to {@value #DEFAULT_EXPECTED_OBJ_COUNT}, used to size
   * the bloom filter identifying the referenced {@link Obj}s. If {@link
   * ReferencedObjectsResolver#resolve()} throws {@link MustRestartWithBiggerFilterException}, it is
   * recommended to increase this value.
   */
  @Value.Default
  default long expectedObjCount() {
    return DEFAULT_EXPECTED_OBJ_COUNT;
  }

  /**
   * Returns an updated instance of {@code this} value with {@link #expectedObjCount()} increased by
   * {@value #DEFAULT_EXPECTED_OBJ_COUNT} as a convenience function to handle {@link
   * MustRestartWithBiggerFilterException} thrown by {@link ReferencedObjectsResolver#resolve()} .
   */
  default CleanupParams withIncreasedExpectedObjCount() {
    return builder()
        .from(this)
        .expectedObjCount(expectedObjCount() + DEFAULT_EXPECTED_OBJ_COUNT)
        .build();
  }

  /**
   * Related to {@link #expectedObjCount()}, used to size the bloom filter identifying the
   * referenced {@link Obj}s, defaults to {@value #DEFAULT_FALSE_POSITIVE_PROBABILITY}.
   */
  @Value.Default
  default double falsePositiveProbability() {
    return DEFAULT_FALSE_POSITIVE_PROBABILITY;
  }

  /**
   * Maximum allowed FPP, checked when adding to the bloom filter identifying the referenced {@link
   * Obj}s, defaults to {@value #DEFAULT_ALLOWED_FALSE_POSITIVE_PROBABILITY}. If this value is
   * exceeded, a {@link MustRestartWithBiggerFilterException} will be thrown from {@link
   * ReferencedObjectsResolver#resolve()}.
   */
  @Value.Default
  default double allowedFalsePositiveProbability() {
    return DEFAULT_ALLOWED_FALSE_POSITIVE_PROBABILITY;
  }

  /** Helper functionality to identify related {@link Obj}s, see {@link TransferRelatedObjects}. */
  @Value.Default
  default TransferRelatedObjects relatedObjects() {
    return createCompositeTransferRelatedObjects();
  }

  /**
   * {@link ReferencedObjectsResolver} tries to not walk a commit more than once by memoizing the
   * visited {@link CommitObj#id() commit IDs}, default is {@link
   * #DEFAULT_ALLOW_DUPLICATE_COMMIT_TRAVERSALS}. Setting this to {@code true} disables this
   * optimization.
   */
  @Value.Default
  default boolean allowDuplicateCommitTraversals() {
    return DEFAULT_ALLOW_DUPLICATE_COMMIT_TRAVERSALS;
  }

  /**
   * Rate limit for commit objects per second during {@link ReferencedObjectsResolver#resolve()},
   * default is unlimited. Any positive value enables rate limiting, any value {@code <=0} disables
   * rate limiting.
   */
  @Value.Default
  default int resolveCommitRatePerSecond() {
    return 0;
  }

  /**
   * Rate limit for (non commit) objects per second during {@link
   * ReferencedObjectsResolver#resolve()}, default is unlimited. Any positive value enables rate
   * limiting, any value {@code <=0} disables rate limiting.
   */
  @Value.Default
  default int resolveObjRatePerSecond() {
    return 0;
  }

  /**
   * Rate limit for scanning objects per second during {@link PurgeObjects#purge()}, default is
   * unlimited. Any positive value enables rate limiting, any value {@code <=0} disables rate
   * limiting.
   */
  @Value.Default
  default int purgeScanObjRatePerSecond() {
    return 0;
  }

  /**
   * Rate limit for purging objects per second during {@link PurgeObjects#purge()}, default is
   * unlimited. Any positive value enables rate limiting, any value {@code <=0} disables rate
   * limiting.
   */
  @Value.Default
  default int purgeDeleteObjRatePerSecond() {
    return 0;
  }

  /**
   * {@link ReferencedObjectsResolver} attempts to fetch objects from the backend database in
   * batches, this parameter defines the batch size, defaults to {@link
   * #DEFAULT_PENDING_OBJS_BATCH_SIZE}.
   */
  @Value.Default
  default int pendingObjsBatchSize() {
    return DEFAULT_PENDING_OBJS_BATCH_SIZE;
  }

  /**
   * Size of the "recent object IDs" filter to prevent processing the same {@link ObjId}s. This *
   * happens, when the values referenced from the commit index are iterated, because it iterates *
   * over all keys, not only the keys added by a particular commit.
   *
   * <p>The value defaults to {@value #DEFAULT_RECENT_OBJ_IDS_FILTER_SIZE}. It should be higher than
   * the maximum number of keys in a commit.
   */
  @Value.Default
  default int recentObjIdsFilterSize() {
    return DEFAULT_RECENT_OBJ_IDS_FILTER_SIZE;
  }

  /** Rate limiter factory for the rate limits defined above, useful for testing purposes. */
  @Value.Default
  default IntFunction<RateLimit> rateLimitFactory() {
    return RateLimit::create;
  }

  /** Defines the names of the Nessie internal references, do not change. */
  @Value.Default
  default List<String> internalReferenceNames() {
    return List.of(REF_REFS.name(), REF_REPO.name());
  }

  /**
   * Optionally enable a dry-run mode, which does not delete any objects from the backend database,
   * defaults to {@code false}.
   */
  @Value.Default
  default boolean dryRun() {
    return false;
  }
}
