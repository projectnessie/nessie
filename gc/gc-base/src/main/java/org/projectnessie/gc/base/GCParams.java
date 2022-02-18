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
package org.projectnessie.gc.base;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** Config params for GC. */
@Value.Immutable
public interface GCParams extends Serializable {

  /** Nessie client configurations from {@link org.projectnessie.client.NessieConfigConstants}. */
  Map<String, String> getNessieClientConfigs();

  /** Optional cutoff time per live reference. */
  @Nullable
  Map<String, Instant> getCutOffTimestampPerRef();

  /** Default cutoff time for all the references. */
  Instant getDefaultCutOffTimestamp();

  /** Optional cutoff time for all the dead references. */
  @Nullable
  Instant getDeadReferenceCutOffTimeStamp();

  /**
   * Optional spark partitions count to be used for distributing references. Default total reference
   * count (live + dead) will be used.
   */
  @Nullable
  Integer getSparkPartitionsCount();

  /**
   * Commit protection duration in hours to avoid expiring on going or recent commits. Default is 2
   * hours.
   */
  @Value.Default
  default int getCommitProtectionDuration() {
    // default is kept as 2 hours.
    return 2;
  }

  /**
   * Optional bloom filter expected live commits entries per reference. Default is total commits in
   * the default reference.
   */
  @Nullable
  Long getBloomFilterExpectedEntries();

  /** Optional bloom filter fpp. Default value is 0.03d. */
  @Value.Default
  default double getBloomFilterFpp() {
    // default value is kept same as underlying Guava bloom filter default fpp.
    return 0.03d;
  }

  @Value.Check
  default void validate() {
    Integer taskCount = getSparkPartitionsCount();
    if (taskCount != null && taskCount <= 0) {
      throw new IllegalArgumentException("taskCount has invalid value: " + taskCount);
    }
    int commitProtectionDuration = getCommitProtectionDuration();
    if (commitProtectionDuration < 0) {
      throw new IllegalArgumentException(
          "commitProtectionDuration has invalid value: " + commitProtectionDuration);
    }
    Long bloomFilterExpectedEntries = getBloomFilterExpectedEntries();
    if (bloomFilterExpectedEntries != null && bloomFilterExpectedEntries < 0) {
      throw new IllegalArgumentException(
          "bloomFilterExpectedEntries has invalid value: " + bloomFilterExpectedEntries);
    }
    double bloomFilterFpp = getBloomFilterFpp();
    if (!(bloomFilterFpp > 0.0D && bloomFilterFpp < 1.0D)) {
      throw new IllegalArgumentException("bloomFilterFpp has invalid value: " + bloomFilterFpp);
    }
  }
}
