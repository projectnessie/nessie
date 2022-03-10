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
package org.projectnessie.versioned.persist.adapter;

import org.immutables.value.Value;

@Value.Immutable
public interface GlobalLogCompactionParams {

  int DEFAULT_NO_COMPACTION_WHEN_COMPACTED_WITHIN = 50;
  int DEFAULT_NO_COMPACTION_UP_TO_LENGTH = 50;

  /**
   * When the global-log contains a compacted entry within this number of entries, global-log
   * compaction will not happen, defaults to {@value #DEFAULT_NO_COMPACTION_WHEN_COMPACTED_WITHIN}.
   */
  @Value.Default
  default int getNoCompactionWhenCompactedWithin() {
    return DEFAULT_NO_COMPACTION_WHEN_COMPACTED_WITHIN;
  }

  /**
   * When the global-log contains only up to this number of entries, global-log compaction will not
   * happen, defaults to {@value #DEFAULT_NO_COMPACTION_UP_TO_LENGTH}.
   */
  @Value.Default
  default int getNoCompactionUpToLength() {
    return DEFAULT_NO_COMPACTION_UP_TO_LENGTH;
  }

  static ImmutableGlobalLogCompactionParams.Builder builder() {
    return ImmutableGlobalLogCompactionParams.builder();
  }
}
