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
package org.projectnessie.versioned.persist.nontx;

import org.immutables.value.Value;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;

public interface NonTransactionalDatabaseAdapterConfig extends DatabaseAdapterConfig {
  int DEFAULT_REFERENCES_SEGMENT_SIZE = 250_000;
  int DEFAULT_REF_LOG_STRIPES = 16;
  int DEFAULT_REFERENCES_SEGMENT_PREFETCH = 1;
  int DEFAULT_REFERENCE_NAMES_BATCH_SIZE = 25;

  @Value.Default
  default int getReferencesSegmentSize() {
    return DEFAULT_REFERENCES_SEGMENT_SIZE;
  }

  @Value.Default
  default int getReferencesSegmentPrefetch() {
    return DEFAULT_REFERENCES_SEGMENT_PREFETCH;
  }

  @Value.Default
  default int getReferenceNamesBatchSize() {
    return DEFAULT_REFERENCE_NAMES_BATCH_SIZE;
  }

  @Value.Default
  default int getRefLogStripes() {
    return DEFAULT_REF_LOG_STRIPES;
  }
}
