/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.common.persist;

import org.immutables.value.Value;

@Value.Immutable
public interface SizeLimits {

  SizeLimits NO_LIMITS = SizeLimits.builder().build();

  @Value.Default
  default int incrementalIndexSizeLimit() {
    return Integer.MAX_VALUE;
  }

  @Value.Default
  default int serializedIndexSizeLimit() {
    return Integer.MAX_VALUE;
  }

  static ImmutableSizeLimits.Builder builder() {
    return ImmutableSizeLimits.builder();
  }
}
