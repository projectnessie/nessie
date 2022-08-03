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

import java.util.Set;
import org.immutables.value.Value;
import org.projectnessie.versioned.Hash;

@Value.Immutable
public interface HeadsAndForkPoints {
  @Value.Parameter(order = 1)
  Set<Hash> getHeads();

  @Value.Parameter(order = 2)
  Set<Hash> getForkPoints();

  @Value.Parameter(order = 3)
  long getScanStartedAtInMicros();

  static HeadsAndForkPoints of(Set<Hash> heads, Set<Hash> forkPoints, long scanStartedAtInMicros) {
    return ImmutableHeadsAndForkPoints.of(heads, forkPoints, scanStartedAtInMicros);
  }
}
