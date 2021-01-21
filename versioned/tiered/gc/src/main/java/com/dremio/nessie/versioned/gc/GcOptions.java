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
package com.dremio.nessie.versioned.gc;

import org.immutables.value.Value.Immutable;

@Immutable
public interface GcOptions {

  /**
   * The expected bloomfilter capacity.
   * @return The number of expected items.
   */
  long getBloomFilterCapacity();

  /**
   * The age in micros beyond which a referenced L1 can be collected.
   * @return
   */
  long getMaxAgeMicros();

  /**
   * Describes the the minimum amount of time "slop" before the algorithm can
   * consider removing an unreferenced value (based on the DT of that value or L1).
   *
   * @return Time in Micros before the present moment.
   */
  long getTimeSlopMicros();

}
