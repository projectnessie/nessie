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
package org.projectnessie.versioned.storage.common.logic;

import java.util.Map;
import java.util.Set;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@Value.Immutable
public interface ReferencedAndUnreferencedHeads {
  @Value.Parameter(order = 1)
  Map<ObjId, Set<String>> getReferencedHeads();

  @Value.Parameter(order = 2)
  Set<ObjId> getUnreferencedHeads();

  static ReferencedAndUnreferencedHeads of(
      Map<ObjId, Set<String>> referencedHeads, Set<ObjId> unreferencedHeads) {
    return ImmutableReferencedAndUnreferencedHeads.of(referencedHeads, unreferencedHeads);
  }
}
