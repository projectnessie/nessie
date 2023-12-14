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
package org.projectnessie.versioned;

import jakarta.annotation.Nullable;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.model.CommitConsistency;
import org.projectnessie.model.CommitMeta;

@Value.Immutable
public interface ReferenceHistory {
  ReferenceHistoryElement current();

  List<ReferenceHistoryElement> previous();

  CommitConsistency commitLogConsistency();

  @Value.Immutable
  interface ReferenceHistoryElement {
    @Value.Parameter(order = 1)
    Hash pointer();

    @Value.Parameter(order = 2)
    CommitConsistency commitConsistency();

    @Value.Parameter(order = 3)
    @Nullable
    CommitMeta meta();

    static ReferenceHistoryElement referenceHistoryElement(
        Hash pointer, CommitConsistency commitConsistency, CommitMeta meta) {
      return ImmutableReferenceHistoryElement.of(pointer, commitConsistency, meta);
    }
  }
}
