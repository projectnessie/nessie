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

import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.model.IdentifiedContentKey;

@Value.Immutable
public interface CommitValidation {
  List<CommitOperation> operations();

  static ImmutableCommitValidation.Builder builder() {
    return ImmutableCommitValidation.builder();
  }

  @Value.Immutable
  interface CommitOperation {
    @Value.Parameter(order = 1)
    IdentifiedContentKey identifiedKey();

    @Value.Parameter(order = 2)
    Operation operation();

    static CommitOperation commitOperation(
        IdentifiedContentKey identifiedKey, Operation operation) {
      return ImmutableCommitOperation.of(identifiedKey, operation);
    }
  }
}
