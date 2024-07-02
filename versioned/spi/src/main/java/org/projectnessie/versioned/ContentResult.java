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

import static org.projectnessie.versioned.ResultType.CONTENT_RESULT;

import jakarta.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.model.Documentation;
import org.projectnessie.model.IdentifiedContentKey;

@Value.Immutable
public interface ContentResult extends Result {
  @Override
  default ResultType getResultType() {
    return CONTENT_RESULT;
  }

  @Value.Parameter(order = 1)
  IdentifiedContentKey identifiedKey();

  @Value.Parameter(order = 2)
  @Nullable
  Content content();

  @Value.Parameter(order = 3)
  @Nullable
  Documentation documentation();

  static ContentResult contentResult(
      IdentifiedContentKey identifiedKey, Content content, Documentation documentation) {
    return ImmutableContentResult.of(identifiedKey, content, documentation);
  }
}
