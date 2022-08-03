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
package org.projectnessie.versioned;

import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.model.Content;

@Value.Immutable
public interface Diff {

  @Value.Parameter(order = 1)
  Key getKey();

  @Value.Parameter(order = 2)
  Optional<Content> getFromValue();

  @Value.Parameter(order = 3)
  Optional<Content> getToValue();

  static Diff of(Key key, Optional<Content> from, Optional<Content> to) {
    return ImmutableDiff.of(key, from, to);
  }
}
