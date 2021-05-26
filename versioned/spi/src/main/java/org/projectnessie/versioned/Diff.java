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
import org.immutables.value.Value.Immutable;

@Immutable
public interface Diff<VALUE> {

  Key getKey();

  Optional<VALUE> getFromValue();

  Optional<VALUE> getToValue();

  public static <VALUE> Diff<VALUE> of(Key key, Optional<VALUE> from, Optional<VALUE> to) {
    return ImmutableDiff.<VALUE>builder().key(key).fromValue(from).toValue(to).build();
  }
}
