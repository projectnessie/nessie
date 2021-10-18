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
package org.projectnessie.versioned.persist.adapter;

import com.google.protobuf.ByteString;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.versioned.Key;

@Value.Immutable
public interface Difference {

  Key getKey();

  Optional<ByteString> getGlobal();

  Optional<ByteString> getFromValue();

  Optional<ByteString> getToValue();

  static Difference of(
      Key key, Optional<ByteString> global, Optional<ByteString> from, Optional<ByteString> to) {
    return ImmutableDifference.builder()
        .key(key)
        .global(global)
        .fromValue(from)
        .toValue(to)
        .build();
  }
}
