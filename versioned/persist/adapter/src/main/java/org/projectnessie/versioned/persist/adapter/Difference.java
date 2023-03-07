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
import org.projectnessie.model.ContentKey;

@Value.Immutable
public interface Difference {

  @Value.Parameter(order = 1)
  byte getPayload();

  @Value.Parameter(order = 2)
  ContentKey getKey();

  @Value.Parameter(order = 3)
  Optional<ByteString> getFromValue();

  @Value.Parameter(order = 4)
  Optional<ByteString> getToValue();

  @Value.Parameter(order = 5)
  Optional<ByteString> getGlobal();

  static Difference of(
      byte payload,
      ContentKey key,
      Optional<ByteString> global,
      Optional<ByteString> from,
      Optional<ByteString> to) {
    return ImmutableDifference.of(payload, key, from, to, global);
  }
}
