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

import java.util.function.Supplier;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.store.DefaultStoreWorker;

/**
 * A PUT operation that has been retrieved from the version store. The content value is kept in its
 * raw (serialized) form; deserialization happens lazily, on first access to {@link #getValue()}.
 */
@Value.Immutable
public interface LazyPut extends Operation {

  int getPayload();

  ByteString getRawValue();

  @Value.Lazy
  @SuppressWarnings("deprecation")
  default Content getValue() {
    return DefaultStoreWorker.instance()
        .valueFromStore((byte) getPayload(), getRawValue(), getGlobalStateSupplier());
  }

  /** TODO remove global state supplier completely when it's removed from VersionStore. */
  @Value.Default
  @Value.Auxiliary
  default Supplier<ByteString> getGlobalStateSupplier() {
    return () -> null;
  }

  static LazyPut of(ContentKey key, int payload, ByteString value) {
    return of(key, payload, value, () -> null);
  }

  static LazyPut of(
      ContentKey key, int payload, ByteString value, Supplier<ByteString> globalStateSupplier) {
    return ImmutableLazyPut.builder()
        .key(key)
        .payload(payload)
        .rawValue(value)
        .globalStateSupplier(globalStateSupplier)
        .build();
  }
}
