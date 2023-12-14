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

import com.google.common.base.MoreObjects;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Objects;
import java.util.function.Supplier;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.store.DefaultStoreWorker;

/** Setting a new value. Can optionally declare whether the prior hash must match. */
@Value.Immutable
public abstract class Put implements Operation {

  /**
   * The value to store for this operation.
   *
   * @return the value
   */
  @Value.Lazy
  public Content getValue() {
    return getValueSupplier().get();
  }

  @Override
  public OperationType getType() {
    return OperationType.PUT;
  }

  protected abstract Supplier<Content> getValueSupplier();

  /**
   * Creates an (eagerly-evaluated) put-operation for the given key and value.
   *
   * <p>{@code value} with a {@code null} content ID is <em>required</em> when creating/adding new
   * content.
   *
   * <p>{@code value} with a non-{@code null} content ID is <em>required</em> when updating existing
   * content.
   *
   * <p>A content object is considered to be the same using the {@link ContentKey content-key} and
   * the {@link Content#getId() content-id}.
   *
   * @param key the key impacted by the operation
   * @param value the new value associated with the key
   * @return a put operation for the key and value
   */
  @Nonnull
  public static Put of(@Nonnull ContentKey key, @Nonnull Content value) {
    return ImmutablePut.builder().key(key).valueSupplier(() -> value).build();
  }

  /** Creates a lazily-evaluated put-operation for the given key, payload and ByteString value. */
  @Nonnull
  public static Put ofLazy(ContentKey key, int payload, ByteString value) {
    return ofLazy(key, payload, value, () -> null);
  }

  /**
   * Creates a lazily-evaluated put-operation for the given key, payload, ByteString value and
   * global state supplier.
   */
  @SuppressWarnings("deprecation")
  @Nonnull
  public static Put ofLazy(
      ContentKey key, int payload, ByteString value, Supplier<ByteString> globalStateSupplier) {
    return ImmutablePut.builder()
        .key(key)
        .valueSupplier(
            () ->
                DefaultStoreWorker.instance()
                    .valueFromStore((byte) payload, value, globalStateSupplier))
        .build();
  }

  // Redefine equals() and hashCode() because even if getValue() is @Lazy,
  // we want it in the computation, and not getValueSupplier().

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Put)) {
      return false;
    }
    Put that = (Put) o;
    return this.shouldMatchHash() == that.shouldMatchHash()
        && this.getKey().equals(that.getKey())
        && this.getValue().equals(that.getValue());
  }

  @Override
  public int hashCode() {
    return Objects.hash(shouldMatchHash(), getKey(), getValue());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Put")
        .omitNullValues()
        .add("shouldMatchHash", shouldMatchHash())
        .add("key", getKey())
        .add("value", getValue())
        .toString();
  }
}
