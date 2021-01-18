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
package com.dremio.nessie.versioned.impl;

import java.util.Objects;
import com.dremio.nessie.tiered.builder.BaseWrappedValue;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

/**
 * A base implementation of a opaque byte object stored in the VersionStore. Used for both for commit metadata and values.
 *
 * <p>Generates an Id based on the hash of the data plus a unique hash seed per object type.</p>
 */
abstract class WrappedValueBean<C extends BaseWrappedValue<C>> extends PersistentBase<C> {

  private static final int MAX_SIZE = 1024 * 256;
  private final ByteString value;

  protected WrappedValueBean(Id id, ByteString value, Long dt) {
    super(id, dt);
    this.value = value;
    Preconditions.checkArgument(value.size() < MAX_SIZE, "Values and commit metadata must be less than 256K once serialized.");
  }

  ByteString getBytes() {
    return value;
  }

  /**
   * Return a consistent hash seed for this object type to avoid accidental object hash conflicts.
   * @return A seed value that is consistent for this object type.
   */
  protected abstract long getSeed();

  @Override
  Id generateId() {
    return Id.build(h -> {
      h.putLong(getSeed()).putBytes(value.asReadOnlyByteBuffer());
    });
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, getSeed());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof WrappedValueBean)) {
      return false;
    }
    WrappedValueBean<?> other = (WrappedValueBean<?>) obj;
    return Objects.equals(getSeed(),  other.getSeed())
        && Objects.equals(value, other.value);
  }

  @Override
  C applyToConsumer(C consumer) {
    return super.applyToConsumer(consumer)
        .value(getBytes());
  }

  public interface Creator<C> {
    C create(Id id, ByteString value, Long dt);
  }

  /**
   * Base builder-implementation for both {@link InternalCommitMetadata} and {@link InternalValue}.
   */
  // Needs to be a package private class, otherwise class-initialization of ValueType fails with j.l.IllegalAccessError
  static class Builder<E extends HasId, C extends BaseWrappedValue<C>>
      extends EntityBuilder<E> implements BaseWrappedValue<C> {

    private Id id;
    private Long dt;
    private ByteString value;

    private final Creator<E> builder;

    Builder(Creator<E> builder) {
      this.builder = builder;
    }

    @SuppressWarnings("unchecked")
    @Override
    public C id(Id id) {
      checkCalled(this.id, "id");
      this.id = id;
      return (C) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public C dt(long dt) {
      checkCalled(this.dt, "dt");
      this.dt = dt;
      return (C) this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public C value(ByteString value) {
      checkCalled(this.value, "value");
      this.value = value;
      return (C) this;
    }

    E build() {
      // null-id is allowed (will be generated)
      checkSet(value, "value");

      return builder.create(id, value, dt);
    }
  }
}
