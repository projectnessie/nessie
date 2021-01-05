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

import static com.dremio.nessie.versioned.impl.ValidationHelper.checkCalled;
import static com.dremio.nessie.versioned.impl.ValidationHelper.checkSet;

import com.dremio.nessie.tiered.builder.Producer;
import com.dremio.nessie.tiered.builder.WrappedValueConsumer;
import com.dremio.nessie.versioned.store.ValueType;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

/**
 * A base implementation of a opaque byte object stored in the VersionStore. Used for both for commit metadata and values.
 *
 * <p>Generates an Id based on the hash of the data plus a unique hash seed per object type.
 *
 */
abstract class WrappedValueBean<C extends WrappedValueConsumer<C>> extends MemoizedId<C> {

  private static final int MAX_SIZE = 1024 * 256;
  private final ByteString value;

  protected WrappedValueBean(Id id, ByteString value) {
    super(id);
    this.value = value;
    Preconditions.checkArgument(value.size() < MAX_SIZE, "Values and commit metadata must be less than 256K once serialized.");
  }

  public ByteString getBytes() {
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

  protected static class WrappedValueSchema<T extends WrappedValueBean<?>> extends SimpleSchema<T> {

    private static final String ID = "id";
    private static final String VALUE = "value";
    private final BiFunction<Id, ByteString, T> deserializer;

    protected WrappedValueSchema(Class<T> clazz, BiFunction<Id, ByteString, T> deserializer) {
      super(clazz);
      this.deserializer = deserializer;
    }

    @Override
    public T deserialize(Map<String, Entity> attributeMap) {
      return deserializer.apply(Id.fromEntity(attributeMap.get(ID)), attributeMap.get(VALUE).getBinary());
    }

    @Override
    public Map<String, Entity> itemToMap(T item, boolean ignoreNulls) {
      return ImmutableMap.<String, Entity>builder()
          .put(ID, item.getId().toEntity())
          .put(VALUE, Entity.ofBinary(item.getBytes()))
          .build();
    }
  }

  @Override
  public C applyToConsumer(C consumer) {
    return consumer.id(getId())
        .value(getBytes());
  }

  static final class Builder<E, C extends WrappedValueConsumer<C>> implements WrappedValueConsumer<C>,
      Producer<E, C> {

    private Id id;
    private ByteString value;

    private final ValueType valueType;
    private final BiFunction<Id, ByteString, E> builder;

    protected Builder(ValueType valueType, BiFunction<Id, ByteString, E> builder) {
      this.valueType = valueType;
      this.builder = builder;
    }

    @Override
    public C id(Id id) {
      checkCalled(this.id, "id");
      this.id = id;
      return me();
    }

    @Override
    public C value(ByteString value) {
      checkCalled(this.value, "value");
      this.value = value;
      return me();
    }

    @SuppressWarnings("unchecked")
    private C me() {
      return (C) this;
    }

    @Override
    public boolean canHandleType(ValueType valueType) {
      return this.valueType == valueType;
    }

    @Override
    public E build() {
      checkSet(id, "id");
      checkSet(value, "value");

      return builder.apply(id, value);
    }
  }
}
