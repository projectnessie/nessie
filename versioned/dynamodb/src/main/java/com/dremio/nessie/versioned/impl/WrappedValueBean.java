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

import java.util.Map;
import java.util.function.BiFunction;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A base implementation of a opaque byte object stored in the VersionStore. Used for both for commit metadata and values.
 *
 * <p>Generates an Id based on the hash of the data plus a unique hash seed per object type.
 *
 */
abstract class WrappedValueBean extends MemoizedId {

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

  protected static class WrappedValueSchema<T extends WrappedValueBean> extends SimpleSchema<T> {

    private static final String ID = "id";
    private static final String VALUE = "value";
    private final BiFunction<Id, ByteString, T> deserializer;

    protected WrappedValueSchema(Class<T> clazz, BiFunction<Id, ByteString, T> deserializer) {
      super(clazz);
      this.deserializer = deserializer;
    }

    @Override
    public T deserialize(Map<String, AttributeValue> attributeMap) {
      return deserializer.apply(Id.fromAttributeValue(attributeMap.get(ID)),
          ByteString.copyFrom(attributeMap.get(VALUE).b().asByteArray()));
    }

    @Override
    public Map<String, AttributeValue> itemToMap(T item, boolean ignoreNulls) {
      return ImmutableMap.<String, AttributeValue>builder()
          .put(ID, item.getId().toAttributeValue())
          .put(VALUE, AttributeValue.builder().b(SdkBytes.fromByteBuffer(item.getBytes().asReadOnlyByteBuffer())).build())
          .build();
    }
  }
}
