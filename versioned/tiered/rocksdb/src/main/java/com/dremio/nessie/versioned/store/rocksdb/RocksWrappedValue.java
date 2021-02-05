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
package com.dremio.nessie.versioned.store.rocksdb;

import com.dremio.nessie.tiered.builder.BaseWrappedValue;
import com.dremio.nessie.versioned.store.StoreException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A RocksDB specific implementation of {@link com.dremio.nessie.tiered.builder.BaseWrappedValue} providing
 * SerDe and Condition evaluation.
 */
class RocksWrappedValue<C extends BaseWrappedValue<C>> extends RocksBaseValue<C> implements BaseWrappedValue<C> {

  static final String VALUE = "value";
  protected ByteString byteValue;

  RocksWrappedValue() {
    super();
  }

  @SuppressWarnings("unchecked")
  @Override
  public C value(ByteString value) {
    this.byteValue = value;
    return (C) this;
  }

  @Override
  byte[] build() {
    checkPresent(byteValue, VALUE);
    return ValueProtos.WrappedValue.newBuilder()
      .setBase(buildBase())
      .setValue(byteValue)
      .build()
      .toByteArray();
  }

  /**
   * Deserialize a RocksDB value into the given consumer.
   *
   * @param value the protobuf formatted value.
   * @param consumer the consumer to put the value into.
   */
  static <C extends BaseWrappedValue<C>> void toConsumer(byte[] value, C consumer) {
    try {
      final ValueProtos.WrappedValue wrappedValue = ValueProtos.WrappedValue.parseFrom(value);
      setBase(consumer, wrappedValue.getBase());
      consumer.value(wrappedValue.getValue());
    } catch (InvalidProtocolBufferException e) {
      throw new StoreException("Corrupt WrappedValue value encountered when deserializing.", e);
    }
  }
}
