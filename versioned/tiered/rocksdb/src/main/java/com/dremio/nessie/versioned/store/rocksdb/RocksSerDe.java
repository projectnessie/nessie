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

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.tiered.builder.CommitMetadata;
import com.dremio.nessie.tiered.builder.Fragment;
import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.tiered.builder.L2;
import com.dremio.nessie.tiered.builder.L3;
import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.tiered.builder.Value;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Serializes and deserializes metadata to a RocksDB store.
 */
final class RocksSerDe {
  private static final Map<ValueType<?>, Supplier<RocksBaseValue<?>>> CONSUMERS;
  private static final Map<ValueType<?>, BiConsumer<byte[], BaseValue<?>>> DESERIALIZERS;

  static {
    final ImmutableMap.Builder<ValueType<?>, Supplier<RocksBaseValue<?>>> consumers = ImmutableMap.builder();
    final ImmutableMap.Builder<ValueType<?>, BiConsumer<byte[], BaseValue<?>>> deserializers = ImmutableMap.builder();

    consumers.put(ValueType.L1, RocksL1::new);
    deserializers.put(ValueType.L1, (e, c) -> RocksL1.toConsumer(e, (L1) c));

    consumers.put(ValueType.L2, RocksL2::new);
    deserializers.put(ValueType.L2, (e, c) -> RocksL2.toConsumer(e, (L2) c));

    consumers.put(ValueType.L3, RocksL3::new);
    deserializers.put(ValueType.L3, (e, c) -> RocksL3.toConsumer(e, (L3) c));

    consumers.put(ValueType.COMMIT_METADATA, RocksCommitMetadata::new);
    deserializers.put(ValueType.COMMIT_METADATA, (e, c) -> RocksCommitMetadata.toConsumer(e, (CommitMetadata) c));

    consumers.put(ValueType.VALUE, RocksValue::new);
    deserializers.put(ValueType.VALUE, (e, c) -> RocksValue.toConsumer(e, (Value) c));

    consumers.put(ValueType.REF, RocksRef::new);
    deserializers.put(ValueType.REF, (e, c) -> RocksRef.toConsumer(e, (Ref) c));

    consumers.put(ValueType.KEY_FRAGMENT, RocksFragment::new);
    deserializers.put(ValueType.KEY_FRAGMENT, (e, c) -> RocksFragment.toConsumer(e, (Fragment) c));

    CONSUMERS = consumers.build();
    DESERIALIZERS = deserializers.build();

    if (!CONSUMERS.keySet().equals(DESERIALIZERS.keySet())) {
      throw new UnsupportedOperationException("The enum-maps CONSUMERS and DESERIALIZERS "
          + "are not equal. This is a bug in the implementation of RocksSerDe.");
    }
    if (!CONSUMERS.keySet().containsAll(ValueType.values())) {
      throw new UnsupportedOperationException(String.format("The implementation of the RocksDB backend does not have "
              + "implementations for all supported value-type. Supported by RocksDB: %s, available: %s. "
              + "This is a bug in the implementation of RocksSerDe.",
          CONSUMERS.keySet(),
          ValueType.values()));
    }
  }

  private RocksSerDe() {
    // empty
  }

  /**
   * Serialize using a RocksDB native consumer to a protobuf format.
   * <p>
   * The actual entity to serialize is not passed into this method, but this method calls
   * a Java {@link Consumer} that receives an instance of {@link RocksBaseValue} that receives
   * the entity components.
   * </p>
   * @param saveOp the operation representing the entity to serialize.
   * @return the bytes representing the entity in protobuf format.
   */
  @SuppressWarnings("unchecked")
  static <C extends BaseValue<C>> byte[] serializeWithConsumer(SaveOp<C> saveOp) {
    Preconditions.checkNotNull(saveOp, "saveOp parameter is null");

    // No need for any 'type' validation - that's done in the static initializer
    final C consumer = (C) CONSUMERS.get(saveOp.getType()).get();

    saveOp.serialize(consumer);

    return ((RocksBaseValue<C>) consumer).build();
  }

  /**
   * Get the Consumer for the provided saveOp.
   * @param saveOp the saveOp for which a consumer if required
   * @param <C> The consumer type
   * @return the consumer
   */
  static <C extends BaseValue<C>> RocksBaseValue getConsumer(SaveOp<C> saveOp) {
    return CONSUMERS.get(saveOp.getType()).get();
  }

  /**
   * Deserialize the given {@code value} as the given {@link ValueType type} directly into
   * the given {@code consumer}.
   *
   * @param valueType type of the value to deserialize.
   * @param value protobuf value to deserialize.
   * @param consumer consumer that receives the deserialized parts of the value.
   * @param <C> type of the consumer.
   */
  static <C extends BaseValue<C>> void deserializeToConsumer(
      ValueType<C> valueType, byte[] value, BaseValue<C> consumer) {
    Preconditions.checkNotNull(valueType, "valueType parameter is null");
    Preconditions.checkNotNull(value, "value parameter is null");
    Preconditions.checkNotNull(consumer, "consumer parameter is null");

    // No need for any 'valueType' validation against the static map - that's done in the static initializer.
    DESERIALIZERS.get(valueType).accept(value, consumer);
  }
}
