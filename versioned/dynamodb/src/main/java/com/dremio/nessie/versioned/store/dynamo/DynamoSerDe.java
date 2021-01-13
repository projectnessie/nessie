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
package com.dremio.nessie.versioned.store.dynamo;

import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.attributeValue;
import static com.dremio.nessie.versioned.store.dynamo.AttributeValueUtil.deserializeId;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.dremio.nessie.tiered.builder.BaseConsumer;
import com.dremio.nessie.tiered.builder.CommitMetadataConsumer;
import com.dremio.nessie.tiered.builder.FragmentConsumer;
import com.dremio.nessie.tiered.builder.L1Consumer;
import com.dremio.nessie.tiered.builder.L2Consumer;
import com.dremio.nessie.tiered.builder.L3Consumer;
import com.dremio.nessie.tiered.builder.RefConsumer;
import com.dremio.nessie.tiered.builder.ValueConsumer;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

final class DynamoSerDe {

  private static final EnumMap<ValueType, Supplier<DynamoConsumer<?>>> dynamoEntityMapProducers;
  private static final EnumMap<ValueType, BiConsumer<Map<String, AttributeValue>, BaseConsumer<?>>> deserializeToConsumer;

  static {
    dynamoEntityMapProducers = new EnumMap<>(ValueType.class);
    deserializeToConsumer = new EnumMap<>(ValueType.class);

    dynamoEntityMapProducers.put(ValueType.L1, DynamoL1Consumer::new);
    deserializeToConsumer.put(ValueType.L1, (e, c) -> DynamoL1Consumer.toConsumer(e, (L1Consumer) c));

    dynamoEntityMapProducers.put(ValueType.L2, DynamoL2Consumer::new);
    deserializeToConsumer.put(ValueType.L2, (e, c) -> DynamoL2Consumer.toConsumer(e, (L2Consumer) c));

    dynamoEntityMapProducers.put(ValueType.L3, DynamoL3Consumer::new);
    deserializeToConsumer.put(ValueType.L3, (e, c) -> DynamoL3Consumer.toConsumer(e, (L3Consumer) c));

    dynamoEntityMapProducers.put(ValueType.COMMIT_METADATA, DynamoCommitMetadataConsumer::new);
    deserializeToConsumer.put(ValueType.COMMIT_METADATA,
        (e, c) -> DynamoWrappedValueConsumer.produceToConsumer(e, (CommitMetadataConsumer) c));

    dynamoEntityMapProducers.put(ValueType.VALUE, DynamoValueConsumer::new);
    deserializeToConsumer
        .put(ValueType.VALUE, (e, c) -> DynamoWrappedValueConsumer.produceToConsumer(e, (ValueConsumer) c));

    dynamoEntityMapProducers.put(ValueType.REF, DynamoRefConsumer::new);
    deserializeToConsumer.put(ValueType.REF, (e, c) -> DynamoRefConsumer.toConsumer(e, (RefConsumer) c));

    dynamoEntityMapProducers.put(ValueType.KEY_FRAGMENT, DynamoFragmentConsumer::new);
    deserializeToConsumer
        .put(ValueType.KEY_FRAGMENT, (e, c) -> DynamoFragmentConsumer.toConsumer(e, (FragmentConsumer) c));

    if (!dynamoEntityMapProducers.keySet().equals(deserializeToConsumer.keySet())) {
      throw new UnsupportedOperationException("The enum-maps dynamoConsumerSuppliers and dynamoProducerSuppliers "
          + "are not equal. This is a bug in the implementation of DynamoSerDe.");
    }
    if (!dynamoEntityMapProducers.keySet().containsAll(Arrays.asList(ValueType.values()))) {
      throw new UnsupportedOperationException(String.format("The implementation of the Dynamo backend does not have "
          + "implementations for all supported value-type. Supported by Dynamo: %s, available: %s. "
          + "This is a bug in the implementation of DynamoSerDe.",
          dynamoEntityMapProducers.keySet(),
          Arrays.asList(ValueType.values())));
    }
  }

  private DynamoSerDe() {
    // empty
  }

  /**
   * Serialize using a DynamoDB native consumer to a DynamoDB entity map.
   * <p>
   * The actual entity to serialize is not passed into this method, but this method calls
   * a Java {@link Consumer} that receives an instance of {@link DynamoConsumer} that receives
   * the entity components.
   * </p>
   */
  @SuppressWarnings("unchecked")
  public static <C extends BaseConsumer<C>> Map<String, AttributeValue> serializeWithConsumer(
      ValueType valueType, Consumer<BaseConsumer<C>> serializer) {
    Preconditions.checkNotNull(valueType, "valueType parameter is null");
    Preconditions.checkNotNull(serializer, "serializer parameter is null");

    // No need for any 'type' validation - that's done in the static initializer
    DynamoConsumer<C> consumer = (DynamoConsumer<C>) dynamoEntityMapProducers.get(valueType).get();

    serializer.accept((C) consumer);

    return consumer.build();
  }

  /**
   * Convenience functionality around {@link #deserializeToConsumer(ValueType, Map, BaseConsumer)}
   * that deserializes directly into a materialized entity instance,
   * deserialize the given DynamoDB {@code entity}-map as the given {@link ValueType type}
   * and returns a materialized object.
   *
   * @see #deserializeToConsumer(ValueType, Map, BaseConsumer)
   */
  public static <V extends HasId, C extends BaseConsumer<C>> V deserialize(
      ValueType valueType, Map<String, AttributeValue> entity) {
    return valueType.buildEntity(producer -> deserializeToConsumer(valueType, entity, producer));
  }

  /**
   * Deserialize the given {@code entity} as the given {@link ValueType type} directly into
   * the given {@code consumer}.
   *
   * @param valueType type of the entity to deserialize
   * @param entity entity to deserialize
   * @param consumer consumer that receives the deserialized parts of the entity
   * @param <C> type of the consumer
   */
  public static <C extends BaseConsumer<C>> void deserializeToConsumer(
      ValueType valueType, Map<String, AttributeValue> entity, BaseConsumer<C> consumer) {
    Preconditions.checkNotNull(valueType, "valueType parameter is null");
    Preconditions.checkNotNull(entity, "entity parameter is null");
    Preconditions.checkNotNull(consumer, "consumer parameter is null");
    String loadedType = Preconditions.checkNotNull(attributeValue(entity, ValueType.SCHEMA_TYPE).s(),
        "Schema-type ('t') in entity is not a string");
    Id id = deserializeId(entity, Store.KEY_NAME);
    Preconditions.checkNotNull(loadedType,
        "Missing type tag for schema for id %s.", id.getHash());
    Preconditions.checkArgument(valueType.getValueName().equals(loadedType),
        "Expected schema for id %s to be of type '%s' but is actually '%s'.",
        id.getHash(), valueType.getValueName(), loadedType);

    // No need for any 'valueType' validation against the static map - that's done in the static initializer
    deserializeToConsumer.get(valueType).accept(entity, consumer);
  }
}
