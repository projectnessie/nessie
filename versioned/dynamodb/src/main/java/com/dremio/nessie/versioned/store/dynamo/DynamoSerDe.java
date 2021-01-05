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

import com.dremio.nessie.tiered.builder.CommitMetadataConsumer;
import com.dremio.nessie.tiered.builder.FragmentConsumer;
import com.dremio.nessie.tiered.builder.HasIdConsumer;
import com.dremio.nessie.tiered.builder.L1Consumer;
import com.dremio.nessie.tiered.builder.L2Consumer;
import com.dremio.nessie.tiered.builder.L3Consumer;
import com.dremio.nessie.tiered.builder.Producer;
import com.dremio.nessie.tiered.builder.RefConsumer;
import com.dremio.nessie.tiered.builder.ValueConsumer;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

final class DynamoSerDe {

  private static final EnumMap<ValueType, Supplier<DynamoConsumer<?>>> dynamoConsumerSuppliers;
  private static final EnumMap<ValueType, BiConsumer<Map<String, AttributeValue>, HasIdConsumer<?>>> dynamoProducerMethodss;

  static {
    dynamoConsumerSuppliers = new EnumMap<>(ValueType.class);
    dynamoProducerMethodss = new EnumMap<>(ValueType.class);

    dynamoConsumerSuppliers.put(ValueType.L1, DynamoL1Consumer::new);
    dynamoProducerMethodss.put(ValueType.L1, (e, c) -> DynamoL1Consumer.produceToConsumer(e, (L1Consumer) c));

    dynamoConsumerSuppliers.put(ValueType.L2, DynamoL2Consumer::new);
    dynamoProducerMethodss.put(ValueType.L2, (e, c) -> DynamoL2Consumer.produceToConsumer(e, (L2Consumer) c));

    dynamoConsumerSuppliers.put(ValueType.L3, DynamoL3Consumer::new);
    dynamoProducerMethodss.put(ValueType.L3, (e, c) -> DynamoL3Consumer.produceToConsumer(e, (L3Consumer) c));

    dynamoConsumerSuppliers.put(ValueType.COMMIT_METADATA, DynamoCommitMetadataConsumer::new);
    dynamoProducerMethodss.put(ValueType.COMMIT_METADATA,
        (e, c) -> DynamoWrappedValueConsumer.produceToConsumer(e, (CommitMetadataConsumer) c));

    dynamoConsumerSuppliers.put(ValueType.VALUE, DynamoValueConsumer::new);
    dynamoProducerMethodss.put(ValueType.VALUE, (e, c) -> DynamoWrappedValueConsumer.produceToConsumer(e, (ValueConsumer) c));

    dynamoConsumerSuppliers.put(ValueType.REF, DynamoRefConsumer::new);
    dynamoProducerMethodss.put(ValueType.REF, (e, c) -> DynamoRefConsumer.produceToConsumer(e, (RefConsumer) c));

    dynamoConsumerSuppliers.put(ValueType.KEY_FRAGMENT, DynamoFragmentConsumer::new);
    dynamoProducerMethodss.put(ValueType.KEY_FRAGMENT, (e, c) -> DynamoFragmentConsumer.produceToConsumer(e, (FragmentConsumer) c));

    if (!dynamoConsumerSuppliers.keySet().equals(dynamoProducerMethodss.keySet())) {
      throw new UnsupportedOperationException("The enum-maps dynamoConsumerSuppliers and dynamoProducerSuppliers "
          + "are not equal. This is a bug in the implementation of DynamoSerDe.");
    }
    if (!dynamoConsumerSuppliers.keySet().containsAll(Arrays.asList(ValueType.values()))) {
      throw new UnsupportedOperationException(String.format("The implementation of the Dynamo backend does not have "
          + "implementations for all supported value-type. Supported by Dynamo: %s, available: %s. "
          + "This is a bug in the implementation of DynamoSerDe.",
          dynamoConsumerSuppliers.keySet(),
          Arrays.asList(ValueType.values())));
    }
  }

  private DynamoSerDe() {
    // empty
  }

  /**
   * Serialize using the DynamoDB native consumer to the DynamoDB entity map.
   * <p>
   * The actual entity to serialize is not passed into this method, but this method calls
   * a Java {@link Consumer} that receives an instance of {@link DynamoConsumer} that receives
   * the entity components.
   * </p>
   */
  @SuppressWarnings("unchecked")
  public static <C extends HasIdConsumer<C>> Map<String, AttributeValue> serializeWithConsumer(
      ValueType valueType, Consumer<C> serializer) {
    Preconditions.checkNotNull(valueType, "valueType parameter is null");
    Preconditions.checkNotNull(serializer, "serializer parameter is null");

    // No need for any 'type' validation - that's done in the static initializer
    DynamoConsumer<C> consumer = (DynamoConsumer<C>) dynamoConsumerSuppliers.get(valueType).get();

    serializer.accept((C) consumer);

    return consumer.build();
  }

  /**
   * Convenience functionality that deserialized directly into a materialized entity instance,
   * deserialize the given {@code entity} as the given {@link ValueType type}.
   *
   * @see #deserializeToConsumer(ValueType, Map, HasIdConsumer)
   */
  public static <V extends HasId, C extends HasIdConsumer<C>> V deserialize(
      ValueType valueType, Map<String, AttributeValue> entity) {
    Producer<V, C> producer = valueType.newEntityProducer();

    deserializeToConsumer(valueType, entity, producer);

    return producer.build();
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
  public static <C extends HasIdConsumer<C>> void deserializeToConsumer(
      ValueType valueType, Map<String, AttributeValue> entity, HasIdConsumer<C> consumer) {
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
    dynamoProducerMethodss.get(valueType).accept(entity, consumer);
  }
}
