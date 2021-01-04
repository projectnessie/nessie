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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.dremio.nessie.tiered.builder.HasIdConsumer;
import com.dremio.nessie.tiered.builder.Producer;
import com.dremio.nessie.versioned.impl.Persistent;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoSerDe {

  private static final EnumMap<ValueType, Supplier<DynamoConsumer<?>>> dynamoConsumerSuppliers;
  private static final EnumMap<ValueType, Function<Map<String, AttributeValue>, DynamoProducer<?>>> dynamoProducerSuppliers;

  static {
    dynamoConsumerSuppliers = new EnumMap<>(ValueType.class);
    dynamoProducerSuppliers = new EnumMap<>(ValueType.class);

    dynamoConsumerSuppliers.put(ValueType.L1, DynamoL1Consumer::new);
    dynamoProducerSuppliers.put(ValueType.L1, DynamoL1Consumer.Producer::new);

    dynamoConsumerSuppliers.put(ValueType.L2, DynamoL2Consumer::new);
    dynamoProducerSuppliers.put(ValueType.L2, DynamoL2Consumer.Producer::new);

    dynamoConsumerSuppliers.put(ValueType.L3, DynamoL3Consumer::new);
    dynamoProducerSuppliers.put(ValueType.L3, DynamoL3Consumer.Producer::new);

    dynamoConsumerSuppliers.put(ValueType.COMMIT_METADATA, DynamoCommitMetadataConsumer::new);
    dynamoProducerSuppliers.put(ValueType.COMMIT_METADATA, DynamoCommitMetadataConsumer.Producer::new);

    dynamoConsumerSuppliers.put(ValueType.VALUE, DynamoValueConsumer::new);
    dynamoProducerSuppliers.put(ValueType.VALUE, DynamoValueConsumer.Producer::new);

    dynamoConsumerSuppliers.put(ValueType.REF, DynamoRefConsumer::new);
    dynamoProducerSuppliers.put(ValueType.REF, DynamoRefConsumer.Producer::new);

    dynamoConsumerSuppliers.put(ValueType.KEY_FRAGMENT, DynamoFragmentConsumer::new);
    dynamoProducerSuppliers.put(ValueType.KEY_FRAGMENT, DynamoFragmentConsumer.Producer::new);

    if (!dynamoConsumerSuppliers.keySet().equals(dynamoProducerSuppliers.keySet())) {
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

  /**
   * TODO javadoc for checkstyle.
   */
  public static <C extends DynamoConsumer<C>, V> Map<String, AttributeValue> serializeEntity(
      ValueType type,
      V value) {

    @SuppressWarnings("unchecked") Persistent<HasIdConsumer<C>> persistent = (Persistent<HasIdConsumer<C>>) value;
    Preconditions.checkArgument(type == persistent.type(),
        "Expected type %s is not the type of the value %s (%s)",
        type.name(), value.getClass(), persistent.type());

    DynamoConsumer<C> consumer = newConsumer(persistent.type());

    Preconditions.checkArgument(consumer.canHandleType(type),
        "Given consumer %s cannot handle ValueType.%s",
        consumer.getClass(), type.name());

    persistent.applyToConsumer(consumer);
    Map<String, AttributeValue> map = consumer.build();

    if (false) {
      AttributeValueUtil.sanityCheckFromSaveOp(type, persistent, map);
    }
    return map;
  }

  /**
   * Deserialize the given {@code entity} as the given {@link ValueType type}.
   */
  public static <V extends HasId, C extends HasIdConsumer<C>> V deserialize(
      ValueType valueType, Map<String, AttributeValue> entity) {
    Producer<V, C> producer = valueType.newEntityProducer();
    @SuppressWarnings("unchecked") C consumer = (C) producer;

    deserializeTo(valueType, entity, consumer);

    V item = producer.build();

    if (false) {
      AttributeValueUtil.sanityCheckToConsumer(entity, valueType, item);
    }

    return item;
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
  public static <C extends HasIdConsumer<C>> void deserializeTo(ValueType valueType, Map<String, AttributeValue> entity, C consumer) {
    Preconditions.checkNotNull(entity, "entity parameter is null");
    String loadedType = entity.get(ValueType.SCHEMA_TYPE).s();
    Id id = DynamoConsumer.deserializeId(entity.get(Store.KEY_NAME));
    Preconditions.checkNotNull(loadedType,
        "Missing type tag for schema for id %s.", id.getHash());
    Preconditions.checkArgument(valueType.getValueName().equals(loadedType),
        "Expected schema for id %s to be of type '%s' but is actually '%s'.",
        id.getHash(), valueType.getValueName(), loadedType);
    Preconditions.checkArgument(consumer.canHandleType(valueType),
        "Given consumer %s cannot handle ValueType.%s",
        consumer.getClass(), valueType.name());

    DynamoProducer<C> dynamoProducer = newProducer(valueType, entity);
    dynamoProducer.applyToConsumer(consumer);
  }

  /**
   * TODO add some javadoc.
   */
  @SuppressWarnings("unchecked")
  static <C extends HasIdConsumer<C>> DynamoConsumer<C> newConsumer(ValueType type) {
    DynamoConsumer<?> c = dynamoConsumerSuppliers.get(type).get();
    if (c == null) {
      throw new IllegalArgumentException("No DynamoConsumer implementation for " + type);
    }
    return (DynamoConsumer<C>) c;
  }

  /**
   * TODO add some javadoc.
   */
  @SuppressWarnings("unchecked")
  static <C extends HasIdConsumer<C>> DynamoProducer<C> newProducer(ValueType type, Map<String, AttributeValue> map) {
    DynamoProducer<?> c = dynamoProducerSuppliers.get(type).apply(map);
    if (c == null) {
      throw new IllegalArgumentException("No DynamoConsumer implementation for " + type);
    }
    return (DynamoProducer<C>) c;
  }
}
