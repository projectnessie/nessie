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
package com.dremio.nessie.versioned.store.mongodb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

@SuppressWarnings({"unchecked", "rawtypes"})
final class MongoSerDe {
  private static final Map<ValueType<?>, Function<BsonWriter, MongoBaseValue>> CONSUMERS;
  private static final Map<ValueType<?>, Map<String, BiConsumer<BaseValue, BsonReader>>> DESERIALIZERS;

  private static final String MONGO_ID_NAME = "_id";

  private static final String KEY_ADDITION = "a";
  private static final String KEY_REMOVAL = "d";

  static {
    ImmutableMap.Builder<ValueType<?>, Function<BsonWriter, MongoBaseValue>> consumers = ImmutableMap.builder();
    ImmutableMap.Builder<ValueType<?>, Map<String, BiConsumer<BaseValue, BsonReader>>> deserializers = ImmutableMap.builder();

    consumers.put(ValueType.L1, MongoL1::new);
    deserializers.put(ValueType.L1, (Map) MongoL1.PROPERTY_PRODUCERS);
    consumers.put(ValueType.L2, MongoL2::new);
    deserializers.put(ValueType.L2, (Map) MongoL2.PROPERTY_PRODUCERS);
    consumers.put(ValueType.L3, MongoL3::new);
    deserializers.put(ValueType.L3, (Map) MongoL3.PROPERTY_PRODUCERS);
    consumers.put(ValueType.KEY_FRAGMENT, MongoFragment::new);
    deserializers.put(ValueType.KEY_FRAGMENT, (Map) MongoFragment.PROPERTY_PRODUCERS);
    consumers.put(ValueType.REF, MongoRef::new);
    deserializers.put(ValueType.REF, (Map) MongoRef.PROPERTY_PRODUCERS);
    consumers.put(ValueType.VALUE, MongoWrappedValue::new);
    deserializers.put(ValueType.VALUE, (Map) MongoWrappedValue.PROPERTY_PRODUCERS);
    consumers.put(ValueType.COMMIT_METADATA, MongoWrappedValue::new);
    deserializers.put(ValueType.COMMIT_METADATA, (Map) MongoWrappedValue.PROPERTY_PRODUCERS);

    CONSUMERS = consumers.build();
    DESERIALIZERS = deserializers.build();

    if (!CONSUMERS.keySet().equals(DESERIALIZERS.keySet())) {
      throw new UnsupportedOperationException("The enum-maps ENTITY_MAP_PRODUCERS and DESERIALIZERS "
          + "are not equal. This is a bug in the implementation of MongoSerDe.");
    }
    if (!DESERIALIZERS.keySet().equals(new HashSet<>(ValueType.values()))) {
      throw new UnsupportedOperationException(String.format("The enum-map producerMaps does not "
              + "have producer-maps matching the available value types (%s vs %s).",
          DESERIALIZERS.keySet(), new HashSet<>(ValueType.values())));
    }
  }

  private MongoSerDe() {
    // empty
  }

  /**
   * Deserialize a MongoDB entity into the given consumer.
   */
  static <C extends BaseValue<C>> void produceToConsumer(BsonDocument entity, ValueType<C> valueType, C consumer) {
    produceToConsumer(new BsonDocumentReader(entity), valueType, x -> consumer, x -> {});
  }

  /**
   * Deserialize a MongoDB entity into the given consumer.
   */
  static void produceToConsumer(BsonReader entity, ValueType<?> valueType, Function<Id, BaseValue> onIdParsed, Consumer<Id> parsed) {
    Map<String, BiConsumer<BaseValue, BsonReader>> propertyProducers = DESERIALIZERS.get(valueType);
    deserializeToConsumer(entity, onIdParsed, parsed, propertyProducers);
  }

  private static <C extends BaseValue<C>> MongoBaseValue<C> newMongoConsumer(ValueType<C> valueType, BsonWriter bsonWriter) {
    return CONSUMERS.get(valueType).apply(bsonWriter);
  }

  static <C extends BaseValue<C>> Bson bsonForValueType(SaveOp<C> saveOp, String updateOperator) {
    return new Bson() {
      @Override
      public <T> BsonDocument toBsonDocument(Class<T> clazz, CodecRegistry codecRegistry) {
        final BsonDocumentWriter writer = new BsonDocumentWriter(new BsonDocument());

        writer.writeStartDocument();
        writer.writeName(updateOperator);
        serializeEntity(writer, saveOp);
        writer.writeEndDocument();

        return writer.getDocument();
      }
    };
  }

  static <C extends BaseValue<C>> void serializeEntity(BsonWriter writer, SaveOp<C> saveOp) {
    writer.writeStartDocument();
    MongoBaseValue<C> consumer = newMongoConsumer(saveOp.getType(), writer);
    consumer.id(saveOp.getId());
    saveOp.serialize((C) consumer);
    consumer.build();
    writer.writeEndDocument();
  }

  static BsonBinary serializeId(Id id) {
    return new BsonBinary(id.toBytes());
  }

  static void serializeId(BsonWriter writer, String property, Id id) {
    writer.writeBinaryData(property, new BsonBinary(id.toBytes()));
  }

  static Id deserializeId(BsonReader reader) {
    BsonBinary value = reader.readBinaryData();
    return Id.of(value.getData());
  }

  static Stream<Id> deserializeIds(BsonReader reader) {
    return deserializeArray(reader, MongoSerDe::deserializeId).stream();
  }

  static BsonBinary serializeBytes(ByteString value) {
    return new BsonBinary(value.toByteArray());
  }

  static ByteString deserializeBytes(BsonReader reader) {
    BsonBinary value = reader.readBinaryData();
    return ByteString.copyFrom(value.getData());
  }

  static Stream<Key> deserializeKeys(BsonReader reader) {
    return deserializeArray(reader, MongoSerDe::deserializeKey).stream();
  }

  static void serializeKey(BsonWriter bsonWriter, String prop, Key key) {
    bsonWriter.writeStartArray(prop);
    key.getElements().forEach(bsonWriter::writeString);
    bsonWriter.writeEndArray();
  }

  static Key deserializeKey(BsonReader reader) {
    return Key.of(deserializeArray(reader, BsonReader::readString).toArray(new String[0]));
  }

  static <X> void serializeArray(BsonWriter writer, String prop, Stream<X> src, BiConsumer<BsonWriter, X> inner) {
    writer.writeStartArray(prop);
    src.forEach(e -> inner.accept(writer, e));
    writer.writeEndArray();
  }

  static <X> List<X> deserializeArray(BsonReader reader, Function<BsonReader, X> inner) {
    reader.readStartArray();
    List<X> list = new ArrayList<>();
    while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
      list.add(inner.apply(reader));
    }
    reader.readEndArray();
    return list;
  }

  static void deserializeToConsumer(BsonReader reader,
      Function<Id, BaseValue> onIdParsed,
      Consumer<Id> parsed,
      Map<String, BiConsumer<BaseValue, BsonReader>> propertyProducers) {
    reader.readStartDocument();

    Id id = null;
    BaseValue consumer = null;
    while (BsonType.END_OF_DOCUMENT != reader.readBsonType()) {
      final String name = reader.readName();
      if (name.equals(MONGO_ID_NAME)) {
        reader.skipValue();
        continue;
      }

      if (MongoBaseValue.ID.equals(name)) {
        id = MongoSerDe.deserializeId(reader);
        consumer = onIdParsed.apply(id);
        consumer.id(id);
        continue;
      }

      if (consumer == null) {
        throw new IllegalStateException(
            String.format("Got property '%s', but '%s' must be the first property in every document", name, MongoBaseValue.ID));
      }

      BiConsumer<BaseValue, BsonReader> propertyProducer = propertyProducers.get(name);
      propertyProducer.accept(consumer, reader);
    }

    reader.readEndDocument();

    parsed.accept(id);
  }

  static void serializeKeyMutation(BsonWriter writer, Key.Mutation keyMutation) {
    writer.writeStartDocument();
    serializeKey(writer, mutationName(keyMutation.getType()), keyMutation.getKey());
    writer.writeEndDocument();
  }

  private static String mutationName(Key.MutationType type) {
    switch (type) {
      case ADDITION:
        return KEY_ADDITION;
      case REMOVAL:
        return KEY_REMOVAL;
      default:
        throw new IllegalArgumentException("unknown mutation type " + type);
    }
  }

  static List<Key.Mutation> deserializeKeyMutations(BsonReader bsonReader) {
    return deserializeArray(bsonReader, MongoSerDe::deserializeKeyMutation);
  }

  static Key.Mutation deserializeKeyMutation(BsonReader r) {
    r.readStartDocument();
    String addRemove = r.readName();
    Key key = deserializeKey(r);
    r.readEndDocument();
    switch (addRemove) {
      case KEY_ADDITION:
        return key.asAddition();
      case KEY_REMOVAL:
        return key.asRemoval();
      default:
        throw new IllegalArgumentException(String.format("Unsupported key '%s' in key-mutation map", addRemove));
    }
  }
}
