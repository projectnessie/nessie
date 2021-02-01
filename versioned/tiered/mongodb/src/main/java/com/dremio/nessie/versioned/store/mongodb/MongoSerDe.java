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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.tiered.builder.BaseWrappedValue;
import com.dremio.nessie.tiered.builder.Fragment;
import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.tiered.builder.L2;
import com.dremio.nessie.tiered.builder.L3;
import com.dremio.nessie.tiered.builder.Ref;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

@SuppressWarnings({"unchecked", "rawtypes"})
final class MongoSerDe {
  private static final Map<ValueType<?>, Function<BsonWriter, MongoBaseValue>> CONSUMERS =
      ImmutableMap.<ValueType<?>, Function<BsonWriter, MongoBaseValue>>builder()
          .put(ValueType.L1, MongoL1::new)
          .put(ValueType.L2, MongoL2::new)
          .put(ValueType.L3, MongoL3::new)
          .put(ValueType.KEY_FRAGMENT, MongoFragment::new)
          .put(ValueType.REF, MongoRef::new)
          .put(ValueType.VALUE, MongoWrappedValue::new)
          .put(ValueType.COMMIT_METADATA, MongoWrappedValue::new)
          .build();
  private static final Map<ValueType<?>, BiConsumer<Document, BaseValue>> DESERIALIZERS =
      ImmutableMap.<ValueType<?>, BiConsumer<Document, BaseValue>>builder()
          .put(ValueType.L1, (d, c) -> MongoL1.produce(d, (L1) c))
          .put(ValueType.L2, (d, c) -> MongoL2.produce(d, (L2) c))
          .put(ValueType.L3, (d, c) -> MongoL3.produce(d, (L3) c))
          .put(ValueType.KEY_FRAGMENT, (d, c) -> MongoFragment.produce(d, (Fragment) c))
          .put(ValueType.REF, (d, c) -> MongoRef.produce(d, (Ref) c))
          .put(ValueType.VALUE, (d, c) -> MongoWrappedValue.produce(d, (BaseWrappedValue) c))
          .put(ValueType.COMMIT_METADATA, (d, c) -> MongoWrappedValue.produce(d, (BaseWrappedValue) c))
          .build();

  private static final String KEY_ADDITION = "a";
  private static final String KEY_REMOVAL = "d";

  static {
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

  static <C extends BaseValue<C>> void produceToConsumer(Document d, ValueType<C> valueType, C consumer) {
    DESERIALIZERS.get(valueType).accept(d, consumer);
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

  static Id deserializeId(Document d, String param) {
    return Id.of(((Binary) d.get(param)).getData());
  }

  static Stream<Id> deserializeIds(Document d, String param) {
    List<Binary> ids = (List<Binary>) d.get(param);
    return ids.stream()
        .map(b -> Id.of(b.getData()));
  }

  static BsonBinary serializeBytes(ByteString value) {
    return new BsonBinary(value.toByteArray());
  }

  static Stream<Key> deserializeKeys(Document document, String param) {
    List<Object> l = (List<Object>) document.get(param);
    return l.stream()
        .map(o -> (List<String>) o)
        .map(MongoSerDe::deserializeKey);
  }

  static void serializeKey(BsonWriter bsonWriter, String prop, Key key) {
    bsonWriter.writeStartArray(prop);
    key.getElements().forEach(bsonWriter::writeString);
    bsonWriter.writeEndArray();
  }

  static Key deserializeKey(List<String> lst) {
    return Key.of(lst.toArray(new String[0]));
  }

  static <X> void serializeArray(BsonWriter writer, String prop, Stream<X> src, BiConsumer<BsonWriter, X> inner) {
    writer.writeStartArray(prop);
    src.forEach(e -> inner.accept(writer, e));
    writer.writeEndArray();
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

  static Stream<Key.Mutation> deserializeKeyMutations(Document d, String param) {
    List<Document> kms = (List<Document>) d.get(param);
    return kms.stream().map(MongoSerDe::deserializeKeyMutation);
  }

  private static Key.Mutation deserializeKeyMutation(Document d) {
    Entry<String, Object> e = d.entrySet().stream().findFirst().get();
    String addRemove = e.getKey();
    Key key = deserializeKey((List<String>) e.getValue());
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
