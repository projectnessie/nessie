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

import static java.util.stream.Collectors.toList;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.dremio.nessie.versioned.ImmutableKey;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Tools to convert to and from Entity/AttributeValue.
 */
public final class AttributeValueUtil {

  private AttributeValueUtil() {
    // empty
  }

  /**
   * Convert an attribute value to an entity.
   * @param av Attribute value to convert
   * @return Entity version of value
   */
  @Deprecated
  public static Entity toEntity(AttributeValue av) {
    if (av.hasL()) {
      return Entity.ofList(av.l().stream().map(AttributeValueUtil::toEntity).collect(ImmutableList.toImmutableList()));
    } else if (av.hasM()) {
      return Entity.ofMap(Maps.transformValues(av.m(), AttributeValueUtil::toEntity));
    } else if (av.s() != null) {
      return Entity.ofString(av.s());
    } else if (av.bool() != null) {
      return Entity.ofBoolean(av.bool());
    } else if (av.n() != null) {
      return Entity.ofNumber(Long.parseLong(av.n()));
    } else if (av.b() != null) {
      return Entity.ofBinary(UnsafeByteOperations.unsafeWrap(av.b().asByteArray()));
    } else {
      throw new UnsupportedOperationException("Unable to convert: " + av.toString());
    }
  }

  /**
   * Convert from entity to AttributeValue.
   * @param e Entity to convert
   * @return AttributeValue to return
   */
  @Deprecated
  public static AttributeValue fromEntity(Entity e) {
    switch (e.getType()) {
      case BINARY:
        return bytes(e.getBinary());
      case BOOLEAN:
        return bool(e.getBoolean());
      case LIST:
        return list(e.getList().stream().map(AttributeValueUtil::fromEntity));
      case MAP:
        return map(Maps.transformValues(e.getMap(), AttributeValueUtil::fromEntity));
      case NUMBER:
        return builder().n(String.valueOf(e.getNumber())).build();
      case STRING:
        return string(e.getString());
      default:
        throw new UnsupportedOperationException("Unable to convert type " + e);
    }
  }

  static AttributeValue keyElements(Key key) {
    Preconditions.checkNotNull(key);
    return list(key.getElements().stream().map(AttributeValueUtil::string));
  }

  static AttributeValue idsList(Stream<Id> ids) {
    Preconditions.checkNotNull(ids);
    return list(ids.map(AttributeValueUtil::idValue));
  }

  static AttributeValue idValue(Id id) {
    Preconditions.checkNotNull(id);
    return bytes(id.getValue());
  }

  static AttributeValue bytes(ByteString bytes) {
    Preconditions.checkNotNull(bytes);
    return builder().b(SdkBytes.fromByteBuffer(bytes.asReadOnlyByteBuffer())).build();
  }

  static AttributeValue bool(boolean bool) {
    return builder().bool(bool).build();
  }

  static AttributeValue number(int number) {
    return builder().n(Integer.toString(number)).build();
  }

  static AttributeValue string(String string) {
    return builder().s(Preconditions.checkNotNull(string)).build();
  }

  static AttributeValue list(Stream<AttributeValue> list) {
    Preconditions.checkNotNull(list);
    return builder().l(list.collect(toList())).build();
  }

  static AttributeValue map(Map<String, AttributeValue> map) {
    Preconditions.checkNotNull(map);
    return builder().m(map).build();
  }

  static AttributeValue singletonMap(String key, AttributeValue value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    return builder().m(Collections.singletonMap(key, value)).build();
  }

  static void deserializeKeyMutations(
      Map<String, AttributeValue> map,
      String key,
      Consumer<Key> addConsumer,
      Consumer<Key> removalConsumer
  ) {
    List<AttributeValue> mutations = mandatoryList(attributeValue(map, key));
    for (AttributeValue mutation : mutations) {
      Map<String, AttributeValue> m = mutation.m();
      if (m.size() > 2) {
        throw new IllegalStateException("Ugh - got a keys.mutations map like this: " + m);
      }
      AttributeValue raw = m.get(DynamoConsumer.KEY_ADDITION);
      if (raw != null) {
        addConsumer.accept(deserializeKey(raw));
      }
      raw = m.get(DynamoConsumer.KEY_REMOVAL);
      if (raw != null) {
        removalConsumer.accept(deserializeKey(raw));
      }
    }
  }

  static AttributeValue attributeValue(Map<String, AttributeValue> map, String key) {
    Preconditions.checkNotNull(map);
    Preconditions.checkNotNull(key);
    AttributeValue av = map.get(key);
    if (av == null) {
      throw new IllegalStateException("Missing mandatory attribtue '" + key + "'");
    }
    return av;
  }

  static Key deserializeKey(Map<String, AttributeValue> map, String key) {
    AttributeValue raw = attributeValue(map, key);
    return deserializeKey(raw);
  }

  static Key deserializeKey(AttributeValue raw) {
    ImmutableKey.Builder keyBuilder = ImmutableKey.builder();
    for (AttributeValue keyPart : mandatoryList(raw)) {
      keyBuilder.addElements(Preconditions.checkNotNull(keyPart.s(), "mandatory part of key-list is not a string"));
    }
    return keyBuilder.build();
  }

  static List<AttributeValue> mandatoryList(AttributeValue raw) {
    Preconditions.checkNotNull(raw);
    return Preconditions.checkNotNull(raw.l(), "mandatory list value is null");
  }

  static Map<String, AttributeValue> mandatoryMap(AttributeValue raw) {
    Preconditions.checkNotNull(raw);
    return Preconditions.checkNotNull(raw.m(), "mandatory map value is null");
  }

  static Stream<Id> deserializeIdStream(Map<String, AttributeValue> map, String key) {
    AttributeValue raw = attributeValue(map, key);
    return raw.l()
        .stream()
        .map(AttributeValueUtil::deserializeId);
  }

  static Id deserializeId(Map<String, AttributeValue> map, String key) {
    AttributeValue raw = attributeValue(map, key);
    return deserializeId(raw);
  }

  static Id deserializeId(AttributeValue raw) {
    SdkBytes b = Preconditions.checkNotNull(raw.b(), "mandatory binary value is null");
    return Id.of(b.asByteArrayUnsafe());
  }

  static int deserializeInt(Map<String, AttributeValue> map, String key) {
    AttributeValue raw = attributeValue(map, key);
    String b = Preconditions.checkNotNull(raw.n(), "mandatory number value is null");
    return Integer.parseInt(raw.n());
  }

  static ByteString deserializeBytes(Map<String, AttributeValue> map, String key) {
    AttributeValue raw = attributeValue(map, key);
    SdkBytes b = Preconditions.checkNotNull(raw.b(), "mandatory binary value is null");
    return ByteString.copyFrom(b.asByteArrayUnsafe());
  }

  /**
   * A "cast to everything" helper method - looks neat, but it's actually not.
   */
  @SuppressWarnings("unchecked")
  static <T> T cast(Object o) {
    return (T) o;
  }
}
