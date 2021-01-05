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
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder;

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

  /**
   * Convenience method to produce a built {@link AttributeValue} for an {@link Key},
   * consist of a list of strings.
   */
  static AttributeValue keyElements(Key key) {
    Preconditions.checkNotNull(key);
    return list(key.getElements().stream().map(AttributeValueUtil::string));
  }

  /**
   * Convenience method to produce a built list of {@link Builder#b(SdkBytes)} from a
   * stream of {@link Id}s.
   */
  static AttributeValue idsList(Stream<Id> ids) {
    Preconditions.checkNotNull(ids);
    return list(ids.map(AttributeValueUtil::idValue));
  }

  /**
   * Convenience method to produce a built {@link Builder#b(SdkBytes)} for an {@link Id}.
   */
  static AttributeValue idValue(Id id) {
    Preconditions.checkNotNull(id);
    return bytes(id.getValue());
  }

  /**
   * Convenience method to produce a built {@link Builder#b(SdkBytes)}.
   */
  static AttributeValue bytes(ByteString bytes) {
    Preconditions.checkNotNull(bytes);
    return builder().b(SdkBytes.fromByteBuffer(bytes.asReadOnlyByteBuffer())).build();
  }

  /**
   * Convenience method to produce a built {@link Builder#bool(Boolean)}.
   */
  static AttributeValue bool(boolean bool) {
    return builder().bool(bool).build();
  }

  /**
   * Convenience method to produce a built {@link Builder#n(String)}.
   */
  static AttributeValue number(long number) {
    return builder().n(Long.toString(number)).build();
  }

  /**
   * Convenience method to produce a built {@link Builder#s(String)}.
   */
  static AttributeValue string(String string) {
    return builder().s(Preconditions.checkNotNull(string)).build();
  }

  /**
   * Convenience method to produce a built {@link Builder#l()}.
   */
  static AttributeValue list(Stream<AttributeValue> list) {
    Preconditions.checkNotNull(list);
    return builder().l(list.collect(toList())).build();
  }

  /**
   * Convenience method to produce a built {@link AttributeValue.Builder#m(Map)}.
   */
  static AttributeValue map(Map<String, AttributeValue> map) {
    Preconditions.checkNotNull(map);
    return builder().m(map).build();
  }

  /**
   * Convenience method to produce a built {@link AttributeValue.Builder#m(Map)} with a single entry.
   */
  static AttributeValue singletonMap(String key, AttributeValue value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    return builder().m(Collections.singletonMap(key, value)).build();
  }

  /**
   * Deserializes key-mutations the given {@code key} from {@code map} and passes
   * key-additions to {@code addConsumer} and key-removals to {@code removalConsumer}.
   */
  static void deserializeKeyMutations(
      Map<String, AttributeValue> map,
      String key,
      Consumer<Key> addConsumer,
      Consumer<Key> removalConsumer
  ) {
    List<AttributeValue> mutations = mandatoryList(attributeValue(map, key));
    for (AttributeValue mutation : mutations) {
      Map<String, AttributeValue> m = mutation.m();
      int sz = m.size();
      AttributeValue raw = m.get(DynamoConsumer.KEY_ADDITION);
      if (raw != null) {
        addConsumer.accept(deserializeKey(raw));
        sz--;
      }
      raw = m.get(DynamoConsumer.KEY_REMOVAL);
      if (raw != null) {
        removalConsumer.accept(deserializeKey(raw));
        sz--;
      }
      if (sz > 0) {
        throw new IllegalStateException("keys.mutations map has unsupported entries: " + m);
      }
    }
  }

  /**
   * Deserialize the given {@code key} from {@code map}.
   *
   * @throws IllegalArgumentException if {@code key} is not present.
   * @throws NullPointerException if {@code key} or {@code map} are null.
   */
  static AttributeValue attributeValue(Map<String, AttributeValue> map, String key) {
    Preconditions.checkNotNull(map);
    Preconditions.checkNotNull(key);
    AttributeValue av = map.get(key);
    if (av == null) {
      throw new IllegalArgumentException("Missing mandatory attribute '" + key + "'");
    }
    return av;
  }

  /**
   * Deserialize the given {@code key} from {@code map} as a {@link Key}.
   *
   * @throws IllegalArgumentException if {@code key} is not present.
   * @throws NullPointerException if {@code key} or {@code map} are null.
   * @see #deserializeKey(AttributeValue)
   */
  static Key deserializeKey(Map<String, AttributeValue> map, String key) {
    AttributeValue raw = attributeValue(map, key);
    return deserializeKey(raw);
  }

  /**
   * Deserialize a {@link Key} from the given {@code raw}.
   */
  static Key deserializeKey(AttributeValue raw) {
    ImmutableKey.Builder keyBuilder = ImmutableKey.builder();
    for (AttributeValue keyPart : mandatoryList(raw)) {
      keyBuilder.addElements(Preconditions.checkNotNull(keyPart.s(), "mandatory part of key-list is not a string"));
    }
    return keyBuilder.build();
  }

  /**
   * Extracts {@link AttributeValue#l() raw.l()}.
   *
   * @throws NullPointerException if {@code raw} or {@code raw.l()} are null.
   */
  static List<AttributeValue> mandatoryList(AttributeValue raw) {
    Preconditions.checkNotNull(raw);
    return Preconditions.checkNotNull(raw.l(), "mandatory list value is null");
  }

  /**
   * Extracts {@link AttributeValue#m() raw.m()}.
   *
   * @throws NullPointerException if {@code raw} or {@code raw.l()} are null.
   */
  static Map<String, AttributeValue> mandatoryMap(AttributeValue raw) {
    Preconditions.checkNotNull(raw);
    return Preconditions.checkNotNull(raw.m(), "mandatory map value is null");
  }

  /**
   * Deserialize the given {@code key} from {@code map} as a {@link Stream} of {@link Id}s.
   *
   * @throws IllegalArgumentException if {@code key} is not present.
   * @throws NullPointerException if {@code key} or {@code map} are null.
   */
  static Stream<Id> deserializeIdStream(Map<String, AttributeValue> map, String key) {
    AttributeValue raw = attributeValue(map, key);
    return raw.l()
        .stream()
        .map(AttributeValueUtil::deserializeId);
  }

  /**
   * Deserialize the given {@code key} from {@code map} as an {@link Id}.
   *
   * @throws IllegalArgumentException if {@code key} is not present.
   * @throws NullPointerException if {@code key} or {@code map} are null.
   */
  static Id deserializeId(Map<String, AttributeValue> map, String key) {
    AttributeValue raw = attributeValue(map, key);
    return deserializeId(raw);
  }

  /**
   * Deserialize an {@link Id} from {@code raw}.
   *
   * @throws NullPointerException if {@code raw} or {@code raw.b()} are null.
   */
  static Id deserializeId(AttributeValue raw) {
    SdkBytes b = Preconditions.checkNotNull(raw.b(), "mandatory binary value is null");
    return Id.of(b.asByteArrayUnsafe());
  }

  /**
   * Deserialize the given {@code key} from {@code map} as an {@code int}.
   *
   * @throws IllegalArgumentException if {@code key} is not present.
   * @throws NullPointerException if {@code key} or {@code map} are null.
   */
  static int deserializeInt(Map<String, AttributeValue> map, String key) {
    return Ints.saturatedCast(deserializeLong(map, key));
  }

  /**
   * Deserialize the given {@code key} from {@code map} as a {@code long}.
   *
   * @throws IllegalArgumentException if {@code key} is not present.
   * @throws NullPointerException if {@code key} or {@code map} are null.
   */
  static long deserializeLong(Map<String, AttributeValue> map, String key) {
    AttributeValue raw = attributeValue(map, key);
    String b = Preconditions.checkNotNull(raw.n(), "mandatory number value is null");
    return Long.parseLong(raw.n());
  }

  /**
   * Deserialize the given {@code key} from {@code map} as a {@link ByteString}.
   *
   * @throws IllegalArgumentException if {@code key} is not present.
   * @throws NullPointerException if {@code key} or {@code map} are null.
   */
  static ByteString deserializeBytes(Map<String, AttributeValue> map, String key) {
    AttributeValue raw = attributeValue(map, key);
    SdkBytes b = Preconditions.checkNotNull(raw.b(), "mandatory binary value is null");
    return ByteString.copyFrom(b.asByteArrayUnsafe());
  }
}
