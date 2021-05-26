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
package org.projectnessie.versioned.dynamodb;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.projectnessie.versioned.ImmutableKey;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.WithPayload;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.tiered.Mutation;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder;

/** Tools to convert to and from Entity/AttributeValue. */
public final class AttributeValueUtil {
  private static final char ZERO_BYTE = '\u0000';
  private static final String KEY_ADDITION = "a";
  private static final String KEY_REMOVAL = "d";
  private static final String DT = "dt";

  private AttributeValueUtil() {
    // empty
  }

  /**
   * Convert an attribute value to an entity.
   *
   * @param av Attribute value to convert
   * @return Entity version of value
   */
  @Deprecated
  public static Entity toEntity(AttributeValue av) {
    if (av.hasL()) {
      return Entity.ofList(
          av.l().stream()
              .map(AttributeValueUtil::toEntity)
              .collect(ImmutableList.toImmutableList()));
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
   *
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
   * Convenience method to produce a built {@link AttributeValue} for an {@link Key}, consist of a
   * list of strings.
   */
  static AttributeValue keyElementsWithPayload(WithPayload<Key> key) {
    Stream<AttributeValue> keyValue =
        checkNotNull(key.getValue()).getElements().stream().map(AttributeValueUtil::string);
    AttributeValue payload;
    if (key.getPayload() == null) {
      payload = AttributeValueUtil.string(Character.toString(ZERO_BYTE));
    } else {
      payload = AttributeValueUtil.string(key.getPayload().toString());
    }
    return list(Stream.concat(Stream.of(payload), keyValue));
  }

  static long getDt(Map<String, AttributeValue> map) {
    AttributeValue av = map.get(DT);
    if (av == null) {
      return 0;
    }

    return Long.parseLong(av.n());
  }

  /**
   * Convenience method to produce a built list of {@link Builder#b(SdkBytes)} from a stream of
   * {@link Id}s.
   */
  static AttributeValue idsList(Stream<Id> ids) {
    return list(checkNotNull(ids).map(AttributeValueUtil::idValue));
  }

  /** Convenience method to produce a built {@link Builder#b(SdkBytes)} for an {@link Id}. */
  static AttributeValue idValue(Id id) {
    return bytes(checkNotNull(id).getValue());
  }

  /** Convenience method to produce a built {@link Builder#b(SdkBytes)}. */
  static AttributeValue bytes(ByteString bytes) {
    return builder().b(SdkBytes.fromByteBuffer(checkNotNull(bytes).asReadOnlyByteBuffer())).build();
  }

  /** Convenience method to produce a built {@link Builder#bool(Boolean)}. */
  static AttributeValue bool(boolean bool) {
    return builder().bool(bool).build();
  }

  /** Convenience method to produce a built {@link Builder#n(String)}. */
  static AttributeValue number(long number) {
    return builder().n(Long.toString(number)).build();
  }

  /** Convenience method to produce a built {@link Builder#s(String)}. */
  static AttributeValue string(String string) {
    return builder().s(checkNotNull(string)).build();
  }

  /** Convenience method to produce a built {@link Builder#l()}. */
  static AttributeValue list(Stream<AttributeValue> list) {
    return builder().l(checkNotNull(list).collect(toList())).build();
  }

  /** Convenience method to produce a built {@link AttributeValue.Builder#m(Map)}. */
  static AttributeValue map(Map<String, AttributeValue> map) {
    return builder().m(checkNotNull(map)).build();
  }

  /** Deserializes a single key-mutation. */
  static Mutation deserializeKeyMutation(AttributeValue mutation) {
    Map<String, AttributeValue> m = mutation.m();
    AttributeValue raw = m.get(KEY_ADDITION);
    if (raw != null) {
      WithPayload<Key> key = deserializeKeyWithPayload(raw);
      return Mutation.Addition.of(key.getValue(), key.getPayload());
    }
    raw = m.get(KEY_REMOVAL);
    if (raw != null) {
      WithPayload<Key> key = deserializeKeyWithPayload(raw);
      return Mutation.Removal.of(key.getValue());
    }
    throw new IllegalStateException("keys.mutations map has unsupported entries: " + m);
  }

  static AttributeValue serializeKeyMutation(Mutation km) {
    WithPayload<Key> key;
    switch (km.getType()) {
      case ADDITION:
        key = WithPayload.of(((Mutation.Addition) km).getPayload(), km.getKey());
        break;
      case REMOVAL:
        key = WithPayload.of(null, km.getKey());
        break;
      default:
        throw new IllegalArgumentException("unknown mutation type " + km.getType());
    }
    return map(Collections.singletonMap(mutationName(km.getType()), keyElementsWithPayload(key)));
  }

  private static String mutationName(Mutation.MutationType type) {
    switch (type) {
      case ADDITION:
        return KEY_ADDITION;
      case REMOVAL:
        return KEY_REMOVAL;
      default:
        throw new IllegalArgumentException("unknown mutation type " + type);
    }
  }

  /**
   * Deserialize the given {@code key} from {@code map}.
   *
   * @throws IllegalArgumentException if {@code key} is not present.
   * @throws NullPointerException if {@code key} or {@code map} are null.
   */
  static AttributeValue attributeValue(Map<String, AttributeValue> map, String key) {
    checkNotNull(map);
    checkNotNull(key);
    AttributeValue av = map.get(key);
    if (av == null) {
      throw new IllegalArgumentException("Missing mandatory attribute '" + key + "'");
    }
    return av;
  }

  /** Deserialize a {@link Key} from the given {@code raw}. */
  static WithPayload<Key> deserializeKeyWithPayload(AttributeValue raw) {
    ImmutableKey.Builder keyBuilder = ImmutableKey.builder();
    String payloadString = raw.l().get(0).s();
    Byte payload = null;
    int skip = 0;
    try {
      payload = (payloadString.charAt(0) == ZERO_BYTE) ? null : Byte.parseByte(payloadString);
      skip = 1;
    } catch (NumberFormatException e) {
      // assume old client first element is actually first element of key
      // todo remove once format is stable
    }
    raw.l().stream().skip(skip).forEach(keyPart -> keyBuilder.addElements(keyPart.s()));
    return WithPayload.of(payload, keyBuilder.build());
  }

  /**
   * Deserialize the given {@code key} from {@code map} as a {@link Stream} of {@link Id}s.
   *
   * @throws IllegalArgumentException if {@code key} is not present.
   * @throws NullPointerException if {@code key} or {@code map} are null.
   */
  static Stream<Id> deserializeIdStream(Map<String, AttributeValue> map, String key) {
    return attributeValue(map, key).l().stream().map(AttributeValueUtil::deserializeId);
  }

  /**
   * Deserialize the given {@code key} from {@code map} as an {@link Id}.
   *
   * @throws IllegalArgumentException if {@code key} is not present.
   * @throws NullPointerException if {@code key} or {@code map} are null.
   */
  static Id deserializeId(Map<String, AttributeValue> map, String key) {
    return deserializeId(attributeValue(map, key));
  }

  /**
   * Deserialize an {@link Id} from {@code raw}.
   *
   * @throws NullPointerException if {@code raw} or {@code raw.b()} are null.
   */
  private static Id deserializeId(AttributeValue raw) {
    return Id.of(checkNotNull(raw.b(), "mandatory binary value is null").asByteArrayUnsafe());
  }

  /**
   * Deserialize the given {@code key} from {@code map} as an {@code int}.
   *
   * @throws IllegalArgumentException if {@code key} is not present.
   * @throws NullPointerException if {@code key} or {@code map} are null.
   */
  static int deserializeInt(Map<String, AttributeValue> map, String key) {
    return Ints.saturatedCast(
        Long.parseLong(
            checkNotNull(attributeValue(map, key).n(), "mandatory number value is null")));
  }
}
