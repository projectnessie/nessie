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

import static com.dremio.nessie.versioned.store.dynamo.DynamoL1Consumer.FRAGMENTS;
import static com.dremio.nessie.versioned.store.dynamo.DynamoL1Consumer.MUTATIONS;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.google.protobuf.UnsafeByteOperations;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Tools to convert to and from Entity/AttributeValue.
 */
@Deprecated // TOOD REMOVE THIS CLASS
public class AttributeValueUtil {

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
        return AttributeValue.builder().b(SdkBytes.fromByteBuffer(e.getBinary().asReadOnlyByteBuffer())).build();
      case BOOLEAN:
        return AttributeValue.builder().bool(e.getBoolean()).build();
      case LIST:
        return AttributeValue.builder().l(e.getList().stream().map(AttributeValueUtil::fromEntity)
            .collect(ImmutableList.toImmutableList())).build();
      case MAP:
        return AttributeValue.builder().m(fromEntity(e.getMap())).build();
      case NUMBER:
        return AttributeValue.builder().n(String.valueOf(e.getNumber())).build();
      case STRING:
        return AttributeValue.builder().s(e.getString()).build();
      default:
        throw new UnsupportedOperationException("Unable to convert type " + e);
    }
  }

  /**
   * TODO javadoc for checkstyle.
   */
  @Deprecated
  public static Map<String, AttributeValue> fromEntity(Map<String, Entity> map) {
    return Maps.transformValues(map, AttributeValueUtil::fromEntity);
  }

  // TODO REMOVE THE FOLLOWING STUFF

  static <V> void sanityCheckFromSaveOp(ValueType type, V value, Map<String, AttributeValue> con) {
    SimpleSchema<V> schema = type.getSchema();
    Map<String, Entity> x = schema.itemToMap(value, true);
    Map<String, AttributeValue> ref = Maps.transformValues(x, AttributeValueUtil::fromEntity);

    MapDifference<String, AttributeValue> mapDiff = Maps.difference(ref, con);

    StringBuilder errors = new StringBuilder();
    Map<String, AttributeValue> onlyInRef = mapDiff.entriesOnlyOnLeft();
    Map<String, AttributeValue> onlyInCon = mapDiff.entriesOnlyOnRight();
    Map<String, ValueDifference<AttributeValue>> diff = new HashMap<>(mapDiff.entriesDiffering());
    if (!onlyInRef.isEmpty()) {
      errors.append("\nOnly in 'fromEntity': ").append(onlyInRef);
    }
    if (!onlyInCon.isEmpty()) {
      errors.append("\nOnly in 'fromConsumer': ").append(onlyInCon);
    }
    boolean keysDiff = diff.containsKey("keys");
    if (keysDiff) {
      diff.remove("keys");
    }
    if (!diff.isEmpty()) {
      errors.append("\nDifferent: ").append(diff);
    }

    if (keysDiff) {
      Map<String, AttributeValue> refKeys = ref.get("keys").m();
      Map<String, AttributeValue> conKeys = con.get("keys").m();
      mapDiff = Maps.difference(refKeys, conKeys);
      onlyInRef = mapDiff.entriesOnlyOnLeft();
      onlyInCon = mapDiff.entriesOnlyOnRight();
      diff = new HashMap<>(mapDiff.entriesDiffering());
      if (!onlyInRef.isEmpty()) {
        errors.append("\nOnly in 'fromEntity.keys': ").append(onlyInRef);
      }
      if (!onlyInCon.isEmpty()) {
        errors.append("\nOnly in 'fromConsumer.keys': ").append(onlyInCon);
      }
      boolean mutationsDiff = diff.containsKey(MUTATIONS);
      boolean fragmentsDiff = diff.containsKey(FRAGMENTS);
      if (mutationsDiff) {
        diff.remove(MUTATIONS);
        Set<List<String>> refAdd = new HashSet<>();
        Set<List<String>> refRem = new HashSet<>();
        Set<List<String>> conAdd = new HashSet<>();
        Set<List<String>> conRem = new HashSet<>();
        refKeys.get(MUTATIONS).l().stream()
            .map(AttributeValue::m)
            .forEach(m -> {
              if (m.containsKey("a")) {
                refAdd.add(
                    m.get("a").l().stream().map(AttributeValue::s).collect(Collectors.toList()));
              }
              if (m.containsKey("d")) {
                refRem.add(
                    m.get("d").l().stream().map(AttributeValue::s).collect(Collectors.toList()));
              }
            });
        conKeys.get(MUTATIONS).l().stream()
            .map(AttributeValue::m)
            .forEach(m -> {
              if (m.containsKey("a")) {
                conAdd.add(
                    m.get("a").l().stream().map(AttributeValue::s).collect(Collectors.toList()));
              }
              if (m.containsKey("d")) {
                conRem.add(
                    m.get("d").l().stream().map(AttributeValue::s).collect(Collectors.toList()));
              }
            });
        if (!refAdd.equals(conAdd) || !refRem.equals(conRem)) {
          errors.append("\nfromEntity.keys.mutations(a): ").append(refAdd);
          errors.append("\nfromEntity.keys.mutations(d): ").append(refRem);
          errors.append("\nfromConsumer.keys.mutations(a): ").append(conAdd);
          errors.append("\nfromConsumer.keys.mutations(d): ").append(conRem);
        }
      }
      if (fragmentsDiff) {
        diff.remove(FRAGMENTS);
        Set<ByteBuffer> refFrags = refKeys.get(FRAGMENTS).l().stream()
            .map(a -> a.b().asByteBuffer()).collect(Collectors.toSet());
        Set<ByteBuffer> conFrags = conKeys.get(FRAGMENTS).l().stream()
            .map(a -> a.b().asByteBuffer()).collect(Collectors.toSet());
        if (!refFrags.equals(conFrags)) {
          errors.append("\nfromEntity.keys.fragments: ").append(refFrags);
          errors.append("\nfromConsumer.keys.fragments: ").append(conFrags);
        }
      }
      if (!diff.isEmpty()) {
        errors.append("\nDifferent in 'keys': ").append(diff);
      }
    }

    if (errors.length() > 0) {
      throw new AssertionError("Ugh! fromEntity is different from fromConsumer!"
          + errors
          + "\nfromEntity:\n    "
          + ref.entrySet().stream().map(Object::toString).sorted()
          .collect(Collectors.joining("\n    "))
          + "\nfromConsumer:\n    "
          + con.entrySet().stream().map(Object::toString).sorted()
          .collect(Collectors.joining("\n    ")));
    }
  }

  static void sanityCheckToConsumer(Map<String, AttributeValue> attributeMap,
      ValueType valueType, HasId item) {
    Map<String, Entity> oldOne = Maps.transformValues(attributeMap, AttributeValueUtil::toEntity);
    Object oldItem = valueType.getSchema().mapToItem(valueType.checkType(oldOne));
    if (!item.equals(oldItem)) {
      throw new IllegalStateException("Uh! " + valueType + " item building is broken");
    }
  }
}
