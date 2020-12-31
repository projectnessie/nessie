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

import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.FRAGMENTS;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.KEY_LIST;
import static com.dremio.nessie.versioned.store.dynamo.DynamoConstants.MUTATIONS;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.dremio.nessie.versioned.ImmutableKey;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.Persistent;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Preconditions;
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
public class AttributeValueUtil {

  /**
   * TODO javadoc for checkstyle.
   */
  public static <C extends DynamoConsumer<C>> Map<String, AttributeValue> fromSaveOp(SaveOp<?> saveOp) {
    if (saveOp.getType().isConsumerized()) {
      Persistent<DynamoConsumer<C>> persistent = (Persistent<DynamoConsumer<C>>) saveOp.getValue();
      Map<String, AttributeValue> ref = fromEntity(saveOp.toEntity());
      Map<String, AttributeValue> con = fromConsumer(persistent);
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
      boolean keysDiff = diff.containsKey(KEY_LIST);
      if (keysDiff) {
        diff.remove(KEY_LIST);
      }
      if (!diff.isEmpty()) {
        errors.append("\nDifferent: ").append(diff);
      }

      if (keysDiff) {
        Map<String, AttributeValue> refKeys = ref.get(KEY_LIST).m();
        Map<String, AttributeValue> conKeys = con.get(KEY_LIST).m();
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
                  refAdd.add(m.get("a").l().stream().map(AttributeValue::s).collect(Collectors.toList()));
                }
                if (m.containsKey("d")) {
                  refRem.add(m.get("d").l().stream().map(AttributeValue::s).collect(Collectors.toList()));
                }
              });
          conKeys.get(MUTATIONS).l().stream()
              .map(AttributeValue::m)
              .forEach(m -> {
                if (m.containsKey("a")) {
                  conAdd.add(m.get("a").l().stream().map(AttributeValue::s).collect(Collectors.toList()));
                }
                if (m.containsKey("d")) {
                  conRem.add(m.get("d").l().stream().map(AttributeValue::s).collect(Collectors.toList()));
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
          Set<ByteBuffer> refFrags = refKeys.get(FRAGMENTS).l().stream().map(a -> a.b().asByteBuffer()).collect(Collectors.toSet());
          Set<ByteBuffer> conFrags = conKeys.get(FRAGMENTS).l().stream().map(a -> a.b().asByteBuffer()).collect(Collectors.toSet());
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
            + ref.entrySet().stream().map(Object::toString).sorted().collect(Collectors.joining("\n    "))
            + "\nfromConsumer:\n    "
            + con.entrySet().stream().map(Object::toString).sorted().collect(Collectors.joining("\n    ")));
      }

      // END OF TEMPORARY COMPARISON-CODE

      return con;
    } else {
      return fromEntity(saveOp.toEntity());
    }
  }

  /**
   * Convert an attribute value to an entity.
   * @param av Attribute value to convert
   * @return Entity version of value
   */
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
   * TODO javadoc for checkstyle.
   */
  // todo make generic, Probably want to add this into ValueType somehow
  public static Map<String, Entity> toEntity(Map<String, AttributeValue> map) {
    ValueType valueType = ValueType.byValueName(map.get("t").s());
    if (valueType.isConsumerized()) {
      throw new UnsupportedOperationException("We should never ever get here!"); // TODO
    }
    return Maps.transformValues(map, AttributeValueUtil::toEntity);
  }

  /**
   * Convert from entity to AttributeValue.
   * @param e Entity to convert
   * @return AttributeValue to return
   */
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

  /**
   * TODO javadoc for checkstyle.
   */
  public static HasId toConsumer(ValueType valueType, Map<String, AttributeValue> attributeMap) {
    HasId item = DynamoConsumer.newProducer(valueType)
        .deserialize(attributeMap);

    // TODO remove this sanity check
    Map<String, Entity> oldOne = Maps.transformValues(attributeMap, AttributeValueUtil::toEntity);
    Object oldItem = valueType.getSchema().mapToItem(valueType.checkType(oldOne));
    if (!item.equals(oldItem)) {
      throw new IllegalStateException("Uh! " + valueType + " item building is broken");
    }

    return item;
  }

  /**
   * TODO add some javadoc.
   */
  public static Key deserializeKey(AttributeValue raw) {
    ImmutableKey.Builder keyBuilder = ImmutableKey.builder();
    for (AttributeValue keyPart : raw.l()) {
      keyBuilder.addElements(keyPart.s());
    }
    return keyBuilder.build();
  }

  /**
   * TODO add some javadoc.
   */
  public static List<Id> deserializeIdList(AttributeValue raw) {
    return raw.l()
        .stream()
        .map(AttributeValueUtil::deserializeId)
        .collect(Collectors.toList());
  }

  /**
   * TODO javadoc.
   */
  public static <C extends DynamoConsumer<C>> Map<String, AttributeValue> fromConsumer(Persistent<DynamoConsumer<C>> value) {
    DynamoConsumer<C> consumer = DynamoConsumer.newConsumer(value.type());
    value.applyToConsumer(consumer);
    return consumer.getEntity();
  }

  public static Id deserializeId(Map<String, AttributeValue> item) {
    return deserializeId(item.get(Store.KEY_NAME));
  }

  public static Id deserializeId(AttributeValue raw) {
    return Id.of(raw.b().asByteArrayUnsafe());
  }

  public static AttributeValue serializeId(Id id) {
    return AttributeValue.builder().b(SdkBytes.fromByteBuffer(id.getValue().asReadOnlyByteBuffer())).build();
  }

  /**
   * Deserialize the given {code map} as the given {@link ValueType type}.
   */
  @SuppressWarnings("unchecked")
  public static <V> V deserialize(ValueType valueType, Map<String, AttributeValue> item) {
    checkType(valueType, item);
    if (valueType.isConsumerized()) {
      return (V) toConsumer(valueType, item);
    }
    // TODO once all types have been migrated off of `Entity`, get rid of this one.
    return (V) valueType.getSchema().mapToItem(AttributeValueUtil.toEntity(item));
  }

  // Adopted from ValueType.checkType()
  private static void checkType(ValueType valueType, Map<String, AttributeValue> map) {
    Preconditions.checkNotNull(map, "map parameter is null");
    String loadedType = map.get(ValueType.SCHEMA_TYPE).s();
    Id id = deserializeId(map.get(Store.KEY_NAME));
    Preconditions.checkNotNull(loadedType, "Missing type tag for schema for id %s.", id.getHash());
    Preconditions.checkArgument(valueType.getValueName().equals(loadedType),
        "Expected schema for id %s to be of type '%s' but is actually '%s'.", id.getHash(), valueType.getValueName(), loadedType);
  }
}
