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

import java.util.List;
import java.util.Map;

import com.dremio.nessie.versioned.store.Entity;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.protobuf.UnsafeByteOperations;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Tools to convert to and from Entity/AttributeValue.
 */
public class AttributeValueUtil {

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
      return Entity.ofNumber(av.n());
    } else if (av.b() != null) {
      return Entity.ofBinary(UnsafeByteOperations.unsafeWrap(av.b().asByteArray()));
    } else if (av.hasSs()) {
      return Entity.ofStringSet(av.ss().stream().collect(ImmutableSet.toImmutableSet()));
    } else {
      throw new UnsupportedOperationException("Unable to convert: " + av.toString());
    }
  }

  public static Map<String, Entity> toEntity(Map<String, AttributeValue> map) {
    return Maps.transformValues(map, AttributeValueUtil::toEntity);
  }

  public static List<Entity> toEntity(List<AttributeValue> list) {
    return list.stream().map(AttributeValueUtil::toEntity).collect(ImmutableList.toImmutableList());
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
        return AttributeValue.builder().n(e.getNumber()).build();
      case STRING:
        return AttributeValue.builder().s(e.getString()).build();
      case STRING_SET:
        return AttributeValue.builder().ss(e.getStringSet()).build();
      default:
        throw new UnsupportedOperationException("Unable to convert type " + e);
    }
  }

  public static Map<String, AttributeValue> fromEntity(Map<String, Entity> map) {
    return Maps.transformValues(map, AttributeValueUtil::fromEntity);
  }

  public static List<AttributeValue> fromEntity(List<Entity> list) {
    return list.stream().map(AttributeValueUtil::fromEntity).collect(ImmutableList.toImmutableList());
  }


}
