/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.dynamodb;

import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteBuffer;
import static software.amazon.awssdk.core.SdkBytes.fromByteBuffer;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class DynamoDBSerde {

  private static final String COL_STRIPES_FIRST_KEY = "f";
  private static final String COL_STRIPES_LAST_KEY = "l";
  private static final String COL_STRIPES_SEGMENT = "s";

  private DynamoDBSerde() {}

  public static void attributeToObjIds(
      Map<String, AttributeValue> i, String n, Consumer<ObjId> receiver) {
    AttributeValue v = i.get(n);
    if (v != null) {
      v.l().stream().map(el -> objIdFromByteBuffer(el.b().asByteBuffer())).forEach(receiver);
    }
  }

  public static String attributeToString(Map<String, AttributeValue> i, String n) {
    AttributeValue v = i.get(n);
    return v != null ? v.s() : null;
  }

  public static ByteString attributeToBytes(Map<String, AttributeValue> i, String n) {
    AttributeValue v = i.get(n);
    return v != null ? unsafeWrap(v.b().asByteArrayUnsafe()) : null;
  }

  public static boolean attributeToBool(Map<String, AttributeValue> i, String n) {
    AttributeValue v = i.get(n);
    if (v == null) {
      return false;
    }
    Boolean b = v.bool();
    return b != null && b;
  }

  public static ObjId attributeToObjId(Map<String, AttributeValue> i, String n) {
    return attributeToObjId(i.get(n));
  }

  public static ObjId attributeToObjId(AttributeValue v) {
    return v == null ? null : objIdFromByteBuffer(v.b().asByteBuffer());
  }

  public static void objIdToAttribute(Map<String, AttributeValue> i, String n, ObjId id) {
    i.put(n, id != null ? fromB(fromByteBuffer(id.asByteBuffer())) : null);
  }

  public static void objIdsAttribute(Map<String, AttributeValue> i, String n, List<ObjId> l) {
    if (l == null || l.isEmpty()) {
      return;
    }
    i.put(
        n,
        fromL(
            l.stream()
                .map(ObjId::asByteBuffer)
                .map(SdkBytes::fromByteBuffer)
                .map(AttributeValue::fromB)
                .collect(Collectors.toList())));
  }

  public static void bytesAttribute(Map<String, AttributeValue> i, String n, ByteString b) {
    i.put(n, fromB(fromByteBuffer(b.asReadOnlyByteBuffer())));
  }

  public static void fromStripesAttrList(AttributeValue attrList, Consumer<IndexStripe> consumer) {
    if (attrList != null) {
      for (AttributeValue seg : attrList.l()) {
        Map<String, AttributeValue> m = seg.m();
        consumer.accept(
            indexStripe(
                keyFromString(DynamoDBSerde.attributeToString(m, COL_STRIPES_FIRST_KEY)),
                keyFromString(DynamoDBSerde.attributeToString(m, COL_STRIPES_LAST_KEY)),
                DynamoDBSerde.attributeToObjId(m, COL_STRIPES_SEGMENT)));
      }
    }
  }

  public static AttributeValue stripesAttrList(List<IndexStripe> stripes) {
    List<AttributeValue> stripeAttr = new ArrayList<>();
    for (IndexStripe stripe : stripes) {
      Map<String, AttributeValue> sv = new HashMap<>();
      sv.put(COL_STRIPES_FIRST_KEY, fromS(stripe.firstKey().rawString()));
      sv.put(COL_STRIPES_LAST_KEY, fromS(stripe.lastKey().rawString()));
      DynamoDBSerde.objIdToAttribute(sv, COL_STRIPES_SEGMENT, stripe.segment());
      stripeAttr.add(fromM(sv));
    }
    return fromL(stripeAttr);
  }
}
