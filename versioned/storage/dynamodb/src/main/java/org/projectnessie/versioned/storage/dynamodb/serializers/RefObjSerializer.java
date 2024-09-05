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
package org.projectnessie.versioned.storage.dynamodb.serializers;

import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToObjId;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToString;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.objIdToAttribute;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;

import java.util.Map;
import org.projectnessie.versioned.storage.common.objtypes.RefObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class RefObjSerializer implements ObjSerializer<RefObj> {

  public static final RefObjSerializer INSTANCE = new RefObjSerializer();

  private static final String COL_REF = "e";

  private static final String COL_REF_NAME = "n";
  private static final String COL_REF_INITIAL_POINTER = "p";
  private static final String COL_REF_CREATED_AT = "c";
  private static final String COL_REF_EXTENDED_INFO = "e";

  private RefObjSerializer() {}

  @Override
  public String attributeName() {
    return COL_REF;
  }

  @Override
  public void toMap(
      RefObj obj,
      Map<String, AttributeValue> i,
      int incrementalIndexSize,
      int maxSerializedIndexSize) {
    i.put(COL_REF_NAME, fromS(obj.name()));
    i.put(COL_REF_CREATED_AT, fromS(Long.toString(obj.createdAtMicros())));
    objIdToAttribute(i, COL_REF_INITIAL_POINTER, obj.initialPointer());
    ObjId extendedInfoObj = obj.extendedInfoObj();
    if (extendedInfoObj != null) {
      objIdToAttribute(i, COL_REF_EXTENDED_INFO, extendedInfoObj);
    }
  }

  @Override
  public RefObj fromMap(
      ObjId id, ObjType type, long referenced, Map<String, AttributeValue> i, String versionToken) {
    String createdAtStr = attributeToString(i, COL_REF_CREATED_AT);
    long createdAt = createdAtStr != null ? Long.parseLong(createdAtStr) : 0L;
    return ref(
        id,
        referenced,
        attributeToString(i, COL_REF_NAME),
        attributeToObjId(i, COL_REF_INITIAL_POINTER),
        createdAt,
        attributeToObjId(i, COL_REF_EXTENDED_INFO));
  }
}
