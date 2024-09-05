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

import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uniqueId;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToBytes;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToString;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.bytesAttribute;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;

import java.util.Map;
import org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class UniqueIdObjSerializer implements ObjSerializer<UniqueIdObj> {

  public static final UniqueIdObjSerializer INSTANCE = new UniqueIdObjSerializer();

  private static final String COL_UNIQUE = "u";

  static final String COL_UNIQUE_SPACE = "s";
  static final String COL_UNIQUE_VALUE = "v";

  private UniqueIdObjSerializer() {}

  @Override
  public String attributeName() {
    return COL_UNIQUE;
  }

  @Override
  public void toMap(
      UniqueIdObj obj,
      Map<String, AttributeValue> i,
      int incrementalIndexSize,
      int maxSerializedIndexSize) {
    i.put(COL_UNIQUE_SPACE, fromS(obj.space()));
    bytesAttribute(i, COL_UNIQUE_VALUE, obj.value());
  }

  @Override
  public UniqueIdObj fromMap(
      ObjId id, ObjType type, long referenced, Map<String, AttributeValue> i, String versionToken) {
    return uniqueId(
        id,
        referenced,
        attributeToString(i, COL_UNIQUE_SPACE),
        attributeToBytes(i, COL_UNIQUE_VALUE));
  }
}
