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

import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToBytes;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToObjIds;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToString;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.bytesAttribute;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.objIdsAttribute;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class StringObjSerializer implements ObjSerializer<StringObj> {

  public static final StringObjSerializer INSTANCE = new StringObjSerializer();

  private static final String COL_STRING = "s";

  private static final String COL_STRING_CONTENT_TYPE = "y";
  private static final String COL_STRING_COMPRESSION = "c";
  private static final String COL_STRING_FILENAME = "f";
  private static final String COL_STRING_PREDECESSORS = "p";
  private static final String COL_STRING_TEXT = "t";

  private StringObjSerializer() {}

  @Override
  public String attributeName() {
    return COL_STRING;
  }

  @Override
  public void toMap(
      StringObj obj,
      Map<String, AttributeValue> i,
      int incrementalIndexSize,
      int maxSerializedIndexSize) {
    String s = obj.contentType();
    if (s != null && !s.isEmpty()) {
      i.put(COL_STRING_CONTENT_TYPE, fromS(s));
    }
    i.put(COL_STRING_COMPRESSION, fromS(obj.compression().name()));
    s = obj.filename();
    if (s != null && !s.isEmpty()) {
      i.put(COL_STRING_FILENAME, fromS(s));
    }
    objIdsAttribute(i, COL_STRING_PREDECESSORS, obj.predecessors());
    bytesAttribute(i, COL_STRING_TEXT, obj.text());
  }

  @Override
  public StringObj fromMap(
      ObjId id, ObjType type, long referenced, Map<String, AttributeValue> i, String versionToken) {
    List<ObjId> predecessors = new ArrayList<>();
    attributeToObjIds(i, COL_STRING_PREDECESSORS, predecessors::add);
    return stringData(
        id,
        referenced,
        attributeToString(i, COL_STRING_CONTENT_TYPE),
        Compression.valueOf(attributeToString(i, COL_STRING_COMPRESSION)),
        attributeToString(i, COL_STRING_FILENAME),
        predecessors,
        attributeToBytes(i, COL_STRING_TEXT));
  }
}
