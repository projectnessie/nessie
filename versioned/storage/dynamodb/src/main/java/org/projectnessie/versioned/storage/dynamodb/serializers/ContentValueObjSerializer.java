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

import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToBytes;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToString;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.bytesAttribute;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;

import java.util.Map;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class ContentValueObjSerializer implements ObjSerializer<ContentValueObj> {

  public static final ContentValueObjSerializer INSTANCE = new ContentValueObjSerializer();

  private static final String COL_VALUE = "v";

  private static final String COL_VALUE_CONTENT_ID = "i";
  private static final String COL_VALUE_PAYLOAD = "p";
  private static final String COL_VALUE_DATA = "d";

  private ContentValueObjSerializer() {}

  @Override
  public String attributeName() {
    return COL_VALUE;
  }

  @Override
  public void toMap(
      ContentValueObj obj,
      Map<String, AttributeValue> i,
      int incrementalIndexSize,
      int maxSerializedIndexSize) {
    i.put(COL_VALUE_CONTENT_ID, fromS(obj.contentId()));
    i.put(COL_VALUE_PAYLOAD, fromS(Integer.toString(obj.payload())));
    bytesAttribute(i, COL_VALUE_DATA, obj.data());
  }

  @Override
  public ContentValueObj fromMap(
      ObjId id, ObjType type, long referenced, Map<String, AttributeValue> i, String versionToken) {
    return contentValue(
        id,
        referenced,
        attributeToString(i, COL_VALUE_CONTENT_ID),
        Integer.parseInt(requireNonNull(attributeToString(i, COL_VALUE_PAYLOAD))),
        attributeToBytes(i, COL_VALUE_DATA));
  }
}
