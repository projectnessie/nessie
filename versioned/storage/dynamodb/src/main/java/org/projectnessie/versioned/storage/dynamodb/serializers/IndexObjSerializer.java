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
import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToBytes;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.bytesAttribute;

import java.util.Map;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class IndexObjSerializer implements ObjSerializer<IndexObj> {

  public static final IndexObjSerializer INSTANCE = new IndexObjSerializer();

  private static final String COL_INDEX = "i";
  private static final String COL_INDEX_INDEX = "i";

  private IndexObjSerializer() {}

  @Override
  public String attributeName() {
    return COL_INDEX;
  }

  @Override
  public void toMap(
      IndexObj obj,
      Map<String, AttributeValue> i,
      int incrementalIndexSize,
      int maxSerializedIndexSize)
      throws ObjTooLargeException {
    ByteString index = obj.index();
    if (index.size() > maxSerializedIndexSize) {
      throw new ObjTooLargeException(index.size(), maxSerializedIndexSize);
    }
    bytesAttribute(i, COL_INDEX_INDEX, index);
  }

  @Override
  public IndexObj fromMap(
      ObjId id, ObjType type, long referenced, Map<String, AttributeValue> i, String versionToken) {
    return index(id, referenced, requireNonNull(attributeToBytes(i, COL_INDEX_INDEX)));
  }
}
