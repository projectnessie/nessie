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
package org.projectnessie.versioned.storage.mongodb.serializers;

import static org.projectnessie.versioned.storage.common.objtypes.IndexObj.index;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.binaryToBytes;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.bytesToBinary;

import org.bson.Document;
import org.bson.types.Binary;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class IndexObjSerializer implements ObjSerializer<IndexObj> {

  public static final IndexObjSerializer INSTANCE = new IndexObjSerializer();

  private static final String COL_INDEX = "i";
  private static final String COL_INDEX_INDEX = "i";

  private IndexObjSerializer() {}

  @Override
  public String fieldName() {
    return COL_INDEX;
  }

  @Override
  public void objToDoc(
      IndexObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize)
      throws ObjTooLargeException {
    ByteString index = obj.index();
    if (index.size() > maxSerializedIndexSize) {
      throw new ObjTooLargeException(index.size(), maxSerializedIndexSize);
    }
    doc.put(COL_INDEX_INDEX, bytesToBinary(index));
  }

  @Override
  public IndexObj docToObj(
      ObjId id, ObjType type, long referenced, Document doc, String versionToken) {
    return index(id, referenced, binaryToBytes(doc.get(COL_INDEX_INDEX, Binary.class)));
  }
}
