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

import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.binaryToBytes;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.bytesToBinary;

import org.bson.Document;
import org.bson.types.Binary;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class ContentValueObjSerializer implements ObjSerializer<ContentValueObj> {

  public static final ContentValueObjSerializer INSTANCE = new ContentValueObjSerializer();

  private static final String COL_VALUE = "v";

  private static final String COL_VALUE_CONTENT_ID = "i";
  private static final String COL_VALUE_PAYLOAD = "p";
  private static final String COL_VALUE_DATA = "d";

  private ContentValueObjSerializer() {}

  @Override
  public String fieldName() {
    return COL_VALUE;
  }

  @Override
  public void objToDoc(
      ContentValueObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize) {
    doc.put(COL_VALUE_CONTENT_ID, obj.contentId());
    doc.put(COL_VALUE_PAYLOAD, obj.payload());
    doc.put(COL_VALUE_DATA, bytesToBinary(obj.data()));
  }

  @Override
  public ContentValueObj docToObj(
      ObjId id, ObjType type, long referenced, Document doc, String versionToken) {
    return contentValue(
        id,
        referenced,
        doc.getString(COL_VALUE_CONTENT_ID),
        doc.getInteger(COL_VALUE_PAYLOAD),
        binaryToBytes(doc.get(COL_VALUE_DATA, Binary.class)));
  }
}
