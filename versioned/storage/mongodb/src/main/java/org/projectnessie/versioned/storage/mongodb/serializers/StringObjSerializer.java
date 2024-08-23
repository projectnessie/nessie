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

import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.binaryToBytes;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.binaryToObjIds;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.bytesToBinary;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.objIdsToDoc;

import org.bson.Document;
import org.bson.types.Binary;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

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
  public String fieldName() {
    return COL_STRING;
  }

  @Override
  public void objToDoc(
      StringObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize) {
    String s = obj.contentType();
    if (s != null && !s.isEmpty()) {
      doc.put(COL_STRING_CONTENT_TYPE, s);
    }
    doc.put(COL_STRING_COMPRESSION, obj.compression().name());
    s = obj.filename();
    if (s != null && !s.isEmpty()) {
      doc.put(COL_STRING_FILENAME, s);
    }
    objIdsToDoc(doc, COL_STRING_PREDECESSORS, obj.predecessors());
    doc.put(COL_STRING_TEXT, bytesToBinary(obj.text()));
  }

  @Override
  public StringObj docToObj(
      ObjId id, ObjType type, long referenced, Document doc, String versionToken) {
    return stringData(
        id,
        referenced,
        doc.getString(COL_STRING_CONTENT_TYPE),
        Compression.valueOf(doc.getString(COL_STRING_COMPRESSION)),
        doc.getString(COL_STRING_FILENAME),
        binaryToObjIds(doc.getList(COL_STRING_PREDECESSORS, Binary.class)),
        binaryToBytes(doc.get(COL_STRING_TEXT, Binary.class)));
  }
}
