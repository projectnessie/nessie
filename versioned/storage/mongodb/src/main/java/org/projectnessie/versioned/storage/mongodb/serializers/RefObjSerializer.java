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

import static org.projectnessie.versioned.storage.common.objtypes.RefObj.ref;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.binaryToObjId;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.objIdToBinary;

import org.bson.Document;
import org.bson.types.Binary;
import org.projectnessie.versioned.storage.common.objtypes.RefObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class RefObjSerializer implements ObjSerializer<RefObj> {

  public static final RefObjSerializer INSTANCE = new RefObjSerializer();

  private static final String COL_REF = "e";

  private static final String COL_REF_NAME = "n";
  private static final String COL_REF_INITIAL_POINTER = "p";
  private static final String COL_REF_CREATED_AT = "c";
  private static final String COL_REF_EXTENDED_INFO = "e";

  private RefObjSerializer() {}

  @Override
  public String fieldName() {
    return COL_REF;
  }

  @Override
  public void objToDoc(
      RefObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize) {
    doc.put(COL_REF_NAME, obj.name());
    doc.put(COL_REF_CREATED_AT, obj.createdAtMicros());
    doc.put(COL_REF_INITIAL_POINTER, objIdToBinary(obj.initialPointer()));
    ObjId extendedInfoObj = obj.extendedInfoObj();
    if (extendedInfoObj != null) {
      doc.put(COL_REF_EXTENDED_INFO, objIdToBinary(extendedInfoObj));
    }
  }

  @Override
  public RefObj docToObj(
      ObjId id, ObjType type, long referenced, Document doc, String versionToken) {
    return ref(
        id,
        referenced,
        doc.getString(COL_REF_NAME),
        binaryToObjId(doc.get(COL_REF_INITIAL_POINTER, Binary.class)),
        doc.getLong(COL_REF_CREATED_AT),
        binaryToObjId(doc.get(COL_REF_EXTENDED_INFO, Binary.class)));
  }
}
