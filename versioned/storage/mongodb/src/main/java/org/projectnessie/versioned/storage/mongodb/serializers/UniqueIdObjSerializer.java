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

import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uniqueId;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.binaryToBytes;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.bytesToBinary;

import org.bson.Document;
import org.bson.types.Binary;
import org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class UniqueIdObjSerializer implements ObjSerializer<UniqueIdObj> {

  public static final UniqueIdObjSerializer INSTANCE = new UniqueIdObjSerializer();

  private static final String COL_UNIQUE = "u";

  static final String COL_UNIQUE_SPACE = "s";
  static final String COL_UNIQUE_VALUE = "v";

  private UniqueIdObjSerializer() {}

  @Override
  public String fieldName() {
    return COL_UNIQUE;
  }

  @Override
  public void objToDoc(
      UniqueIdObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize) {
    doc.put(COL_UNIQUE_SPACE, obj.space());
    doc.put(COL_UNIQUE_VALUE, bytesToBinary(obj.value()));
  }

  @Override
  public UniqueIdObj docToObj(
      ObjId id, ObjType type, long referenced, Document doc, String versionToken) {
    return uniqueId(
        id,
        referenced,
        doc.getString(COL_UNIQUE_SPACE),
        binaryToBytes(doc.get(COL_UNIQUE_VALUE, Binary.class)));
  }
}
