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

import org.bson.Document;
import org.bson.types.Binary;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.serialize.SmileSerialization;

public class CustomObjSerializer implements ObjSerializer<Obj> {

  public static final ObjSerializer<?> INSTANCE = new CustomObjSerializer();

  private static final String COL_CUSTOM = "x";

  private static final String COL_CUSTOM_DATA = "xd";
  private static final String COL_CUSTOM_COMPRESSION = "xC";

  private CustomObjSerializer() {}

  @Override
  public String fieldName() {
    return COL_CUSTOM;
  }

  @Override
  public void objToDoc(
      Obj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize) {
    doc.put(
        COL_CUSTOM_DATA,
        new Binary(
            SmileSerialization.serializeObj(
                obj, compression -> doc.put(COL_CUSTOM_COMPRESSION, compression.valueString()))));
  }

  @Override
  public Obj docToObj(ObjId id, ObjType type, long referenced, Document doc, String versionToken) {
    byte[] data = doc.get(COL_CUSTOM_DATA, Binary.class).getData();
    return SmileSerialization.deserializeObj(
        id, versionToken, data, type, referenced, doc.getString(COL_CUSTOM_COMPRESSION));
  }
}
