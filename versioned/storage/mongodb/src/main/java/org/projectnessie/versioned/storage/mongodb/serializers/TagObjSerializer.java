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

import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.binaryToBytes;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.bytesToBinary;

import java.util.List;
import org.bson.Document;
import org.bson.types.Binary;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.TagObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class TagObjSerializer implements ObjSerializer<TagObj> {

  public static final TagObjSerializer INSTANCE = new TagObjSerializer();

  private static final String COL_TAG = "t";

  private static final String COL_TAG_MESSAGE = "m";
  private static final String COL_TAG_HEADERS = "h";
  private static final String COL_TAG_SIGNATURE = "s";

  private TagObjSerializer() {}

  @Override
  public String fieldName() {
    return COL_TAG;
  }

  @Override
  public void objToDoc(
      TagObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize) {
    String message = obj.message();
    if (message != null) {
      doc.put(COL_TAG_MESSAGE, message);
    }

    Document headerDoc = new Document();
    CommitHeaders headers = obj.headers();
    if (headers != null) {
      for (String s : headers.keySet()) {
        headerDoc.put(s, headers.getAll(s));
      }
      if (!headerDoc.isEmpty()) {
        doc.put(COL_TAG_HEADERS, headerDoc);
      }
    }

    ByteString signature = obj.signature();
    if (signature != null) {
      doc.put(COL_TAG_SIGNATURE, bytesToBinary(signature));
    }
  }

  @Override
  public TagObj docToObj(
      ObjId id, ObjType type, long referenced, Document doc, String versionToken) {
    CommitHeaders tagHeaders = null;
    Document headerDoc = doc.get(COL_TAG_HEADERS, Document.class);
    if (headerDoc != null) {
      CommitHeaders.Builder headers = newCommitHeaders();
      headerDoc.forEach(
          (k, o) -> {
            @SuppressWarnings({"unchecked", "rawtypes"})
            List<String> l = (List) o;
            l.forEach(hv -> headers.add(k, hv));
          });
      tagHeaders = headers.build();
    }

    return tag(
        id,
        referenced,
        doc.getString(COL_TAG_MESSAGE),
        tagHeaders,
        binaryToBytes(doc.get(COL_TAG_SIGNATURE, Binary.class)));
  }
}
