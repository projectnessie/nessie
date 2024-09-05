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
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.binaryToBytes;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.binaryToObjId;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.binaryToObjIds;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.bytesToBinary;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.fromStripesDocList;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.objIdToBinary;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.objIdsToDoc;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.stripesToDocs;

import java.util.List;
import org.bson.Document;
import org.bson.types.Binary;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class CommitObjSerializer implements ObjSerializer<CommitObj> {

  public static final CommitObjSerializer INSTANCE = new CommitObjSerializer();

  private static final String COL_COMMIT = "c";

  private static final String COL_COMMIT_CREATED = "c";
  private static final String COL_COMMIT_SEQ = "q";
  private static final String COL_COMMIT_MESSAGE = "m";
  private static final String COL_COMMIT_HEADERS = "h";
  private static final String COL_COMMIT_REFERENCE_INDEX = "x";
  private static final String COL_COMMIT_REFERENCE_INDEX_STRIPES = "r";
  private static final String COL_COMMIT_TAIL = "t";
  private static final String COL_COMMIT_SECONDARY_PARENTS = "s";
  private static final String COL_COMMIT_INCREMENTAL_INDEX = "i";
  private static final String COL_COMMIT_INCOMPLETE_INDEX = "n";
  private static final String COL_COMMIT_TYPE = "y";

  private CommitObjSerializer() {}

  @Override
  public String fieldName() {
    return COL_COMMIT;
  }

  @Override
  public void objToDoc(
      CommitObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize)
      throws ObjTooLargeException {
    doc.put(COL_COMMIT_SEQ, obj.seq());
    doc.put(COL_COMMIT_CREATED, obj.created());
    ObjId referenceIndex = obj.referenceIndex();
    if (referenceIndex != null) {
      doc.put(COL_COMMIT_REFERENCE_INDEX, objIdToBinary(referenceIndex));
    }
    doc.put(COL_COMMIT_MESSAGE, obj.message());
    objIdsToDoc(doc, COL_COMMIT_TAIL, obj.tail());
    objIdsToDoc(doc, COL_COMMIT_SECONDARY_PARENTS, obj.secondaryParents());

    ByteString index = obj.incrementalIndex();
    if (index.size() > incrementalIndexLimit) {
      throw new ObjTooLargeException(index.size(), incrementalIndexLimit);
    }
    doc.put(COL_COMMIT_INCREMENTAL_INDEX, bytesToBinary(index));

    List<IndexStripe> indexStripes = obj.referenceIndexStripes();
    if (!indexStripes.isEmpty()) {
      doc.put(COL_COMMIT_REFERENCE_INDEX_STRIPES, stripesToDocs(indexStripes));
    }

    Document headerDoc = new Document();
    CommitHeaders headers = obj.headers();
    for (String s : headers.keySet()) {
      headerDoc.put(s, headers.getAll(s));
    }
    if (!headerDoc.isEmpty()) {
      doc.put(COL_COMMIT_HEADERS, headerDoc);
    }

    doc.put(COL_COMMIT_INCOMPLETE_INDEX, obj.incompleteIndex());
    doc.put(COL_COMMIT_TYPE, obj.commitType().shortName());
  }

  @Override
  public CommitObj docToObj(
      ObjId id, ObjType type, long referenced, Document doc, String versionToken) {
    CommitObj.Builder b =
        commitBuilder()
            .id(id)
            .referenced(referenced)
            .seq(doc.getLong(COL_COMMIT_SEQ))
            .created(doc.getLong(COL_COMMIT_CREATED))
            .message(doc.getString(COL_COMMIT_MESSAGE))
            .incrementalIndex(binaryToBytes(doc.get(COL_COMMIT_INCREMENTAL_INDEX, Binary.class)))
            .incompleteIndex(doc.getBoolean(COL_COMMIT_INCOMPLETE_INDEX))
            .commitType(CommitType.fromShortName(doc.getString(COL_COMMIT_TYPE)));
    Binary v = doc.get(COL_COMMIT_REFERENCE_INDEX, Binary.class);
    if (v != null) {
      b.referenceIndex(binaryToObjId(v));
    }

    fromStripesDocList(doc, COL_COMMIT_REFERENCE_INDEX_STRIPES, b::addReferenceIndexStripes);

    binaryToObjIds(doc.getList(COL_COMMIT_TAIL, Binary.class), b::addTail);
    binaryToObjIds(doc.getList(COL_COMMIT_SECONDARY_PARENTS, Binary.class), b::addSecondaryParents);

    CommitHeaders.Builder headers = newCommitHeaders();
    Document headerDoc = doc.get(COL_COMMIT_HEADERS, Document.class);
    if (headerDoc != null) {
      headerDoc.forEach(
          (k, o) -> {
            @SuppressWarnings({"unchecked", "rawtypes"})
            List<String> l = (List) o;
            l.forEach(hv -> headers.add(k, hv));
          });
    }
    b.headers(headers.build());

    return b.build();
  }
}
