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
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToBool;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToBytes;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToObjId;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToObjIds;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToString;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.bytesAttribute;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.fromStripesAttrList;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.objIdToAttribute;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.objIdsAttribute;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.stripesAttrList;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromBool;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

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
  public String attributeName() {
    return COL_COMMIT;
  }

  @Override
  public void toMap(
      CommitObj obj,
      Map<String, AttributeValue> i,
      int incrementalIndexSize,
      int maxSerializedIndexSize)
      throws ObjTooLargeException {
    i.put(COL_COMMIT_SEQ, fromS(Long.toString(obj.seq())));
    i.put(COL_COMMIT_CREATED, fromS(Long.toString(obj.created())));
    ObjId referenceIndex = obj.referenceIndex();
    if (referenceIndex != null) {
      objIdToAttribute(i, COL_COMMIT_REFERENCE_INDEX, referenceIndex);
    }
    i.put(COL_COMMIT_MESSAGE, fromS(obj.message()));
    objIdsAttribute(i, COL_COMMIT_TAIL, obj.tail());
    objIdsAttribute(i, COL_COMMIT_SECONDARY_PARENTS, obj.secondaryParents());

    ByteString index = obj.incrementalIndex();
    if (index.size() > incrementalIndexSize) {
      throw new ObjTooLargeException(index.size(), incrementalIndexSize);
    }
    bytesAttribute(i, COL_COMMIT_INCREMENTAL_INDEX, index);

    if (!obj.referenceIndexStripes().isEmpty()) {
      i.put(COL_COMMIT_REFERENCE_INDEX_STRIPES, stripesAttrList(obj.referenceIndexStripes()));
    }

    Map<String, AttributeValue> headerMap = new HashMap<>();
    CommitHeaders headers = obj.headers();
    for (String s : headers.keySet()) {
      headerMap.put(
          s,
          fromL(
              headers.getAll(s).stream().map(AttributeValue::fromS).collect(Collectors.toList())));
    }
    if (!headerMap.isEmpty()) {
      i.put(COL_COMMIT_HEADERS, fromM(headerMap));
    }
    i.put(COL_COMMIT_INCOMPLETE_INDEX, fromBool(obj.incompleteIndex()));
    i.put(COL_COMMIT_TYPE, fromS(obj.commitType().shortName()));
  }

  @Override
  public CommitObj fromMap(
      ObjId id, ObjType type, long referenced, Map<String, AttributeValue> i, String versionToken) {
    CommitObj.Builder b =
        commitBuilder()
            .id(id)
            .referenced(referenced)
            .seq(Long.parseLong(requireNonNull(attributeToString(i, COL_COMMIT_SEQ))))
            .created(Long.parseLong(requireNonNull(attributeToString(i, COL_COMMIT_CREATED))))
            .message(attributeToString(i, COL_COMMIT_MESSAGE))
            .incrementalIndex(attributeToBytes(i, COL_COMMIT_INCREMENTAL_INDEX))
            .incompleteIndex(attributeToBool(i, COL_COMMIT_INCOMPLETE_INDEX))
            .commitType(CommitType.fromShortName(attributeToString(i, COL_COMMIT_TYPE)));
    AttributeValue v = i.get(COL_COMMIT_REFERENCE_INDEX);
    if (v != null) {
      b.referenceIndex(attributeToObjId(v));
    }

    fromStripesAttrList(i.get(COL_COMMIT_REFERENCE_INDEX_STRIPES), b::addReferenceIndexStripes);

    attributeToObjIds(i, COL_COMMIT_TAIL, b::addTail);
    attributeToObjIds(i, COL_COMMIT_SECONDARY_PARENTS, b::addSecondaryParents);

    CommitHeaders.Builder headers = newCommitHeaders();
    AttributeValue headerMap = i.get(COL_COMMIT_HEADERS);
    if (headerMap != null) {
      headerMap.m().forEach((k, l) -> l.l().forEach(hv -> headers.add(k, hv.s())));
    }
    b.headers(headers.build());

    return b.build();
  }
}
