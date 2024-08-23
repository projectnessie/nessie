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

import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.TagObj.tag;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToBytes;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToString;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.bytesAttribute;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromL;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromM;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.TagObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class TagObjSerializer implements ObjSerializer<TagObj> {

  public static final TagObjSerializer INSTANCE = new TagObjSerializer();

  private static final String COL_TAG = "t";

  private static final String COL_TAG_MESSAGE = "m";
  private static final String COL_TAG_HEADERS = "h";
  private static final String COL_TAG_SIGNATURE = "s";

  private TagObjSerializer() {}

  @Override
  public String attributeName() {
    return COL_TAG;
  }

  @Override
  public void toMap(
      TagObj obj,
      Map<String, AttributeValue> i,
      int incrementalIndexSize,
      int maxSerializedIndexSize) {
    String message = obj.message();
    if (message != null) {
      i.put(COL_TAG_MESSAGE, fromS(message));
    }

    Map<String, AttributeValue> headerMap = new HashMap<>();
    CommitHeaders headers = obj.headers();
    if (headers != null) {
      for (String s : headers.keySet()) {
        headerMap.put(
            s,
            fromL(
                headers.getAll(s).stream()
                    .map(AttributeValue::fromS)
                    .collect(Collectors.toList())));
      }
      if (!headerMap.isEmpty()) {
        i.put(COL_TAG_HEADERS, fromM(headerMap));
      }
    }

    ByteString signature = obj.signature();
    if (signature != null) {
      bytesAttribute(i, COL_TAG_SIGNATURE, signature);
    }
  }

  @Override
  public TagObj fromMap(
      ObjId id, ObjType type, long referenced, Map<String, AttributeValue> i, String versionToken) {
    CommitHeaders tagHeaders = null;
    AttributeValue headerMap = i.get(COL_TAG_HEADERS);
    if (headerMap != null) {
      CommitHeaders.Builder headers = newCommitHeaders();
      headerMap.m().forEach((k, l) -> l.l().forEach(hv -> headers.add(k, hv.s())));
      tagHeaders = headers.build();
    }

    return tag(
        id,
        referenced,
        attributeToString(i, COL_TAG_MESSAGE),
        tagHeaders,
        attributeToBytes(i, COL_TAG_SIGNATURE));
  }
}
