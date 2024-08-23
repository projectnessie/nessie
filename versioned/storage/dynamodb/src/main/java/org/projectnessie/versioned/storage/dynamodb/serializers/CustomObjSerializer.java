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
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToBytes;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.attributeToString;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.bytesAttribute;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromS;

import java.nio.ByteBuffer;
import java.util.Map;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.serialize.SmileSerialization;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class CustomObjSerializer implements ObjSerializer<Obj> {

  public static final ObjSerializer<?> INSTANCE = new CustomObjSerializer();

  private static final String COL_CUSTOM = "x";

  private static final String COL_CUSTOM_DATA = "xd";
  private static final String COL_CUSTOM_COMPRESSION = "xC";

  private CustomObjSerializer() {}

  @Override
  public String attributeName() {
    return COL_CUSTOM;
  }

  @Override
  public void toMap(
      Obj obj,
      Map<String, AttributeValue> i,
      int incrementalIndexSize,
      int maxSerializedIndexSize) {
    bytesAttribute(
        i,
        COL_CUSTOM_DATA,
        ByteString.copyFrom(
            SmileSerialization.serializeObj(
                obj,
                compression -> i.put(COL_CUSTOM_COMPRESSION, fromS(compression.valueString())))));
  }

  @Override
  public Obj fromMap(
      ObjId id, ObjType type, long referenced, Map<String, AttributeValue> i, String versionToken) {
    ByteBuffer buffer = requireNonNull(attributeToBytes(i, COL_CUSTOM_DATA)).asReadOnlyByteBuffer();
    return SmileSerialization.deserializeObj(
        id, versionToken, buffer, type, referenced, attributeToString(i, COL_CUSTOM_COMPRESSION));
  }
}
