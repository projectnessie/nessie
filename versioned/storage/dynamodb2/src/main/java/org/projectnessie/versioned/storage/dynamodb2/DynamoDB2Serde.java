/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.dynamodb2;

import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteBuffer;
import static software.amazon.awssdk.core.SdkBytes.fromByteBuffer;
import static software.amazon.awssdk.services.dynamodb.model.AttributeValue.fromB;

import java.util.Map;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class DynamoDB2Serde {

  private DynamoDB2Serde() {}

  public static String attributeToString(Map<String, AttributeValue> i, String n) {
    AttributeValue v = i.get(n);
    return v != null ? v.s() : null;
  }

  public static boolean attributeToBool(Map<String, AttributeValue> i, String n) {
    AttributeValue v = i.get(n);
    if (v == null) {
      return false;
    }
    Boolean b = v.bool();
    return b != null && b;
  }

  public static ObjId attributeToObjId(Map<String, AttributeValue> i, String n) {
    return attributeToObjId(i.get(n));
  }

  public static ObjId attributeToObjId(AttributeValue v) {
    return v == null ? null : objIdFromByteBuffer(v.b().asByteBuffer());
  }

  public static void objIdToAttribute(Map<String, AttributeValue> i, String n, ObjId id) {
    i.put(n, id != null ? fromB(fromByteBuffer(id.asByteBuffer())) : null);
  }
}
