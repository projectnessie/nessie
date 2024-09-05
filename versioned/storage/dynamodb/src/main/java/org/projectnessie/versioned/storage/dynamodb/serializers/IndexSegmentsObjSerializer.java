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

import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.fromStripesAttrList;
import static org.projectnessie.versioned.storage.dynamodb.DynamoDBSerde.stripesAttrList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class IndexSegmentsObjSerializer implements ObjSerializer<IndexSegmentsObj> {

  public static final IndexSegmentsObjSerializer INSTANCE = new IndexSegmentsObjSerializer();

  private static final String COL_SEGMENTS = "I";
  private static final String COL_SEGMENTS_STRIPES = "s";

  private IndexSegmentsObjSerializer() {}

  @Override
  public String attributeName() {
    return COL_SEGMENTS;
  }

  @Override
  public void toMap(
      IndexSegmentsObj obj,
      Map<String, AttributeValue> i,
      int incrementalIndexSize,
      int maxSerializedIndexSize) {
    i.put(COL_SEGMENTS_STRIPES, stripesAttrList(obj.stripes()));
  }

  @Override
  public IndexSegmentsObj fromMap(
      ObjId id, ObjType type, long referenced, Map<String, AttributeValue> i, String versionToken) {
    List<IndexStripe> stripes = new ArrayList<>();
    fromStripesAttrList(i.get(COL_SEGMENTS_STRIPES), stripes::add);
    return indexSegments(id, referenced, stripes);
  }
}
