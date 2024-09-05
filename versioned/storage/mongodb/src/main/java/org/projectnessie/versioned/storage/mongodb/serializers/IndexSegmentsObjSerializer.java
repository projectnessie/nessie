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

import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.fromStripesDocList;
import static org.projectnessie.versioned.storage.mongodb.MongoDBSerde.stripesToDocs;

import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public class IndexSegmentsObjSerializer implements ObjSerializer<IndexSegmentsObj> {

  public static final IndexSegmentsObjSerializer INSTANCE = new IndexSegmentsObjSerializer();

  private static final String COL_SEGMENTS = "I";
  private static final String COL_SEGMENTS_STRIPES = "s";

  private IndexSegmentsObjSerializer() {}

  @Override
  public String fieldName() {
    return COL_SEGMENTS;
  }

  @Override
  public void objToDoc(
      IndexSegmentsObj obj, Document doc, int incrementalIndexLimit, int maxSerializedIndexSize) {
    doc.put(COL_SEGMENTS_STRIPES, stripesToDocs(obj.stripes()));
  }

  @Override
  public IndexSegmentsObj docToObj(
      ObjId id, ObjType type, long referenced, Document doc, String versionToken) {
    List<IndexStripe> stripes = new ArrayList<>();
    fromStripesDocList(doc, COL_SEGMENTS_STRIPES, stripes::add);
    return indexSegments(id, referenced, stripes);
  }
}
