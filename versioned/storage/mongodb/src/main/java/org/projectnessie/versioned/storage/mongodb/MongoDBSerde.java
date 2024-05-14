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
package org.projectnessie.versioned.storage.mongodb;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.bson.Document;
import org.bson.types.Binary;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.persist.ObjId;

public final class MongoDBSerde {

  private static final String COL_STRIPES_FIRST_KEY = "f";
  private static final String COL_STRIPES_LAST_KEY = "l";
  private static final String COL_STRIPES_SEGMENT = "s";

  private MongoDBSerde() {}

  public static Binary bytesToBinary(ByteString bytes) {
    return new Binary(bytes.toByteArray());
  }

  public static ByteString binaryToBytes(Binary binary) {
    return binary != null ? unsafeWrap(binary.getData()) : null;
  }

  public static Binary objIdToBinary(ObjId id) {
    return new Binary(id.asByteArray());
  }

  public static ObjId binaryToObjId(Binary id) {
    return id != null ? ObjId.objIdFromByteArray(id.getData()) : null;
  }

  public static void objIdsToDoc(Document doc, String n, List<ObjId> ids) {
    if (ids == null || ids.isEmpty()) {
      return;
    }
    doc.put(n, objIdsToBinary(ids));
  }

  public static List<Binary> objIdsToBinary(List<ObjId> ids) {
    if (ids == null) {
      return emptyList();
    }
    return ids.stream().map(MongoDBSerde::objIdToBinary).collect(toList());
  }

  public static List<ObjId> binaryToObjIds(List<Binary> ids) {
    if (ids == null) {
      return emptyList();
    }
    return ids.stream().map(MongoDBSerde::binaryToObjId).collect(toList());
  }

  public static void binaryToObjIds(List<Binary> ids, Consumer<ObjId> receiver) {
    if (ids != null) {
      ids.stream().map(MongoDBSerde::binaryToObjId).forEach(receiver);
    }
  }

  public static void fromStripesDocList(
      Document doc, String attrName, Consumer<IndexStripe> consumer) {
    List<Document> refIndexStripes = doc.getList(attrName, Document.class);
    if (refIndexStripes != null) {
      for (Document seg : refIndexStripes) {
        consumer.accept(
            indexStripe(
                keyFromString(seg.getString(COL_STRIPES_FIRST_KEY)),
                keyFromString(seg.getString(COL_STRIPES_LAST_KEY)),
                binaryToObjId(seg.get(COL_STRIPES_SEGMENT, Binary.class))));
      }
    }
  }

  @Nonnull
  public static List<Document> stripesToDocs(List<IndexStripe> stripes) {
    List<Document> stripesDocs = new ArrayList<>();
    for (IndexStripe stripe : stripes) {
      Document sv = new Document();
      sv.put(COL_STRIPES_FIRST_KEY, stripe.firstKey().rawString());
      sv.put(COL_STRIPES_LAST_KEY, stripe.lastKey().rawString());
      sv.put(COL_STRIPES_SEGMENT, objIdToBinary(stripe.segment()));
      stripesDocs.add(sv);
    }
    return stripesDocs;
  }
}
