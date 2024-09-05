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
package org.projectnessie.versioned.storage.jdbc.serializers;

import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteBuffer;
import static org.projectnessie.versioned.storage.jdbc.JdbcSerde.serializeBytes;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Stripe;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Stripes;
import org.projectnessie.versioned.storage.jdbc.DatabaseSpecific;
import org.projectnessie.versioned.storage.jdbc.JdbcColumnType;

public class IndexSegmentsObjSerializer implements ObjSerializer<IndexSegmentsObj> {

  public static final ObjSerializer<IndexSegmentsObj> INSTANCE = new IndexSegmentsObjSerializer();

  private static final String COL_SEGMENTS_STRIPES = "i_stripes";

  private static final Map<String, JdbcColumnType> COLS =
      ImmutableMap.of(COL_SEGMENTS_STRIPES, JdbcColumnType.VARBINARY);

  private IndexSegmentsObjSerializer() {}

  @Override
  public Map<String, JdbcColumnType> columns() {
    return COLS;
  }

  @Override
  public void serialize(
      PreparedStatement ps,
      IndexSegmentsObj obj,
      int incrementalIndexLimit,
      int maxSerializedIndexSize,
      Function<String, Integer> nameToIdx,
      DatabaseSpecific databaseSpecific)
      throws SQLException {
    Stripes.Builder b = Stripes.newBuilder();
    obj.stripes().stream()
        .map(
            s ->
                Stripe.newBuilder()
                    .setFirstKey(s.firstKey().rawString())
                    .setLastKey(s.lastKey().rawString())
                    .setSegment(s.segment().asBytes()))
        .forEach(b::addStripes);
    serializeBytes(
        ps, nameToIdx.apply(COL_SEGMENTS_STRIPES), b.build().toByteString(), databaseSpecific);
  }

  @Override
  public IndexSegmentsObj deserialize(
      ResultSet rs, ObjType type, ObjId id, long referenced, String versionToken)
      throws SQLException {
    try {
      Stripes stripes = Stripes.parseFrom(rs.getBytes(COL_SEGMENTS_STRIPES));
      List<IndexStripe> stripeList =
          stripes.getStripesList().stream()
              .map(
                  s ->
                      indexStripe(
                          keyFromString(s.getFirstKey()),
                          keyFromString(s.getLastKey()),
                          objIdFromByteBuffer(s.getSegment().asReadOnlyByteBuffer())))
              .collect(Collectors.toList());
      return indexSegments(id, referenced, stripeList);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
