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
package org.projectnessie.versioned.storage.cassandra.serializers;

import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_PREFIX;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.INSERT_OBJ_VALUES;
import static org.projectnessie.versioned.storage.cassandra.CassandraConstants.STORE_OBJ_SUFFIX;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj.indexSegments;
import static org.projectnessie.versioned.storage.common.objtypes.IndexStripe.indexStripe;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteBuffer;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.projectnessie.versioned.storage.cassandra.CqlColumn;
import org.projectnessie.versioned.storage.cassandra.CqlColumnType;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Stripe;
import org.projectnessie.versioned.storage.common.proto.StorageTypes.Stripes;

public class IndexSegmentsObjSerializer implements ObjSerializer<IndexSegmentsObj> {

  public static final ObjSerializer<IndexSegmentsObj> INSTANCE = new IndexSegmentsObjSerializer();

  private static final CqlColumn COL_SEGMENTS_STRIPES =
      new CqlColumn("i_stripes", CqlColumnType.VARBINARY);

  private static final Set<CqlColumn> COLS = ImmutableSet.of(COL_SEGMENTS_STRIPES);

  private static final String INSERT_CQL =
      INSERT_OBJ_PREFIX
          + COLS.stream().map(CqlColumn::name).collect(Collectors.joining(","))
          + INSERT_OBJ_VALUES
          + COLS.stream().map(c -> ":" + c.name()).collect(Collectors.joining(","))
          + ")";

  private static final String STORE_CQL = INSERT_CQL + STORE_OBJ_SUFFIX;

  private IndexSegmentsObjSerializer() {}

  @Override
  public Set<CqlColumn> columns() {
    return COLS;
  }

  @Override
  public String insertCql(boolean upsert) {
    return upsert ? INSERT_CQL : STORE_CQL;
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Override
  public void serialize(
      IndexSegmentsObj obj,
      BoundStatementBuilder stmt,
      int incrementalIndexLimit,
      int maxSerializedIndexSize)
      throws ObjTooLargeException {
    Stripes.Builder b = Stripes.newBuilder();
    obj.stripes().stream()
        .map(
            s ->
                Stripe.newBuilder()
                    .setFirstKey(s.firstKey().rawString())
                    .setLastKey(s.lastKey().rawString())
                    .setSegment(s.segment().asBytes()))
        .forEach(b::addStripes);
    stmt.setByteBuffer(
        COL_SEGMENTS_STRIPES.name(), b.build().toByteString().asReadOnlyByteBuffer());
  }

  @Override
  public IndexSegmentsObj deserialize(Row row, ObjId id) {
    try {
      Stripes stripes = Stripes.parseFrom(row.getByteBuffer(COL_SEGMENTS_STRIPES.name()));
      List<IndexStripe> stripeList =
          stripes.getStripesList().stream()
              .map(
                  s ->
                      indexStripe(
                          keyFromString(s.getFirstKey()),
                          keyFromString(s.getLastKey()),
                          objIdFromByteBuffer(s.getSegment().asReadOnlyByteBuffer())))
              .collect(Collectors.toList());
      return indexSegments(id, stripeList);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
