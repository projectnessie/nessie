/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.gc.iceberg;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.spark.procedures.BaseGcProcedure;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.projectnessie.gc.base.GCImpl;
import org.projectnessie.gc.base.GCUtil;
import org.projectnessie.gc.base.ImmutableGCParams;

/**
 * Nessie GC procedure to identify the expired snapshots per content id per reference via the {@link
 * org.projectnessie.gc.base.GCImpl#identifyExpiredContents(SparkSession)} functionality.
 *
 * <p>Writes the identified output to the Iceberg table via {@link
 * org.projectnessie.gc.base.IdentifiedResultsRepo}.
 *
 * <p>Accepts the {@link org.projectnessie.gc.base.GCParams} members as the arguments and returns
 * the run id of the completed GC task which can be used to query the results stored in the Iceberg
 * table via {@link org.projectnessie.gc.base.IdentifiedResultsRepo}.
 */
public class IdentifyExpiredSnapshotsProcedure extends BaseGcProcedure {

  public static final String PROCEDURE_NAME = "identify_expired_snapshots";

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        // Hint: this is in microsecond precision because of Spark's TimestampType
        ProcedureParameter.required("default_cut_off_timestamp", DataTypes.TimestampType),
        ProcedureParameter.required("nessie_catalog_name", DataTypes.StringType),
        ProcedureParameter.required("output_branch_name", DataTypes.StringType),
        ProcedureParameter.required("output_table_identifier", DataTypes.StringType),
        ProcedureParameter.required(
            "nessie_client_configurations",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
        ProcedureParameter.optional(
            "reference_cut_off_timestamps",
            // Hint: this is in microsecond precision because of Spark's TimestampType
            DataTypes.createMapType(DataTypes.StringType, DataTypes.TimestampType)),
        // Hint: this is in microsecond precision because of Spark's TimestampType
        ProcedureParameter.optional("dead_reference_cut_off_timestamp", DataTypes.TimestampType),
        ProcedureParameter.optional("spark_partitions_count", DataTypes.IntegerType),
        ProcedureParameter.optional("commit_protection_time_in_hours", DataTypes.IntegerType),
        ProcedureParameter.optional("bloom_filter_expected_entries", DataTypes.LongType),
        ProcedureParameter.optional("bloom_filter_fpp", DataTypes.DoubleType)
      };

  public static final String OUTPUT_RUN_ID = "run_id";

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(OUTPUT_RUN_ID, DataTypes.StringType, true, Metadata.empty())
          });

  private InternalRow resultRow(String runId) {
    return GcProcedureUtil.internalRow(runId);
  }

  public IdentifyExpiredSnapshotsProcedure(TableCatalog currentCatalog) {
    super(currentCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public String description() {
    return "Identifies the expired snapshots per content id per reference";
  }

  @Override
  public InternalRow[] call(InternalRow internalRow) {
    ImmutableGCParams.Builder paramsBuilder = ImmutableGCParams.builder();
    paramsBuilder.defaultCutOffTimestamp(GCUtil.getInstantFromMicros(internalRow.getLong(0)));
    paramsBuilder.nessieCatalogName(internalRow.getString(1));
    paramsBuilder.outputBranchName(internalRow.getString(2));
    paramsBuilder.outputTableIdentifier(internalRow.getString(3));

    MapData map = internalRow.getMap(4);
    Map<String, String> nessieClientConfig = new HashMap<>();
    for (int i = 0; i < map.numElements(); i++) {
      nessieClientConfig.put(
          map.keyArray().getUTF8String(i).toString(), map.valueArray().getUTF8String(i).toString());
    }
    paramsBuilder.nessieClientConfigs(nessieClientConfig);

    if (!internalRow.isNullAt(5)) {
      map = internalRow.getMap(5);
      Map<String, Instant> perReferenceCutoffs = new HashMap<>();
      for (int i = 0; i < map.numElements(); i++) {
        String refName = map.keyArray().getUTF8String(i).toString();
        Instant cutOffTimestamp = GCUtil.getInstantFromMicros(map.valueArray().getLong(i));
        perReferenceCutoffs.put(refName, cutOffTimestamp);
      }
      paramsBuilder.cutOffTimestampPerRef(perReferenceCutoffs);
    }

    if (!internalRow.isNullAt(6)) {
      paramsBuilder.deadReferenceCutOffTimeStamp(
          GCUtil.getInstantFromMicros(internalRow.getLong(6)));
    }
    if (!internalRow.isNullAt(7)) {
      paramsBuilder.sparkPartitionsCount(internalRow.getInt(7));
    }
    if (!internalRow.isNullAt(8)) {
      paramsBuilder.commitProtectionDuration(Duration.ofHours(internalRow.getInt(8)));
    }
    if (!internalRow.isNullAt(9)) {
      paramsBuilder.bloomFilterExpectedEntries(internalRow.getLong(9));
    }
    if (!internalRow.isNullAt(10)) {
      paramsBuilder.bloomFilterFpp(internalRow.getDouble(10));
    }
    GCImpl gcImpl = new GCImpl(paramsBuilder.build());
    String runId = gcImpl.identifyExpiredContents(spark());
    return new InternalRow[] {resultRow(runId)};
  }
}
