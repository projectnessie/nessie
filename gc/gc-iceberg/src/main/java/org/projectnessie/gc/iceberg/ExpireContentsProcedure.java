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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.procedures.BaseGCProcedure;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.projectnessie.gc.base.IdentifiedResultsRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nessie GC procedure to expire unused snapshots, uses the information written by {@link
 * IdentifyExpiredContentsProcedure} via {@link org.projectnessie.gc.base.IdentifiedResultsRepo}.
 */
public class ExpireContentsProcedure extends BaseGCProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(ExpireContentsProcedure.class);
  public static final String PROCEDURE_NAME = "expire_contents";

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("nessie_catalog_name", DataTypes.StringType),
        ProcedureParameter.required("output_branch_name", DataTypes.StringType),
        ProcedureParameter.required("identify_output_table_identifier", DataTypes.StringType),
        ProcedureParameter.required("expiry_output_table_identifier", DataTypes.StringType),
        ProcedureParameter.required(
            "nessie_client_configurations",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
        ProcedureParameter.optional("run_id", DataTypes.StringType),
        ProcedureParameter.optional("dry_run", DataTypes.BooleanType),
      };

  public static final String OUTPUT_RUN_ID = "runID";

  public static final String OUTPUT_START_TIME = "startTime";

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(OUTPUT_RUN_ID, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OUTPUT_START_TIME, DataTypes.TimestampType, true, Metadata.empty())
          });

  public enum FileType {
    ICEBERG_MANIFEST,
    ICEBERG_MANIFESTLIST,
    DATA_FILE
  }

  // columns from IdentifiedResultsRepo
  private static final String COL_CONTENT_ID = "contentId";
  private static final String COL_METADATA_LOCATION = "metadataLocation";
  private static final String COL_SNAPSHOT_ID = "snapshotId";

  // columns used for intermediate transforms
  private static final String COL_EXPIRED_FILES_ARRAY = "expiredFilesArray";
  private static final String COL_EXPIRED_FILES_WITH_TYPE = "expiredFilesWithType";
  private static final String COL_EXPIRED_FILES_AND_TYPE = "expiredFilesAndType";
  private static final String COL_EXPIRED_FILES = "expiredFiles";
  private static final String COL_EXPIRED_FILES_TYPE = "expiredFilesType";
  private static final String COL_EXPIRED_FILES_COUNT = "expiredFilesCount";
  private static final String COL_EXPIRED_FILES_LIST = "expiredFilesList";
  private static final String COL_GC_RUN_ID = "gcRunID";
  private static final String COL_GC_RUN_START = "gcRunStart";

  public ExpireContentsProcedure(TableCatalog currentCatalog) {
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
    return String.format(
        "Expires the Iceberg snapshots that are collected by '%s' procedure.",
        IdentifyExpiredContentsProcedure.PROCEDURE_NAME);
  }

  @Override
  public InternalRow[] call(InternalRow internalRow) {
    String gcCatalogName = internalRow.getString(0);
    String gcOutputBranchName = internalRow.getString(1);
    String identifyOutputTableIdentifier = internalRow.getString(2);
    String expiryOutputTableIdentifier = internalRow.getString(3);
    Map<String, String> nessieClientConfig = new HashMap<>();
    MapData map = internalRow.getMap(4);
    for (int i = 0; i < map.numElements(); i++) {
      nessieClientConfig.put(
          map.keyArray().getUTF8String(i).toString(), map.valueArray().getUTF8String(i).toString());
    }
    String runId = !internalRow.isNullAt(5) ? internalRow.getString(5) : null;
    boolean dryRun = !internalRow.isNullAt(6) && internalRow.getBoolean(6);

    IdentifiedResultsRepo identifiedResultsRepo =
        new IdentifiedResultsRepo(
            spark(), gcCatalogName, gcOutputBranchName, identifyOutputTableIdentifier);

    if (runId == null) {
      runId = getLatestCompletedRunID(identifyOutputTableIdentifier, identifiedResultsRepo);
    }

    Instant startedAt = Instant.now();

    try (FileIO fileIO = getFileIO(nessieClientConfig)) {
      Dataset<Row> expiredContentsDF =
          getExpiredContents(runId, startedAt, identifiedResultsRepo, fileIO);
      if (!dryRun) {
        expiredContentsDF =
            expiredContentsDF.map(
                new DeleteFunction(fileIO), RowEncoder.apply(expiredContentsDF.schema()));
      }
      ExpiredResultsRepo expiredResultsRepo =
          new ExpiredResultsRepo(
              spark(), gcCatalogName, gcOutputBranchName, expiryOutputTableIdentifier);
      // "startTime", "runID", "contentId", "expiredFilesType", "expiredFilesCount",
      // "expiredFilesList"
      expiredResultsRepo.writeToOutputTable(expiredContentsDF);
      return new InternalRow[] {GCProcedureUtil.internalRow(runId, instantToMicros(startedAt))};
    }
  }

  private FileIO getFileIO(Map<String, String> nessieClientConfig) {
    Configuration config = spark().sparkContext().hadoopConfiguration();
    String fileIOImpl = nessieClientConfig.get(CatalogProperties.FILE_IO_IMPL);
    return fileIOImpl == null
        ? new HadoopFileIO(config)
        : CatalogUtil.loadFileIO(fileIOImpl, nessieClientConfig, config);
  }

  private static Dataset<Row> getExpiredContents(
      String runId, Instant startedAt, IdentifiedResultsRepo identifiedResultsRepo, FileIO fileIO) {
    // Read the expired content-rows from output table for this run id
    Dataset<Row> expiredContents = identifiedResultsRepo.collectExpiredContentsAsDataSet(runId);
    Dataset<Row> expiredContentsDF = computeAllFiles(fileIO, expiredContents);
    // Read the live content-rows from output table for this run id
    Dataset<Row> liveContents = identifiedResultsRepo.collectLiveContentsAsDataSet(runId);
    Dataset<Row> liveContentsDF = computeAllFiles(fileIO, liveContents);
    // remove the files which are used by live contents
    Dataset<Row> expiredFilesDF = expiredContentsDF.except(liveContentsDF);
    // final output
    // Example output row:
    // timestamp1, runID1, content_id_1, manifestLists, 3, {a,b,c}
    // timestamp1, runID1, content_id_1, manifests, 2, {d,e}
    // timestamp1, runID1, content_id_1, datafiles, 2, {f,g}
    return expiredFilesDF
        .groupBy(COL_CONTENT_ID, COL_EXPIRED_FILES_TYPE)
        .agg(functions.collect_list(COL_EXPIRED_FILES).as(COL_EXPIRED_FILES_LIST))
        .withColumn(COL_EXPIRED_FILES_COUNT, functions.size(functions.col(COL_EXPIRED_FILES_LIST)))
        .withColumn(COL_GC_RUN_ID, functions.lit(runId))
        .withColumn(COL_GC_RUN_START, functions.lit(startedAt))
        .select(
            COL_GC_RUN_START,
            COL_GC_RUN_ID,
            COL_CONTENT_ID,
            COL_EXPIRED_FILES_TYPE,
            COL_EXPIRED_FILES_COUNT,
            COL_EXPIRED_FILES_LIST);
  }

  private static Dataset<Row> computeAllFiles(FileIO fileIO, Dataset<Row> rowDataset) {
    // read the metadata file for each expired contents to collect the expired manifestList,
    // manifests,
    // datafiles.
    // Example output row:
    // row1: content_id_1,
    //
    // {manifestLists#a,manifestLists#b,manifestLists#c,manifests#d,manifests#e,datafiles#f,datafiles#g}
    // row2: content_id_1,
    //     {manifestLists#a,manifestLists#b,manifests#d,datafiles#f}
    Dataset<Row> dataset =
        rowDataset.withColumn(
            COL_EXPIRED_FILES_ARRAY,
            computeAllFilesUDF(
                rowDataset.col(COL_METADATA_LOCATION), rowDataset.col(COL_SNAPSHOT_ID), fileIO));
    // explode expired files array and drop duplicates.
    // Example output row:
    // content_id_1, manifestLists#a
    // content_id_1, manifestLists#b
    // content_id_1, manifestLists#c
    // content_id_1, manifests#d
    // content_id_1, manifests#e
    // content_id_1, datafiles#f
    // content_id_1, datafiles#g
    dataset =
        dataset
            .withColumn(
                COL_EXPIRED_FILES_WITH_TYPE,
                functions.explode(dataset.col(COL_EXPIRED_FILES_ARRAY)))
            .select(COL_CONTENT_ID, COL_EXPIRED_FILES_WITH_TYPE)
            .dropDuplicates();

    // split the type and value column of the expired files
    // Example output row:
    // content_id_1, manifestLists, a
    // content_id_1, manifestLists, b
    // content_id_1, manifestLists, c
    // content_id_1, manifests, d
    // content_id_1, manifests, e
    // content_id_1, datafiles, f
    // content_id_1, datafiles, g
    dataset =
        dataset
            .withColumn(
                COL_EXPIRED_FILES_AND_TYPE,
                functions.split(functions.col(COL_EXPIRED_FILES_WITH_TYPE), "#", 2))
            .select(
                functions.col(COL_CONTENT_ID),
                functions.col(COL_EXPIRED_FILES_AND_TYPE).getItem(0).as(COL_EXPIRED_FILES_TYPE),
                functions.col(COL_EXPIRED_FILES_AND_TYPE).getItem(1).as(COL_EXPIRED_FILES));
    return dataset;
  }

  private static Column computeAllFilesUDF(
      Column metadataLocation, Column snapshotId, FileIO fileIO) {
    return functions
        .udf(new ComputeAllFilesUDF(fileIO), DataTypes.createArrayType(DataTypes.StringType))
        .apply(metadataLocation, snapshotId);
  }

  private static String getLatestCompletedRunID(
      String gcOutputTableName, IdentifiedResultsRepo identifiedResultsRepo) {
    Optional<String> latestCompletedRunID = identifiedResultsRepo.getLatestCompletedRunID();
    if (!latestCompletedRunID.isPresent()) {
      throw new RuntimeException(
          String.format(
              "No runId present in gc output table : %s, please execute %s first",
              gcOutputTableName, IdentifyExpiredContentsProcedure.PROCEDURE_NAME));
    }
    return latestCompletedRunID.get();
  }

  /** Returns the instant in microseconds since epoch. */
  private static long instantToMicros(Instant instant) {
    long time = instant.getEpochSecond();
    long nano = instant.getNano();
    return TimeUnit.SECONDS.toMicros(time) + NANOSECONDS.toMicros(nano);
  }

  private static class DeleteFunction implements MapFunction<Row, Row> {
    private final FileIO fileIO;

    private DeleteFunction(FileIO fileIO) {
      this.fileIO = fileIO;
    }

    @Override
    public Row call(Row value) {
      // "startTime", "runID", "contentId", "expiredFilesType", "expiredFilesCount",
      // "expiredFilesList"
      List<String> files = value.getList(5);
      files.forEach(
          file -> {
            try {
              fileIO.deleteFile(file);
            } catch (UncheckedIOException e) {
              LOG.warn("Failed to delete the file.", e);
            }
          });
      return value;
    }
  }
}
