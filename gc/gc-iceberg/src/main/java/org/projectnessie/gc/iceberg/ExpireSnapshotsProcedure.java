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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.procedures.BaseGcProcedure;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.gc.base.GCUtil;
import org.projectnessie.gc.base.IdentifiedResultsRepo;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;

/**
 * Nessie GC procedure to expire unused snapshots, uses the information written by {@link
 * IdentifyExpiredSnapshotsProcedure} via {@link org.projectnessie.gc.base.IdentifiedResultsRepo}.
 */
public class ExpireSnapshotsProcedure extends BaseGcProcedure {

  public static final String PROCEDURE_NAME = "expire_snapshots";

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("expire_procedure_reference_name", DataTypes.StringType),
        ProcedureParameter.required("nessie_catalog_name", DataTypes.StringType),
        ProcedureParameter.required("output_branch_name", DataTypes.StringType),
        ProcedureParameter.required("output_table_identifier", DataTypes.StringType),
        ProcedureParameter.required(
            "nessie_client_configurations",
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
        ProcedureParameter.optional("run_id", DataTypes.StringType),
        // TODO: how the user will know the content ids?
        ProcedureParameter.optional("content_ids_to_expire", DataTypes.StringType),
        ProcedureParameter.optional("dry_run", DataTypes.BooleanType),
      };

  public static final String OUTPUT_CONTENT_ID = "content_id";
  public static final String OUTPUT_EXPIRED_DATA_FILES_COUNT = "deleted_data_files_count";
  public static final String OUTPUT_EXPIRED_MANIFEST_LISTS_COUNT = "deleted_manifest_lists_count";
  public static final String OUTPUT_EXPIRED_MANIFESTS_COUNT = "deleted_manifests_count";
  public static final String OUTPUT_EXPIRED_FILES_LIST = "deleted_files_list";
  public static final String OUTPUT_SNAPSHOT_IDS = "snapshot_ids";

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(OUTPUT_CONTENT_ID, DataTypes.StringType, true, Metadata.empty()),
            new StructField(
                OUTPUT_SNAPSHOT_IDS,
                DataTypes.createArrayType(DataTypes.LongType),
                true,
                Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_DATA_FILES_COUNT, DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_MANIFEST_LISTS_COUNT, DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_MANIFESTS_COUNT, DataTypes.LongType, true, Metadata.empty()),
            new StructField(
                OUTPUT_EXPIRED_FILES_LIST,
                DataTypes.createArrayType(DataTypes.StringType),
                true,
                Metadata.empty())
          });

  public ExpireSnapshotsProcedure(TableCatalog currentCatalog) {
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
        IdentifyExpiredSnapshotsProcedure.PROCEDURE_NAME);
  }

  @Override
  public InternalRow[] call(InternalRow internalRow) {
    String refName = internalRow.getString(0);
    String gcCatalogName = internalRow.getString(1);
    String gcOutputBranchName = internalRow.getString(2);
    String gcOutputTableName = internalRow.getString(3);
    Map<String, String> nessieClientConfig = new HashMap<>();
    MapData map = internalRow.getMap(4);
    for (int i = 0; i < map.numElements(); i++) {
      nessieClientConfig.put(
          map.keyArray().getUTF8String(i).toString(), map.valueArray().getUTF8String(i).toString());
    }
    String runId = !internalRow.isNullAt(5) ? internalRow.getString(5) : null;
    String contentIds = !internalRow.isNullAt(6) ? internalRow.getString(6) : null;
    boolean dryRun = !internalRow.isNullAt(7) && internalRow.getBoolean(7);

    IdentifiedResultsRepo identifiedResultsRepo =
        new IdentifiedResultsRepo(spark(), gcCatalogName, gcOutputBranchName, gcOutputTableName);

    if (runId == null) {
      Optional<String> latestCompletedRunID = identifiedResultsRepo.getLatestCompletedRunID();
      if (!latestCompletedRunID.isPresent()) {
        throw new RuntimeException(
            String.format(
                "No runId present in gc output table : %s, please execute %s first",
                gcOutputTableName, IdentifyExpiredSnapshotsProcedure.PROCEDURE_NAME));
      }
      runId = latestCompletedRunID.get();
    }

    Dataset<Row> rowDataset = identifiedResultsRepo.collectExpiredContentsAsDataSet(runId);
    List<Row> rows =
        rowDataset
            .groupBy("contentId")
            .agg(functions.collect_set("snapshotId").as("snapshotIds"))
            .select("contentId", "snapshotIds")
            .collectAsList();

    Map<String, List<Long>> expiredSnapshotsPerContentId = new HashMap<>();
    rows.forEach(row -> expiredSnapshotsPerContentId.put(row.getString(0), row.getList(1)));

    List<InternalRow> outputRows = new ArrayList<>();

    try (NessieApiV1 api = GCUtil.getApi(nessieClientConfig)) {
      try {
        // create a reference pointing to NO_ANCESTOR
        api.createReference().reference(Branch.of(refName, null)).create();
      } catch (NessieNotFoundException | NessieConflictException e) {
        throw new RuntimeException(e);
      }
      // get Nessie catalog
      Catalog nessieCatalog = GCUtil.loadNessieCatalog(spark(), gcCatalogName, refName);
      expiredSnapshotsPerContentId
          .entrySet()
          .forEach(
              entry -> {
                Row row = identifiedResultsRepo.getAnyCommitHashAndKeyForContentId(entry.getKey());
                String commitHash = row.getString(0);
                String contentKey = row.getString(1);
                String lastCommitHash = getHeadCommitHash(api, refName);
                try {
                  api.assignBranch()
                      .branch(Branch.of(refName, lastCommitHash))
                      .assignTo(Detached.of(commitHash))
                      .assign();
                } catch (NessieNotFoundException | NessieConflictException e) {
                  throw new RuntimeException(e);
                }
                List<String> filesToBeDeleted = new ArrayList<>();
                Table table = nessieCatalog.loadTable(TableIdentifier.parse(contentKey));
                Consumer<String> expiredFilesConsumer;
                if (!dryRun) {
                  expiredFilesConsumer =
                      fileName -> {
                        filesToBeDeleted.add(fileName);
                        // delete the expired files
                        ((HasTableOperations) table).operations().io().deleteFile(fileName);
                      };
                } else {
                  expiredFilesConsumer = filesToBeDeleted::add;
                }
                ExpireSnapshots expireSnapshots =
                    SparkActions.get().expireSnapshots(table).deleteWith(expiredFilesConsumer);
                entry.getValue().forEach(expireSnapshots::expireSnapshotId);
                ExpireSnapshots.Result result = expireSnapshots.execute();
                InternalRow outputRow =
                    GcProcedureUtil.internalRow(
                        entry.getKey(),
                        entry.getValue(),
                        result.deletedDataFilesCount(),
                        result.deletedManifestListsCount(),
                        result.deletedManifestsCount(),
                        filesToBeDeleted);
                outputRows.add(outputRow);
              });
      try {
        String lastCommitHash = getHeadCommitHash(api, refName);
        api.deleteBranch().branchName(refName).hash(lastCommitHash).delete();
      } catch (NessieNotFoundException | NessieConflictException e) {
        throw new RuntimeException(e);
      }
    }
    return outputRows.toArray(new InternalRow[0]);
  }

  private String getHeadCommitHash(NessieApiV1 api, String refName) {
    try {
      return api.getReference().refName(refName).get().getHash();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
