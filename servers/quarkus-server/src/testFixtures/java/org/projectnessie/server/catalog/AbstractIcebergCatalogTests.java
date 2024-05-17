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
package org.projectnessie.server.catalog;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.WAREHOUSE_NAME;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;

public abstract class AbstractIcebergCatalogTests extends CatalogTests<RESTCatalog> {
  public static final String EMPTY_OBJ_ID =
      "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d";

  protected final List<RESTCatalog> catalogs = new ArrayList<>();

  protected RESTCatalog catalog() {
    int catalogServerPort = Integer.getInteger("quarkus.http.port");
    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        getClass().getSimpleName(),
        Map.of(
            CatalogProperties.URI,
            String.format("http://127.0.0.1:%d/iceberg/", catalogServerPort),
            CatalogProperties.WAREHOUSE_LOCATION,
            WAREHOUSE_NAME));
    catalogs.add(catalog);
    return catalog;
  }

  @AfterEach
  void cleanup() throws Exception {
    for (RESTCatalog catalog : catalogs) {
      catalog.close();
    }

    try (NessieApiV2 api = nessieClientBuilder().build(NessieApiV2.class)) {
      Reference main = null;
      for (Reference reference : api.getAllReferences().stream().toList()) {
        if (reference.getName().equals("main")) {
          main = reference;
        } else {
          api.deleteReference().reference(reference).delete();
        }
      }
      api.assignReference().reference(main).assignTo(Branch.of("main", EMPTY_OBJ_ID)).assign();
    }
  }

  protected NessieClientBuilder nessieClientBuilder() {
    int catalogServerPort = Integer.getInteger("quarkus.http.port");
    return NessieClientBuilder.createClientBuilderFromSystemSettings()
        .withUri(String.format("http://127.0.0.1:%d/api/v2/", catalogServerPort));
  }

  @Override
  protected boolean supportsPerTableHistory() {
    return false;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }

  @Override
  protected boolean overridesRequestedLocation() {
    return true;
  }

  @Override
  protected String temporaryLocation() {
    throw new UnsupportedOperationException("Implement in a super class");
  }

  /**
   * Similar to {@link #testRegisterTable()} but places a table-metadata file in the local file
   * system.
   *
   * <p>Need a separate test case, because {@link #testRegisterTable()} uses the metadata-location
   * returned from the server, which points to the Nessie Catalog URL, and Nessie Catalog
   * table-metadata URLs are treated differently. This test cases exercises the "else" part that
   * registers a table from an external object store.
   */
  @Test
  public void testRegisterTableFromFileSystem() throws Exception {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    Map<String, String> properties =
        org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.of(
            "user", "someone", "created-at", "2023-01-15T00:00:01");
    Table originalTable =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .create();

    originalTable.newFastAppend().appendFile(FILE_A).commit();

    TableOperations ops = ((BaseTable) originalTable).operations();

    String metadataLocation = temporaryLocation() + "/my-metadata-" + UUID.randomUUID() + ".json";
    try (OutputStream output = ops.io().newOutputFile(metadataLocation).create()) {
      output.write(TableMetadataParser.toJson(ops.current()).getBytes(StandardCharsets.UTF_8));
    }

    catalog.dropTable(TABLE, false /* do not purge */);

    Table registeredTable = catalog.registerTable(TABLE, metadataLocation);

    Assertions.assertThat(registeredTable).isNotNull();
    Assertions.assertThat(catalog.tableExists(TABLE)).as("Table must exist").isTrue();
    Assertions.assertThat(registeredTable.properties())
        .as("Props must match")
        .containsAllEntriesOf(properties);
    Assertions.assertThat(registeredTable.schema().asStruct())
        .as("Schema must match")
        .isEqualTo(originalTable.schema().asStruct());
    Assertions.assertThat(registeredTable.specs())
        .as("Specs must match")
        .isEqualTo(originalTable.specs());
    Assertions.assertThat(registeredTable.sortOrders())
        .as("Sort orders must match")
        .isEqualTo(originalTable.sortOrders());
    Assertions.assertThat(registeredTable.currentSnapshot())
        .as("Current snapshot must match")
        .isEqualTo(originalTable.currentSnapshot());
    Assertions.assertThat(registeredTable.snapshots())
        .as("Snapshots must match")
        .isEqualTo(originalTable.snapshots());
    Assertions.assertThat(registeredTable.history())
        .as("History must match")
        .isEqualTo(originalTable.history());

    Assertions.assertThat(catalog.loadTable(TABLE)).isNotNull();
    Assertions.assertThat(catalog.dropTable(TABLE)).isTrue();
    Assertions.assertThat(catalog.tableExists(TABLE)).isFalse();
  }

  // TODO the following tests have been copied from Iceberg's `TestRESTCatalog`. Those should
  //  ideally live in `CatalogTests`.

  @Test
  public void diffAgainstSingleTable() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace namespace = Namespace.of("namespace");
    catalog.createNamespace(namespace);
    TableIdentifier identifier = TableIdentifier.of(namespace, "multipleDiffsAgainstSingleTable");

    Table table = catalog.buildTable(identifier, SCHEMA).create();
    Transaction transaction = table.newTransaction();

    UpdateSchema updateSchema =
        transaction.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdatePartitionSpec updateSpec =
        transaction.updateSpec().addField("shard", Expressions.bucket("id", 16));
    PartitionSpec expectedSpec = updateSpec.apply();
    updateSpec.commit();

    TableCommit tableCommit =
        TableCommit.create(
            identifier,
            ((BaseTransaction) transaction).startMetadata(),
            ((BaseTransaction) transaction).currentMetadata());

    catalog.commitTransaction(tableCommit);

    Table loaded = catalog.loadTable(identifier);
    Assertions.assertThat(loaded.schema().asStruct()).isEqualTo(expectedSchema.asStruct());
    Assertions.assertThat(loaded.spec().fields()).isEqualTo(expectedSpec.fields());
  }

  @Test
  public void multipleDiffsAgainstMultipleTables() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace namespace = Namespace.of("multiDiffNamespace");
    catalog.createNamespace(namespace);
    TableIdentifier identifier1 = TableIdentifier.of(namespace, "multiDiffTable1");
    TableIdentifier identifier2 = TableIdentifier.of(namespace, "multiDiffTable2");

    Table table1 = catalog.buildTable(identifier1, SCHEMA).create();
    Table table2 = catalog.buildTable(identifier2, SCHEMA).create();
    Transaction t1Transaction = table1.newTransaction();
    Transaction t2Transaction = table2.newTransaction();

    UpdateSchema updateSchema =
        t1Transaction.updateSchema().addColumn("new_col", Types.LongType.get());
    Schema expectedSchema = updateSchema.apply();
    updateSchema.commit();

    UpdateSchema updateSchema2 =
        t2Transaction.updateSchema().addColumn("new_col2", Types.LongType.get());
    Schema expectedSchema2 = updateSchema2.apply();
    updateSchema2.commit();

    TableCommit tableCommit1 =
        TableCommit.create(
            identifier1,
            ((BaseTransaction) t1Transaction).startMetadata(),
            ((BaseTransaction) t1Transaction).currentMetadata());

    TableCommit tableCommit2 =
        TableCommit.create(
            identifier2,
            ((BaseTransaction) t2Transaction).startMetadata(),
            ((BaseTransaction) t2Transaction).currentMetadata());

    catalog.commitTransaction(tableCommit1, tableCommit2);

    Assertions.assertThat(catalog.loadTable(identifier1).schema().asStruct())
        .isEqualTo(expectedSchema.asStruct());

    Assertions.assertThat(catalog.loadTable(identifier2).schema().asStruct())
        .isEqualTo(expectedSchema2.asStruct());
  }

  @Test
  public void multipleDiffsAgainstMultipleTablesLastFails() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace namespace = Namespace.of("multiDiffNamespace");
    catalog.createNamespace(namespace);
    TableIdentifier identifier1 = TableIdentifier.of(namespace, "multiDiffTable1");
    TableIdentifier identifier2 = TableIdentifier.of(namespace, "multiDiffTable2");

    catalog.createTable(identifier1, SCHEMA);
    catalog.createTable(identifier2, SCHEMA);

    Table table1 = catalog.loadTable(identifier1);
    Table table2 = catalog.loadTable(identifier2);
    Schema originalSchemaOne = table1.schema();

    Transaction t1Transaction = catalog.loadTable(identifier1).newTransaction();
    t1Transaction.updateSchema().addColumn("new_col1", Types.LongType.get()).commit();

    Transaction t2Transaction = catalog.loadTable(identifier2).newTransaction();
    t2Transaction.updateSchema().renameColumn("data", "new-column").commit();

    // delete the colum that is being renamed in the above TX to cause a conflict
    table2
        .updateSchema()
        .addColumn("another", Types.LongType.get())
        .addColumn("more", Types.LongType.get())
        .commit();
    Schema updatedSchemaTwo = table2.schema();

    TableCommit tableCommit1 =
        TableCommit.create(
            identifier1,
            ((BaseTransaction) t1Transaction).startMetadata(),
            ((BaseTransaction) t1Transaction).currentMetadata());

    TableCommit tableCommit2 =
        TableCommit.create(
            identifier2,
            ((BaseTransaction) t2Transaction).startMetadata(),
            ((BaseTransaction) t2Transaction).currentMetadata());

    assertThatThrownBy(() -> catalog.commitTransaction(tableCommit1, tableCommit2))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(
            "Requirement failed: last assigned field id changed: expected 4 != 2");

    Schema schema1 = catalog.loadTable(identifier1).schema();
    Assertions.assertThat(schema1.asStruct()).isEqualTo(originalSchemaOne.asStruct());

    Schema schema2 = catalog.loadTable(identifier2).schema();
    Assertions.assertThat(schema2.asStruct()).isEqualTo(updatedSchemaTwo.asStruct());
    Assertions.assertThat(schema2.findField("data")).isNotNull();
    Assertions.assertThat(schema2.findField("another")).isNotNull();
    Assertions.assertThat(schema2.findField("more")).isNotNull();
    Assertions.assertThat(schema2.findField("new-column")).isNull();
    Assertions.assertThat(schema2.columns()).hasSize(4);
  }

  @Test
  public void testStatistics() {

    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .create();

    // Create a snapshot
    table.newFastAppend().commit();

    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    long snapshotId = metadata.currentSnapshot().snapshotId();

    StatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId,
            "/some/statistics/file.puffin",
            100,
            42,
            List.of(
                new GenericBlobMetadata(
                    "stats-type",
                    snapshotId,
                    metadata.lastSequenceNumber(),
                    List.of(1, 2),
                    Map.of("a-property", "some-property-value"))));

    table.updateStatistics().setStatistics(snapshotId, statisticsFile).commit();

    PartitionStatisticsFile partitionStatisticsFile =
        ImmutableGenericPartitionStatisticsFile.builder()
            .snapshotId(snapshotId)
            .path("/some/partition-statistics/file")
            .fileSizeInBytes(100)
            .build();

    table.updatePartitionStatistics().setPartitionStatistics(partitionStatisticsFile).commit();

    table = catalog.loadTable(TABLE);
    metadata = ((HasTableOperations) table).operations().current();
    Assertions.assertThat(metadata.statisticsFiles()).containsAll(List.of(statisticsFile));
    Assertions.assertThat(metadata.partitionStatisticsFiles())
        .containsAll(List.of(partitionStatisticsFile));

    table.updateStatistics().removeStatistics(snapshotId).commit();
    table.updatePartitionStatistics().removePartitionStatistics(snapshotId).commit();

    table = catalog.loadTable(TABLE);
    metadata = ((HasTableOperations) table).operations().current();
    Assertions.assertThat(metadata.statisticsFiles()).isEmpty();
    Assertions.assertThat(metadata.partitionStatisticsFiles()).isEmpty();
  }

  @Test
  public void testStatisticsTransaction() {

    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table table =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .create();

    // Create a snapshot
    table.newFastAppend().commit();

    TableMetadata metadata = ((HasTableOperations) table).operations().current();
    long snapshotId = metadata.currentSnapshot().snapshotId();

    Transaction transaction = table.newTransaction();

    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId,
            "/some/statistics/file.puffin",
            100,
            42,
            List.of(
                new GenericBlobMetadata(
                    "stats-type",
                    snapshotId,
                    metadata.lastSequenceNumber(),
                    List.of(1, 2),
                    Map.of("a-property", "some-property-value"))));

    transaction.updateStatistics().setStatistics(snapshotId, statisticsFile).commit();

    PartitionStatisticsFile partitionStatisticsFile =
        ImmutableGenericPartitionStatisticsFile.builder()
            .snapshotId(snapshotId)
            .path("/some/partition-statistics/file")
            .fileSizeInBytes(100)
            .build();

    transaction
        .updatePartitionStatistics()
        .setPartitionStatistics(partitionStatisticsFile)
        .commit();

    transaction.commitTransaction();

    table = catalog.loadTable(TABLE);
    metadata = ((HasTableOperations) table).operations().current();
    Assertions.assertThat(metadata.statisticsFiles()).containsAll(List.of(statisticsFile));
    Assertions.assertThat(metadata.partitionStatisticsFiles())
        .containsAll(List.of(partitionStatisticsFile));

    transaction = table.newTransaction();
    transaction.updateStatistics().removeStatistics(snapshotId).commit();
    transaction.updatePartitionStatistics().removePartitionStatistics(snapshotId).commit();
    transaction.commitTransaction();

    table = catalog.loadTable(TABLE);
    metadata = ((HasTableOperations) table).operations().current();
    Assertions.assertThat(metadata.statisticsFiles()).isEmpty();
    Assertions.assertThat(metadata.partitionStatisticsFiles()).isEmpty();
  }
}
