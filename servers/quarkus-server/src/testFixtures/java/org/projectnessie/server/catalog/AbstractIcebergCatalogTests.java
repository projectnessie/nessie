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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.EMPTY_OBJ_ID;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.WAREHOUSE_NAME;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.ToLongFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
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
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.View;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Reference;
import org.projectnessie.storage.uri.StorageUri;

public abstract class AbstractIcebergCatalogTests extends CatalogTests<RESTCatalog> {

  private static final Catalogs CATALOGS = new Catalogs();

  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

  protected RESTCatalog catalog() {
    return CATALOGS.getCatalog(catalogOptions());
  }

  protected Map<String, String> catalogOptions() {
    return ImmutableMap.of(
        CatalogProperties.WAREHOUSE_LOCATION,
        WAREHOUSE_NAME,
        "adls.sas-token.account.dfs.core.windows.net",
        "token");
  }

  @AfterAll
  static void closeRestCatalog() throws Exception {
    CATALOGS.close();
  }

  @AfterEach
  void cleanup() throws Exception {
    try {
      // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
      // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
      soft.assertAll();
    } finally {
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
  }

  protected NessieClientBuilder nessieClientBuilder() {
    int catalogServerPort = Integer.getInteger("quarkus.http.port");
    return NessieClientBuilder.createClientBuilderFromSystemSettings()
        .withUri(format("http://127.0.0.1:%d/api/v2/", catalogServerPort));
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
  protected boolean supportsBranches() {
    return false;
  }

  @Override
  protected boolean supportsSpecV3() {
    return false;
  }

  @Override
  protected abstract String temporaryLocation();

  protected abstract String scheme();

  @Test
  public void testRemovePartitionSpec() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    TableIdentifier ident = TableIdentifier.of("ns", "table");

    assertThat(catalog.tableExists(ident)).as("Table should not exist").isFalse();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ident.namespace());
    }

    Table table =
        catalog
            .buildTable(ident, SCHEMA)
            .withLocation(temporaryLocation())
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .create();
    assertThat(catalog.tableExists(ident)).as("Table should exist").isTrue();
    assertThat(table.specs()).hasSize(1);
    int initialSpecId = table.spec().specId();

    table.updateSpec().addField("data").commit();

    table = catalog.loadTable(ident);
    assertThat(table.specs()).hasSize(2);

    table.updateSpec().commit();

    // TODO there's no API in Iceberg/Java to actually remove a partition spec :(
  }

  @Test
  public void testNewTableLocationFromParentNamespace() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    if (this.requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    catalog.buildTable(TABLE, SCHEMA).withPartitionSpec(SPEC).withSortOrder(WRITE_ORDER).create();

    Table pokeTable = catalog.loadTable(TABLE);

    String pokeTableLocation = pokeTable.location();

    int idx = pokeTableLocation.indexOf(format("/%s/", TABLE.namespace().level(0)));
    StorageUri rootIshUri = StorageUri.of(pokeTableLocation.substring(0, idx));

    String namespaceLocation = rootIshUri + "/some/custom/path";
    Map<String, String> namespaceProps = new HashMap<>();
    namespaceProps.put("location", namespaceLocation);
    Namespace namespace = Namespace.of("new_table_location_from_namespace");
    catalog.createNamespace(namespace, namespaceProps);

    Map<String, String> namespaceMeta = catalog.loadNamespaceMetadata(namespace);
    assertThat(namespaceMeta).containsEntry("location", namespaceLocation);

    Namespace nestedNamespace = Namespace.of("new_table_location_from_namespace", "nested_level");
    catalog.createNamespace(nestedNamespace, namespaceProps);

    Table tableWithNsLoc =
        catalog
            .buildTable(TableIdentifier.of(nestedNamespace, "table_loc_from_ns"), SCHEMA)
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .create();

    String tableLocation = tableWithNsLoc.location();
    assertThat(tableLocation).startsWith(namespaceLocation + "/nested_level/table_loc_from_ns_");

    View viewWithNsLoc =
        catalog
            .buildView(TableIdentifier.of(nestedNamespace, "view_loc_from_ns"))
            .withSchema(SCHEMA)
            .withQuery("magic", "foo")
            .withDefaultNamespace(nestedNamespace)
            .create();
    String viewLocation = viewWithNsLoc.location();
    assertThat(viewLocation).startsWith(namespaceLocation + "/nested_level/view_loc_from_ns_");
  }

  @Test
  public void testNessieCreateOnDifferentBranch() throws Exception {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    try (NessieApiV2 api = nessieClientBuilder().build(NessieApiV2.class)) {
      String tableName = TABLE.name();
      String namespaceName = NS.levels()[0];
      ContentKey keyNamespace = ContentKey.of(namespaceName);
      ContentKey keyTable = ContentKey.of(namespaceName, tableName);

      if (this.requiresNamespaceCreate()) {
        catalog.createNamespace(NS);
      }

      Branch defaultBranch = api.getDefaultBranch();
      Branch branch = Branch.of("testNessieCreateOnDifferentBranch", defaultBranch.getHash());

      api.createReference().reference(branch).sourceRefName(defaultBranch.getName()).create();

      TableIdentifier onDifferentBranch =
          TableIdentifier.of(NS, format("`%s@%s`", tableName, branch.getName()));

      // "Create ICEBERG_TABLE" commit
      Table table =
          catalog
              .buildTable(onDifferentBranch, SCHEMA)
              .withPartitionSpec(SPEC)
              .withSortOrder(WRITE_ORDER)
              .create();
      table.newFastAppend().commit();

      // Create another commit
      catalog.loadTable(onDifferentBranch).newFastAppend().appendFile(FILE_C).commit();

      soft.assertThatThrownBy(() -> catalog.loadTable(TABLE))
          .isInstanceOf(NoSuchTableException.class);
      soft.assertThatCode(() -> catalog.loadTable(onDifferentBranch)).doesNotThrowAnyException();

      Set<ContentKey> onMain =
          api.getEntries().refName(defaultBranch.getName()).stream()
              .map(EntriesResponse.Entry::getName)
              .collect(Collectors.toSet());
      soft.assertThat(onMain).containsExactlyInAnyOrder(keyNamespace);
      Set<ContentKey> onBranch =
          api.getEntries().refName(branch.getName()).stream()
              .map(EntriesResponse.Entry::getName)
              .collect(Collectors.toSet());
      soft.assertThat(onBranch).containsExactlyInAnyOrder(keyNamespace, keyTable);

      soft.assertThatThrownBy(
              () -> catalog.loadTable(TABLE).updateProperties().set("foo", "bar").commit())
          .isInstanceOf(NoSuchTableException.class);
      soft.assertThatCode(
              () ->
                  catalog
                      .loadTable(onDifferentBranch)
                      .updateProperties()
                      .set("foo", "bar")
                      .commit())
          .doesNotThrowAnyException();

      soft.assertThat(catalog.dropTable(TABLE)).isFalse();
      soft.assertThat(catalog.dropTable(onDifferentBranch)).isTrue();
    }
  }

  @Test
  public void testNessieTimeTravel() throws Exception {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    try (NessieApiV2 api = nessieClientBuilder().build(NessieApiV2.class)) {
      String tableName = TABLE.name();
      String namespaceName = NS.levels()[0];
      ContentKey key = ContentKey.of(namespaceName, tableName);

      if (this.requiresNamespaceCreate()) {
        catalog.createNamespace(NS);
      }

      String head = api.getDefaultBranch().getHash();
      String tip;

      List<Long> icebergSnapshotIds = new ArrayList<>();
      List<Long> nessieSnapshotIds = new ArrayList<>();

      ToLongFunction<String> fetchSnapshotId =
          commitId -> {
            try {
              Content content =
                  api.getContent().refName("main").hashOnRef(commitId).getSingle(key).getContent();
              return ((IcebergTable) content).getSnapshotId();
            } catch (NessieNotFoundException e) {
              throw new RuntimeException(e);
            }
          };

      // "Create ICEBERG_TABLE" commit
      Table table =
          catalog
              .buildTable(TABLE, SCHEMA)
              .withPartitionSpec(SPEC)
              .withSortOrder(WRITE_ORDER)
              .create();
      soft.assertThat(table.currentSnapshot()).isNull();
      icebergSnapshotIds.add(-1L);
      tip = api.getDefaultBranch().getHash();
      soft.assertThat(tip).isNotEqualTo(head);
      head = tip;
      nessieSnapshotIds.add(fetchSnapshotId.applyAsLong(head));

      // Create a snapshot
      // 2nd "Update ICEBERG_TABLE" commit
      table.newFastAppend().commit();
      icebergSnapshotIds.add(table.currentSnapshot().snapshotId());
      tip = api.getDefaultBranch().getHash();
      soft.assertThat(tip).isNotEqualTo(head);
      head = tip;
      nessieSnapshotIds.add(fetchSnapshotId.applyAsLong(head));

      // create an initial snapshot
      // 3rd "Update ICEBERG_TABLE" commit
      catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_C).commit();
      table = catalog.loadTable(TABLE);
      icebergSnapshotIds.add(table.currentSnapshot().snapshotId());
      tip = api.getDefaultBranch().getHash();
      soft.assertThat(tip).isNotEqualTo(head);
      head = tip;
      nessieSnapshotIds.add(fetchSnapshotId.applyAsLong(head));

      table.newFastAppend().appendFile(FILE_A).commit();
      // 4th "Update ICEBERG_TABLE" commit
      icebergSnapshotIds.add(table.currentSnapshot().snapshotId());
      tip = api.getDefaultBranch().getHash();
      soft.assertThat(tip).isNotEqualTo(head);
      head = tip;
      nessieSnapshotIds.add(fetchSnapshotId.applyAsLong(head));

      // 5th "Update ICEBERG_TABLE" commit
      catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_B).commit();
      table = catalog.loadTable(TABLE);
      icebergSnapshotIds.add(table.currentSnapshot().snapshotId());
      tip = api.getDefaultBranch().getHash();
      soft.assertThat(tip).isNotEqualTo(head);
      head = tip;
      nessieSnapshotIds.add(fetchSnapshotId.applyAsLong(head));

      List<CommitMeta> commitLog =
          api.getCommitLog().refName("main").stream()
              .limit(icebergSnapshotIds.size())
              .map(LogResponse.LogEntry::getCommitMeta)
              .collect(Collectors.toList());
      Collections.reverse(commitLog);

      List<IcebergTable> contents =
          commitLog.stream()
              .map(
                  m -> {
                    try {
                      return api.getContent()
                          .refName("main")
                          .hashOnRef(m.getHash())
                          .getSingle(key)
                          .getContent();
                    } catch (NessieNotFoundException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .map(IcebergTable.class::cast)
              .collect(Collectors.toList());

      soft.assertThat(contents)
          .extracting(IcebergTable::getSnapshotId)
          .containsExactlyElementsOf(icebergSnapshotIds);
      soft.assertThat(icebergSnapshotIds).containsExactlyElementsOf(nessieSnapshotIds);

      for (int i = 0; i < commitLog.size(); i++) {
        CommitMeta commit = commitLog.get(i);
        IcebergTable c = contents.get(i);

        String tableSpec = format("`%s@main#%s`", tableName, commit.getHash());
        table = catalog.loadTable(TableIdentifier.of(NS, tableSpec));
        soft.assertThat(table)
            .describedAs("using branch + commit ID #%d, table '%s'", i, tableSpec)
            .extracting(
                t -> t.currentSnapshot() != null ? t.currentSnapshot().snapshotId() : -1L,
                t -> t.properties().get("nessie.commit.id"))
            .containsExactly(c.getSnapshotId(), commit.getHash());

        tableSpec = format("`%s@main#%s`", tableName, commit.getCommitTime());
        table = catalog.loadTable(TableIdentifier.of(NS, tableSpec));
        soft.assertThat(table)
            .describedAs("using branch + commit timestamp #%d, table '%s'", i, tableSpec)
            .extracting(
                t -> t.currentSnapshot() != null ? t.currentSnapshot().snapshotId() : -1L,
                t -> t.properties().get("nessie.commit.id"))
            .containsExactly(c.getSnapshotId(), commit.getHash());

        tableSpec = format("`%s#%s`", tableName, commit.getHash());
        table = catalog.loadTable(TableIdentifier.of(NS, tableSpec));
        soft.assertThat(table)
            .describedAs("using commit ID #%d, table '%s'", i, tableSpec)
            .extracting(
                t -> t.currentSnapshot() != null ? t.currentSnapshot().snapshotId() : -1L,
                t -> t.properties().get("nessie.commit.id"))
            .containsExactly(c.getSnapshotId(), commit.getHash());

        tableSpec = format("`%s#%s`", tableName, commit.getCommitTime());
        table = catalog.loadTable(TableIdentifier.of(NS, tableSpec));
        soft.assertThat(table)
            .describedAs("using commit timestamp #%d, table '%s'", i, tableSpec)
            .extracting(
                t -> t.currentSnapshot() != null ? t.currentSnapshot().snapshotId() : -1L,
                t -> t.properties().get("nessie.commit.id"))
            .containsExactly(c.getSnapshotId(), commit.getHash());

        tableSpec =
            format(
                "`%s#%s`",
                tableName,
                ISO_OFFSET_DATE_TIME.format(
                    commit.getCommitTime().atOffset(ZoneOffset.ofHours(-9))));
        table = catalog.loadTable(TableIdentifier.of(NS, tableSpec));
        soft.assertThat(table)
            .describedAs("using commit timestamp #%d, table '%s'", i, tableSpec)
            .extracting(
                t -> t.currentSnapshot() != null ? t.currentSnapshot().snapshotId() : -1L,
                t -> t.properties().get("nessie.commit.id"))
            .containsExactly(c.getSnapshotId(), commit.getHash());
      }
    }
  }

  @Test
  public void testTableWithObjectStorage() throws Exception {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TABLE.namespace());
    }

    Map<String, String> properties = singletonMap("write.object-storage.enabled", "true");
    Table originalTable =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .create();

    originalTable.newFastAppend().appendFile(FILE_A).commit();

    Table table = catalog.loadTable(TABLE);

    assertThat(table.properties())
        .containsKey("write.data.path")
        .doesNotContainKeys("write.object-storage.path", "write.folder-storage.path");
    String writeDataPath = table.properties().get("write.data.path");
    assertThat(table.location()).startsWith(writeDataPath);

    String filename = "my-data-file.txt";
    String dataLocation = table.locationProvider().newDataLocation(filename);

    assertThat(dataLocation).startsWith(writeDataPath + "/");
    String path = dataLocation.substring(writeDataPath.length() + 1);
    // Before Iceberg 1.7.0:
    //   dataLocation ==
    // s3://bucket1/warehouse/iI5Yww/newdb/table_5256a122-69b3-4ec2-a6ce-f98f9ce509bf/my-data-file.txt
    //           path == iI5Yww/newdb/table_5256a122-69b3-4ec2-a6ce-f98f9ce509bf/my-data-file.txt
    //   pathNoRandom == newdb/table_5256a122-69b3-4ec2-a6ce-f98f9ce509bf/my-data-file.txt
    // Since Iceberg 1.7.0:
    //   dataLocation ==
    // s3://bucket1/warehouse/1000/1000/1110/10001000/newdb/table_949afb2c-ed93-4702-b390-f1d4a9c59957/my-data-file.txt
    //           path ==
    // 1000/1000/1110/10001000/newdb/table_949afb2c-ed93-4702-b390-f1d4a9c59957/my-data-file.txt
    Pattern patternNewObjectStorageLayout = Pattern.compile("[01]{4}/[01]{4}/[01]{4}/[01]{8}/(.*)");
    Matcher matcherNewObjectStorageLayout = patternNewObjectStorageLayout.matcher(path);
    String pathNoRandom;
    if (matcherNewObjectStorageLayout.find()) {
      pathNoRandom = matcherNewObjectStorageLayout.group(1);
    } else {
      int idx = path.indexOf('/');
      assertThat(idx).isGreaterThan(1);
      pathNoRandom = path.substring(idx + 1);
    }
    assertThat(pathNoRandom)
        .startsWith(String.join("/", TABLE.namespace().levels()) + '/' + TABLE.name() + '_')
        .endsWith('/' + filename);

    try (PositionOutputStream out = table.io().newOutputFile(dataLocation).create()) {
      out.write("Hello World".getBytes(UTF_8));
    }
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

    String metadataLocation;
    try (FileIO io = temporaryFileIO(catalog, ops.io())) {
      metadataLocation = temporaryLocation() + "/my-metadata-" + UUID.randomUUID() + ".json";
      try (OutputStream output = io.newOutputFile(metadataLocation).create()) {
        output.write(TableMetadataParser.toJson(ops.current()).getBytes(StandardCharsets.UTF_8));
      }
    }

    catalog.dropTable(TABLE, false /* do not purge */);

    Table registeredTable = catalog.registerTable(TABLE, metadataLocation);

    assertThat(registeredTable).isNotNull();
    assertThat(catalog.tableExists(TABLE)).as("Table must exist").isTrue();
    assertThat(registeredTable.properties())
        .as("Props must match")
        .containsAllEntriesOf(properties);
    assertThat(registeredTable.schema().asStruct())
        .as("Schema must match")
        .isEqualTo(originalTable.schema().asStruct());
    assertThat(registeredTable.specs()).as("Specs must match").isEqualTo(originalTable.specs());
    assertThat(registeredTable.sortOrders())
        .as("Sort orders must match")
        .isEqualTo(originalTable.sortOrders());
    assertThat(registeredTable.currentSnapshot())
        .as("Current snapshot must match")
        .isEqualTo(originalTable.currentSnapshot());
    assertThat(registeredTable.snapshots())
        .as("Snapshots must match")
        .isEqualTo(originalTable.snapshots());
    assertThat(registeredTable.history())
        .as("History must match")
        .isEqualTo(originalTable.history());

    assertThat(catalog.loadTable(TABLE)).isNotNull();
    assertThat(catalog.dropTable(TABLE)).isTrue();
    assertThat(catalog.tableExists(TABLE)).isFalse();
  }

  protected FileIO temporaryFileIO(RESTCatalog catalog, FileIO io) {
    String ioImpl =
        catalog
            .properties()
            .getOrDefault(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
    Map<String, String> props = new HashMap<>(io.properties());
    props.put(S3FileIOProperties.REMOTE_SIGNING_ENABLED, "false");
    // dummy credentials - must be overridden if the test requires real credentials
    props.put(S3FileIOProperties.ACCESS_KEY_ID, "accessKey");
    props.put(S3FileIOProperties.SECRET_ACCESS_KEY, "secretKey");
    return CatalogUtil.loadFileIO(ioImpl, props, new Configuration(false));
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
    assertThat(loaded.schema().asStruct()).isEqualTo(expectedSchema.asStruct());
    assertThat(loaded.spec().fields()).isEqualTo(expectedSpec.fields());
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

    assertThat(catalog.loadTable(identifier1).schema().asStruct())
        .isEqualTo(expectedSchema.asStruct());

    assertThat(catalog.loadTable(identifier2).schema().asStruct())
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
    assertThat(schema1.asStruct()).isEqualTo(originalSchemaOne.asStruct());

    Schema schema2 = catalog.loadTable(identifier2).schema();
    assertThat(schema2.asStruct()).isEqualTo(updatedSchemaTwo.asStruct());
    assertThat(schema2.findField("data")).isNotNull();
    assertThat(schema2.findField("another")).isNotNull();
    assertThat(schema2.findField("more")).isNotNull();
    assertThat(schema2.findField("new-column")).isNull();
    assertThat(schema2.columns()).hasSize(4);
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

    table.updateStatistics().setStatistics(statisticsFile).commit();

    PartitionStatisticsFile partitionStatisticsFile =
        ImmutableGenericPartitionStatisticsFile.builder()
            .snapshotId(snapshotId)
            .path("/some/partition-statistics/file")
            .fileSizeInBytes(100)
            .build();

    table.updatePartitionStatistics().setPartitionStatistics(partitionStatisticsFile).commit();

    table = catalog.loadTable(TABLE);
    metadata = ((HasTableOperations) table).operations().current();
    assertThat(metadata.statisticsFiles()).containsAll(List.of(statisticsFile));
    assertThat(metadata.partitionStatisticsFiles()).containsAll(List.of(partitionStatisticsFile));

    table.updateStatistics().removeStatistics(snapshotId).commit();
    table.updatePartitionStatistics().removePartitionStatistics(snapshotId).commit();

    table = catalog.loadTable(TABLE);
    metadata = ((HasTableOperations) table).operations().current();
    assertThat(metadata.statisticsFiles()).isEmpty();
    assertThat(metadata.partitionStatisticsFiles()).isEmpty();
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

    transaction.updateStatistics().setStatistics(statisticsFile).commit();

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
    assertThat(metadata.statisticsFiles()).containsAll(List.of(statisticsFile));
    assertThat(metadata.partitionStatisticsFiles()).containsAll(List.of(partitionStatisticsFile));

    transaction = table.newTransaction();
    transaction.updateStatistics().removeStatistics(snapshotId).commit();
    transaction.updatePartitionStatistics().removePartitionStatistics(snapshotId).commit();
    transaction.commitTransaction();

    table = catalog.loadTable(TABLE);
    metadata = ((HasTableOperations) table).operations().current();
    assertThat(metadata.statisticsFiles()).isEmpty();
    assertThat(metadata.partitionStatisticsFiles()).isEmpty();
  }

  /** This test is based on {@link #testRegisterTable()} but uses a compressed metadata file. */
  @Test
  public void testRegisterCompressedTable() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    catalog.createNamespace(TABLE.namespace());

    Map<String, String> properties = Map.of(TableProperties.METADATA_COMPRESSION, "gzip");
    Table originalTable =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .create();

    originalTable.newFastAppend().appendFile(FILE_A).commit();
    originalTable.newFastAppend().appendFile(FILE_B).commit();
    originalTable.newDelete().deleteFile(FILE_A).commit();
    originalTable.newFastAppend().appendFile(FILE_C).commit();

    catalog.dropTable(TABLE, false /* do not purge */);

    TableOperations ops = ((BaseTable) originalTable).operations();
    String metadataLocation = ops.current().metadataFileLocation();

    String compressedLocation = metadataLocation + ".gz";
    try (FileIO io = originalTable.io()) {
      try (OutputStream os = io.newOutputFile(compressedLocation).create();
          GZIPOutputStream gzos = new GZIPOutputStream(os)) {
        try (InputStream is = io.newInputFile(metadataLocation).newStream()) {
          ByteStreams.copy(is, gzos);
        }
      }
    }

    Table registeredTable = catalog.registerTable(TABLE, compressedLocation);

    assertThat(registeredTable).isNotNull();
    assertThat(catalog.tableExists(TABLE)).as("Table must exist").isTrue();
    assertThat(registeredTable.properties())
        .as("Props must match")
        .containsAllEntriesOf(properties);
    assertThat(registeredTable.schema().asStruct())
        .as("Schema must match")
        .isEqualTo(originalTable.schema().asStruct());
    assertThat(registeredTable.specs()).as("Specs must match").isEqualTo(originalTable.specs());
    assertThat(registeredTable.sortOrders())
        .as("Sort orders must match")
        .isEqualTo(originalTable.sortOrders());
    assertThat(registeredTable.currentSnapshot())
        .as("Current snapshot must match")
        .isEqualTo(originalTable.currentSnapshot());
    assertThat(registeredTable.snapshots())
        .as("Snapshots must match")
        .isEqualTo(originalTable.snapshots());
    assertThat(registeredTable.history())
        .as("History must match")
        .isEqualTo(originalTable.history());

    TestHelpers.assertSameSchemaMap(registeredTable.schemas(), originalTable.schemas());
    assertFiles(registeredTable, FILE_B, FILE_C);

    registeredTable.newFastAppend().appendFile(FILE_A).commit();
    assertFiles(registeredTable, FILE_B, FILE_C, FILE_A);

    assertThat(catalog.loadTable(TABLE)).isNotNull();
    assertThat(catalog.dropTable(TABLE)).isTrue();
    assertThat(catalog.tableExists(TABLE)).isFalse();
  }
}
