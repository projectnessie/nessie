/*
 * Copyright (C) 2022 Dremio
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
package org.apache.iceberg.nessie;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.CommentUpdate;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewDefinition;
import org.apache.iceberg.view.ViewUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.ext.NessieUri;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Reference;

public class TestNessieIcebergViews extends BaseIcebergTest {

  private static final String BRANCH = "iceberg-view-test";
  private static final String SQL = "select id from tbl";
  private static final String SQL_ALTERED = "select id, data from tbl";

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  public static final String CATALOG_NAME = "nessie";
  private static final TableIdentifier VIEW_IDENTIFIER =
      TableIdentifier.of(CATALOG_NAME, DB_NAME, "view_name");
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final Schema SCHEMA =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());

  private Path tableLocation;

  public TestNessieIcebergViews() {
    super(BRANCH);
  }

  @Override
  @BeforeEach
  public void beforeEach(@NessieUri URI uri) throws IOException {
    super.beforeEach(uri);
    Table table = catalog.createTable(TABLE_IDENTIFIER, SCHEMA);
    this.tableLocation = new Path(table.location());
    catalog.create(
        VIEW_IDENTIFIER.toString(),
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, new ArrayList<>()),
        Collections.emptyMap());
  }

  @Override
  @AfterEach
  public void afterEach() throws Exception {
    // drop the table data
    if (tableLocation != null) {
      tableLocation.getFileSystem(hadoopConfig).delete(tableLocation, true);
      catalog.refresh();
      catalog.dropTable(TABLE_IDENTIFIER, false);
      catalog.drop(VIEW_IDENTIFIER.toString());
    }

    super.afterEach();
  }

  @Test
  public void testCreateView() throws IOException {
    ViewDefinition viewDefinition = ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, new ArrayList<>());
    TableIdentifier viewIdentifier = ViewUtils.toCatalogTableIdentifier(VIEW_IDENTIFIER + "x");

    catalog.create(viewIdentifier.toString(), viewDefinition, Collections.emptyMap());
    View icebergView = catalog.load(viewIdentifier.toString());
    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(viewDefinition);
    assertThat(Paths.get(metadataLocationViews(viewIdentifier.name()))).exists();
    assertThat(metadataFilesForViews(viewIdentifier.name())).isNotNull().hasSize(1);

    verifyCommitMetadata();
    assertThat(api.getCommitLog().refName(BRANCH).get().getLogEntries()).hasSize(3);

    verifyViewInNessie(viewIdentifier, icebergView, BRANCH);
  }

  @Test
  public void testReplaceView() throws NessieNotFoundException {
    // add a column
    catalog
        .loadTable(TABLE_IDENTIFIER)
        .updateSchema()
        .addColumn("mother", Types.LongType.get())
        .commit();

    // update schema
    Schema schema = catalog.loadTable(TABLE_IDENTIFIER).schema();
    ViewDefinition updatedView = ViewDefinition.of(SQL, schema, CATALOG_NAME, new ArrayList<>());
    catalog.replace(VIEW_IDENTIFIER.toString(), updatedView, Collections.emptyMap());
    View icebergView = catalog.load(VIEW_IDENTIFIER.toString());
    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(2);
    assertThat(icebergView.currentVersion().parentId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(updatedView);
    assertThat(icebergView.properties()).isEmpty();
    assertThat(Paths.get(metadataLocationViews(VIEW_IDENTIFIER.name()))).exists();
    assertThat(metadataFilesForViews(VIEW_IDENTIFIER.name())).isNotNull().hasSize(2);
    verifyViewInNessie(VIEW_IDENTIFIER, icebergView, BRANCH);

    // update sql
    ViewDefinition updatedSql =
        ViewDefinition.of(SQL_ALTERED, schema, CATALOG_NAME, new ArrayList<>());
    catalog.replace(VIEW_IDENTIFIER.toString(), updatedSql, Collections.emptyMap());
    icebergView = catalog.load(VIEW_IDENTIFIER.toString());
    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(3);
    assertThat(icebergView.currentVersion().parentId()).isEqualTo(2);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(updatedSql);
    assertThat(icebergView.properties()).isEmpty();
    assertThat(Paths.get(metadataLocationViews(VIEW_IDENTIFIER.name()))).exists();
    assertThat(metadataFilesForViews(VIEW_IDENTIFIER.name())).isNotNull().hasSize(3);
    verifyViewInNessie(VIEW_IDENTIFIER, icebergView, BRANCH);

    // update properties
    Map<String, String> properties = new HashMap<>();
    properties.put("prop1", "val1");
    properties.put("prop2", "val2");
    catalog.replace(
        VIEW_IDENTIFIER.toString(),
        ViewDefinition.of(SQL_ALTERED, schema, CATALOG_NAME, new ArrayList<>()),
        properties);
    icebergView = catalog.load(VIEW_IDENTIFIER.toString());
    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(4);
    assertThat(icebergView.currentVersion().parentId()).isEqualTo(3);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(updatedSql);
    assertThat(icebergView.properties()).isEqualTo(properties);
    assertThat(Paths.get(metadataLocationViews(VIEW_IDENTIFIER.name()))).exists();
    assertThat(metadataFilesForViews(VIEW_IDENTIFIER.name())).isNotNull().hasSize(4);

    verifyCommitMetadata();
    assertThat(api.getCommitLog().refName(BRANCH).get().getLogEntries()).hasSize(6);
    verifyViewInNessie(VIEW_IDENTIFIER, icebergView, BRANCH);
  }

  @Test
  public void testViewColumnComments() throws NessieNotFoundException {
    // update comment
    CommentUpdate commentUpdate =
        new CommentUpdate(catalog.getViewCatalog().newViewOps(VIEW_IDENTIFIER));
    String comment = "The column name is id";
    commentUpdate.updateColumnDoc("id", comment);
    Schema schema = commentUpdate.apply();
    assertThat(schema.findField("id").doc()).isEqualTo(comment);
    comment = comment + " and type is integer";
    commentUpdate.updateColumnDoc("id", comment);
    schema = commentUpdate.apply();
    assertThat(schema.findField("id").doc()).isEqualTo(comment);
    commentUpdate.commit();

    View icebergView = catalog.load(VIEW_IDENTIFIER.toString());
    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(2);
    assertThat(icebergView.currentVersion().parentId()).isEqualTo(1);
    assertThat(icebergView.properties()).isEmpty();
    assertThat(Paths.get(metadataLocationViews(VIEW_IDENTIFIER.name()))).exists();
    assertThat(metadataFilesForViews(VIEW_IDENTIFIER.name())).isNotNull().hasSize(2);

    verifyCommitMetadata();
    assertThat(api.getCommitLog().refName(BRANCH).get().getLogEntries()).hasSize(3);
    verifyViewInNessie(VIEW_IDENTIFIER, icebergView, BRANCH);
  }

  @Test
  public void testDropTableAndView() throws NessieNotFoundException {
    assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isTrue();
    assertThat(catalog.dropTable(TABLE_IDENTIFIER)).isTrue();
    assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isFalse();

    assertThat(catalog.load(VIEW_IDENTIFIER.toString())).isNotNull();
    catalog.drop(VIEW_IDENTIFIER.toString());
    assertThatThrownBy(() -> catalog.load(VIEW_IDENTIFIER.toString()))
        .isInstanceOf(NotFoundException.class)
        .hasMessage("View does not exist: " + VIEW_IDENTIFIER);
    verifyCommitMetadata();

    Map<ContentKey, Content> contentMap =
        api.getContent()
            .key(ContentKey.of(VIEW_IDENTIFIER.toString().split("\\.")))
            .refName(BRANCH)
            .get();
    assertThat(contentMap).isEmpty();
  }

  @Test
  public void testRenameViewNotImplemented() {
    assertThatThrownBy(
            () -> catalog.getViewCatalog().rename(VIEW_IDENTIFIER.toString(), "new_name"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testViewsAcrossBranches() throws NessieNotFoundException, NessieConflictException {
    View icebergView = catalog.load(VIEW_IDENTIFIER.toString());
    ViewDefinition viewDefinition = catalog.loadDefinition(VIEW_IDENTIFIER.toString());
    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(viewDefinition);
    assertThat(Paths.get(metadataLocationViews(VIEW_IDENTIFIER.name()))).exists();
    assertThat(metadataFilesForViews(VIEW_IDENTIFIER.name())).isNotNull().hasSize(1);
    verifyViewInNessie(VIEW_IDENTIFIER, icebergView, BRANCH);

    String branch1 = "branch1";
    Reference reference = api.getReference().refName(BRANCH).get();
    api.createReference()
        .reference(Branch.of(branch1, reference.getHash()))
        .sourceRefName(reference.getName())
        .create();
    NessieExtCatalog c = initCatalog(branch1);
    ViewDefinition updatedSql =
        ViewDefinition.of(SQL_ALTERED, SCHEMA, CATALOG_NAME, new ArrayList<>());

    c.replace(VIEW_IDENTIFIER.toString(), updatedSql, ImmutableMap.of());
    View updatedView = c.load(VIEW_IDENTIFIER + "@" + branch1);
    ViewDefinition updatedDefinition = c.loadDefinition(VIEW_IDENTIFIER.toString());
    assertThat(updatedView).isNotNull();
    assertThat(updatedView.currentVersion().versionId()).isEqualTo(2);
    assertThat(updatedView.currentVersion().viewDefinition()).isEqualTo(updatedSql);
    assertThat(updatedView.versions()).hasSize(2);
    assertThat(updatedView.history()).hasSize(2);
    verifyViewInNessie(VIEW_IDENTIFIER, updatedView, branch1);
    assertThat(updatedDefinition.sql()).isEqualTo(SQL_ALTERED);

    IcebergView original = getIcebergView(VIEW_IDENTIFIER, BRANCH);
    IcebergView updated = getIcebergView(VIEW_IDENTIFIER, branch1);
    assertThat(updated).isNotEqualTo(original);
    assertThat(updated.getMetadataLocation()).isEqualTo(original.getMetadataLocation());
    assertThat(original.getVersionId()).isEqualTo(1);
    assertThat(updated.getVersionId()).isEqualTo(2);
    assertThat(original.getSqlText()).isEqualTo(SQL);
    assertThat(updated.getSqlText()).isEqualTo(SQL_ALTERED);

    // original view on the other branch should still have the same version ID/view definition
    View originalView = c.load(VIEW_IDENTIFIER + "@" + BRANCH);
    assertThat(originalView.currentVersion().versionId()).isEqualTo(1);
    assertThat(originalView.currentVersion().viewDefinition()).isEqualTo(viewDefinition);
    assertThat(originalView.versions()).hasSize(1);
    assertThat(originalView.history()).hasSize(1);
  }

  private void verifyCommitMetadata() throws NessieNotFoundException {
    // check that the author is properly set
    List<LogEntry> log = api.getCommitLog().refName(BRANCH).get().getLogEntries();
    assertThat(log)
        .isNotNull()
        .isNotEmpty()
        .allSatisfy(
            logEntry -> {
              CommitMeta commit = logEntry.getCommitMeta();
              assertThat(commit.getAuthor()).isNotNull().isNotEmpty();
              assertThat(commit.getAuthor()).isEqualTo(System.getProperty("user.name"));
              assertThat(commit.getProperties().get(NessieUtil.APPLICATION_TYPE))
                  .isEqualTo("iceberg");
              assertThat(commit.getMessage()).startsWith("Iceberg");
            });
  }

  private void verifyViewInNessie(TableIdentifier viewIdentifier, View icebergView, String branch)
      throws NessieNotFoundException {
    IcebergView view = getIcebergView(viewIdentifier, branch);
    assertThat(metadataFilesForViewsPath(viewIdentifier.name()))
        .contains(view.getMetadataLocation());
    // TODO: currently the schema id is always 0
    assertThat(view.getSchemaId())
        .isEqualTo(icebergView.currentVersion().viewDefinition().schema().schemaId());
    assertThat(view.getVersionId()).isEqualTo(view.getVersionId());
    assertThat(view.getSqlText()).isEqualTo(icebergView.currentVersion().viewDefinition().sql());
    // TODO: currently not implemented in the view definition
    // assertThat(view.getDialect()).isEqualTo(viewDefinition.dialect());
  }

  private IcebergView getIcebergView(TableIdentifier viewIdentifier, String branch)
      throws NessieNotFoundException {
    ContentKey contentKey = ContentKey.of(viewIdentifier.toString().split("\\."));
    Map<ContentKey, Content> contentMap = api.getContent().key(contentKey).refName(branch).get();
    assertThat(contentMap).hasSize(1).containsKey(contentKey);
    Content content = contentMap.get(contentKey);
    assertThat(content.unwrap(IcebergView.class)).isPresent();
    IcebergView view = content.unwrap(IcebergView.class).get();
    return view;
  }

  private String getTableBasePath(String tableName) {
    String databasePath = temp.toString() + "/" + DB_NAME;
    return Paths.get(databasePath, tableName).toAbsolutePath().toString();
  }

  private String getViewBasePath(String viewName) {
    return Paths.get(temp.toString() + "/" + CATALOG_NAME + "." + DB_NAME, viewName)
        .toAbsolutePath()
        .toString();
  }

  private String metadataLocationViews(String viewName) {
    return Paths.get(getViewBasePath(viewName), "metadata").toString();
  }

  private List<String> metadataFilesForViews(String viewName) {
    return Arrays.stream(new File(metadataLocationViews(viewName)).listFiles())
        .map(File::getAbsolutePath)
        .filter(f -> f.endsWith(".metadata.json"))
        .collect(Collectors.toList());
  }

  private List<String> metadataFilesForViewsPath(String viewName) {
    return metadataFilesForViews(viewName).stream()
        .map(x -> String.format("file://%s", x))
        .collect(Collectors.toList());
  }

  private String metadataLocation(String tableName) {
    return Paths.get(getTableBasePath(tableName), "metadata").toString();
  }
}
