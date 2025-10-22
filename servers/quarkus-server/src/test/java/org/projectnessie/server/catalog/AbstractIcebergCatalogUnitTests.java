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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import io.quarkus.test.common.WithTestResource;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTSerializers;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.server.catalog.ObjectStorageMockTestResourceLifecycleManager.AccessCheckHandlerHolder;

@WithTestResource(IcebergResourceLifecycleManager.ForUnitTests.class)
public abstract class AbstractIcebergCatalogUnitTests extends AbstractIcebergCatalogTests {

  HeapStorageBucket heapStorageBucket;
  AccessCheckHandlerHolder accessCheckHandler;

  @BeforeEach
  public void clearBucket() {
    heapStorageBucket.clear();
  }

  @Test
  public void partitioningNestedStruct() {
    @SuppressWarnings("resource")
    var catalog = catalog();

    var namespace = Namespace.of("test_ns");
    var table = TableIdentifier.of(namespace, "table");

    catalog.createNamespace(namespace);

    var headersKv =
        Types.StructType.of(
            Types.NestedField.optional(7, "key", Types.BinaryType.get()),
            Types.NestedField.optional(8, "value", Types.BinaryType.get()));

    var fieldTimestamp =
        Types.NestedField.required(4, "timestamp", Types.TimestampType.withoutZone());
    var systemFields =
        Types.StructType.of(
            Types.NestedField.required(2, "partition", Types.IntegerType.get()),
            Types.NestedField.required(3, "offset", Types.LongType.get()),
            fieldTimestamp,
            Types.NestedField.required(5, "headers", Types.ListType.ofRequired(6, headersKv)),
            Types.NestedField.required(9, "key", Types.BinaryType.get()));

    var fieldTestSchema = Types.NestedField.required(1, "test_schema", systemFields);
    var schema = new Schema(List.of(fieldTestSchema));

    var sourceName = fieldTestSchema.name() + '.' + fieldTimestamp.name();
    var spec = PartitionSpec.builderFor(schema).day(sourceName, "daytime_day").build();

    soft.assertThatCode(() -> catalog.createTable(table, schema, spec)).doesNotThrowAnyException();
  }

  // Paging tests

  @Test
  public void namespacesPaging() throws Exception {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    assertThat(catalog.properties())
        .extractingByKey("rest-page-size", STRING)
        .asInt()
        .isGreaterThanOrEqualTo(3);

    int pageSize = 11;
    int items = 40;

    Namespace namespace0 = Namespace.of("namespace_0");

    for (int i = 0; i < items; i++) {
      catalog.createNamespace(Namespace.of("namespace_" + i));
    }
    for (int i = 0; i < items; i++) {
      catalog.createTable(TableIdentifier.of("namespace_0", "table_" + i), SCHEMA);
    }
    for (int i = 0; i < items; i++) {
      catalog
          .buildView(TableIdentifier.of("namespace_0", "view_" + i))
          .withSchema(SCHEMA)
          .withDefaultNamespace(namespace0)
          .withQuery("foo", "bar")
          .create();
    }

    assertThat(catalog.listNamespaces())
        .hasSize(items)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, items).mapToObj(i -> Namespace.of("namespace_" + i)).toList());
    assertThat(catalog.listTables(namespace0))
        .hasSize(items)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, items)
                .mapToObj(i -> TableIdentifier.of("namespace_0", "table_" + i))
                .toList());
    assertThat(catalog.listViews(namespace0))
        .hasSize(items)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, items)
                .mapToObj(i -> TableIdentifier.of("namespace_0", "view_" + i))
                .toList());

    Map<String, String> props = catalog.properties();
    URI baseUri = URI.create(props.get("uri"));
    String prefix = props.get("prefix");

    Function<String, URI> listNamespaces =
        pageToken ->
            baseUri.resolve(
                format(
                    "v1/%s/namespaces?parent=%s&pageSize=%d&pageToken=%s",
                    prefix, "", pageSize, pageToken));
    Function<String, URI> listTables =
        pageToken ->
            baseUri.resolve(
                format(
                    "v1/%s/namespaces/%s/tables?pageSize=%d&pageToken=%s",
                    prefix, "namespace_0", pageSize, pageToken));
    Function<String, URI> listViews =
        pageToken ->
            baseUri.resolve(
                format(
                    "v1/%s/namespaces/%s/views?pageSize=%d&pageToken=%s",
                    prefix, "namespace_0", pageSize, pageToken));

    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(mapper);

    List<Namespace> namespaces = new ArrayList<>();
    List<TableIdentifier> tables = new ArrayList<>();
    List<TableIdentifier> views = new ArrayList<>();

    // The following can be simplified when Iceberg 1.6.0 is released, and the List*Response types
    // contain the 'nextPageToken' field.

    for (String token = ""; true; ) {
      ListNamespacesResponse resp =
          readValue(mapper, listNamespaces.apply(token).toURL(), ListNamespacesResponse.class);
      namespaces.addAll(resp.namespaces());
      String nextToken = resp.nextPageToken();
      if (nextToken == null || nextToken.isEmpty()) {
        break;
      }
      token = nextToken;
    }

    for (String token = ""; true; ) {
      ListTablesResponse resp =
          readValue(mapper, listTables.apply(token).toURL(), ListTablesResponse.class);
      tables.addAll(resp.identifiers());
      String nextToken = resp.nextPageToken();
      if (nextToken == null || nextToken.isEmpty()) {
        break;
      }
      token = nextToken;
    }

    for (String token = ""; true; ) {
      ListTablesResponse resp =
          readValue(mapper, listViews.apply(token).toURL(), ListTablesResponse.class);
      views.addAll(resp.identifiers());
      String nextToken = resp.nextPageToken();
      if (nextToken == null || nextToken.isEmpty()) {
        break;
      }
      token = nextToken;
    }

    assertThat(namespaces)
        .hasSize(items)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, items).mapToObj(i -> Namespace.of("namespace_" + i)).toList());
    assertThat(tables)
        .hasSize(items)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, items)
                .mapToObj(i -> TableIdentifier.of("namespace_0", "table_" + i))
                .toList());
    assertThat(views)
        .hasSize(items)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, items)
                .mapToObj(i -> TableIdentifier.of("namespace_0", "view_" + i))
                .toList());
  }

  @Test
  void testStorageReadFailure() throws Exception {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    TableIdentifier id1 = TableIdentifier.of("ns", "table1");
    catalog.createNamespace(id1.namespace());
    Table table = catalog.buildTable(id1, SCHEMA).create();

    try (NessieApiV2 api = nessieClientBuilder().build(NessieApiV2.class)) {
      api.createNamespace().reference(api.getDefaultBranch()).namespace("test-ns").create();
      api.commitMultipleOperations()
          .commitMeta(CommitMeta.fromMessage("test"))
          .branch(api.getDefaultBranch())
          .operation(
              Operation.Put.of(
                  ContentKey.of("ns", "table2"),
                  IcebergTable.of(table.location() + "_test_access_denied_file", 1, 2, 3, 4)))
          .operation(
              Operation.Put.of(
                  ContentKey.of("ns", "table3"),
                  IcebergTable.of(table.location() + "_test_non_existent_file", 1, 2, 3, 4)))
          .commit();
    }

    accessCheckHandler.set(key -> !key.contains("_test_access_denied_"));

    assertThatThrownBy(() -> catalog.loadTable(TableIdentifier.of("ns", "table2")))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("_test_access_denied_file");

    assertThatThrownBy(() -> catalog.loadTable(TableIdentifier.of("ns", "table3")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("_test_non_existent_file");

    // Repeat these reads to make sure (stored task) errors are reported the same way
    assertThatThrownBy(() -> catalog.loadTable(TableIdentifier.of("ns", "table2")))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("_test_access_denied_file");

    assertThatThrownBy(() -> catalog.loadTable(TableIdentifier.of("ns", "table3")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("_test_non_existent_file");
  }

  @Test
  void testStorageWriteFailure() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    TableIdentifier id1 = TableIdentifier.of("ns", "table_access_denied");
    catalog.createNamespace(id1.namespace());

    accessCheckHandler.set(key -> !key.contains("table_access_denied"));
    assertThatThrownBy(() -> catalog.buildTable(id1, SCHEMA).create())
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("table_access_denied");
  }

  // Prevent deprecation warning for ObjectMapper.readValue(URL, Class<T>)
  static <T> T readValue(ObjectMapper mapper, URL url, Class<T> clazz) throws Exception {
    URLConnection conn = url.openConnection();
    try (var input = conn.getInputStream()) {
      return mapper.readValue(input, clazz);
    }
  }
}
