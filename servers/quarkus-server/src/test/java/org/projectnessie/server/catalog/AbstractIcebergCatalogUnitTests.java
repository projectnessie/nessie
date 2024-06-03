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
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.ArrayType;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTSerializers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.objectstoragemock.HeapStorageBucket;

public abstract class AbstractIcebergCatalogUnitTests extends AbstractIcebergCatalogTests {

  HeapStorageBucket heapStorageBucket;

  @BeforeEach
  public void clearBucket() {
    heapStorageBucket.clear();
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

    Function<URI, ObjectNode> httpGet =
        uri -> {
          try {
            return mapper.readValue(uri.toURL(), ObjectNode.class);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    ArrayType namespacesType = mapper.getTypeFactory().constructArrayType(Namespace.class);
    ArrayType tablesType = mapper.getTypeFactory().constructArrayType(TableIdentifier.class);

    List<Namespace> namespaces = new ArrayList<>();
    List<TableIdentifier> tables = new ArrayList<>();
    List<TableIdentifier> views = new ArrayList<>();

    // The following can be simplified when Iceberg 1.6.0 is released, and the List*Response types
    // contain the 'nextPageToken' field.

    for (String token = ""; true; ) {
      ObjectNode resp = httpGet.apply(listNamespaces.apply(token));
      Arrays.stream((Object[]) mapper.readValue(resp.get("namespaces").toString(), namespacesType))
          .map(Namespace.class::cast)
          .forEach(namespaces::add);
      String nextToken =
          Optional.ofNullable(resp.get("next-page-token")).map(JsonNode::asText).orElse(null);
      if (nextToken == null || nextToken.isEmpty()) {
        break;
      }
      token = nextToken;
    }

    for (String token = ""; true; ) {
      ObjectNode resp = httpGet.apply(listTables.apply(token));
      Arrays.stream((Object[]) mapper.readValue(resp.get("identifiers").toString(), tablesType))
          .map(TableIdentifier.class::cast)
          .forEach(tables::add);
      String nextToken =
          Optional.ofNullable(resp.get("next-page-token")).map(JsonNode::asText).orElse(null);
      if (nextToken == null || nextToken.isEmpty()) {
        break;
      }
      token = nextToken;
    }

    for (String token = ""; true; ) {
      ObjectNode resp = httpGet.apply(listViews.apply(token));
      Arrays.stream((Object[]) mapper.readValue(resp.get("identifiers").toString(), tablesType))
          .map(TableIdentifier.class::cast)
          .forEach(views::add);
      String nextToken =
          Optional.ofNullable(resp.get("next-page-token")).map(JsonNode::asText).orElse(null);
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
}
