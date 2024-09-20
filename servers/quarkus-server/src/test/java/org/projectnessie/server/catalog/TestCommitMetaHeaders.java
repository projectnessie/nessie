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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.EMPTY_OBJ_ID;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.WAREHOUSE_NAME;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Reference;

@QuarkusTest
@TestProfile(S3UnitTestProfiles.S3UnitTestProfile.class)
public class TestCommitMetaHeaders {
  private static final Catalogs CATALOGS = new Catalogs();

  protected static final Namespace NS = Namespace.of("newdb");
  protected static final TableIdentifier TABLE = TableIdentifier.of(NS, "table");
  protected static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID 🤪"),
          required(4, "data", Types.StringType.get()));
  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("id", 16).build();
  protected static final SortOrder WRITE_ORDER =
      SortOrder.builderFor(SCHEMA).asc(Expressions.bucket("id", 16)).asc("id").build();

  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

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
        .withUri(String.format("http://127.0.0.1:%d/api/v2/", catalogServerPort));
  }

  @Test
  public void testCommitMetaAll() throws Exception {
    String author = "Me <me@me.internal>";
    String message = "My own commit message";
    String signedOffBy = "Myself <myself@me.internal>";

    RESTCatalog catalog =
        CATALOGS.getCatalog(
            Map.of(
                CatalogProperties.WAREHOUSE_LOCATION,
                WAREHOUSE_NAME,
                "header.Nessie-Commit-Message",
                message,
                "header.Nessie-Commit-Authors",
                author,
                "header.Nessie-Commit-SignedOffBy",
                signedOffBy,
                "header.Nessie-Commit-Property-foo",
                "bar",
                "header.Nessie-Commit-Property-dog",
                "Elani"));

    try (NessieApiV2 api = nessieClientBuilder().build(NessieApiV2.class)) {
      catalog.createNamespace(NS);

      // "Create ICEBERG_TABLE" commit
      Table table =
          catalog
              .buildTable(TABLE, SCHEMA)
              .withPartitionSpec(SPEC)
              .withSortOrder(WRITE_ORDER)
              .create();
      table.newFastAppend().commit();

      List<LogResponse.LogEntry> commits = api.getCommitLog().refName("main").stream().toList();
      soft.assertThat(commits)
          .extracting(LogResponse.LogEntry::getCommitMeta)
          .allSatisfy(
              meta -> {
                soft.assertThat(meta.getAllAuthors()).containsExactlyElementsOf(List.of(author));
                soft.assertThat(meta.getAllSignedOffBy())
                    .containsExactlyElementsOf(List.of(signedOffBy));
                soft.assertThat(meta.getMessage()).isEqualTo(message);
                soft.assertThat(meta.getAllProperties())
                    .containsExactlyInAnyOrderEntriesOf(
                        Map.of("foo", List.of("bar"), "dog", List.of("Elani")));
              });
    }
  }
}
