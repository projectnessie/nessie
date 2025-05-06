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
package org.projectnessie.server.catalog.s3;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.projectnessie.minio.MinioContainer;
import org.projectnessie.server.catalog.Catalogs;
import org.projectnessie.server.catalog.MinioTestResourceLifecycleManager;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public abstract class AbstractAssumeRoleIceberg {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  private static final PartitionSpec PARTITION_SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("id", 16).build();

  private static final DataFile FILE_A =
      DataFiles.builder(PARTITION_SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("id_bucket=0")
          .withRecordCount(1)
          .build();

  @Language("JSON")
  private static final String IAM_POLICY =
      """
      { "Version":"2012-10-17",
        "Statement": [
          {"Effect":"Allow", "Action":"s3:*", "Resource":"arn:aws:s3:::*"},
          {"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/blockedNamespace/*"}
         ]
      }
      """;

  @Language("JSON")
  public static final String IAM_STATEMENT_1 =
      """
      {"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/blockedNamespace/*"}
      """;

  @Language("JSON")
  public static final String IAM_STATEMENT_2 =
      """
      {"Effect":"Deny", "Action":"s3:*", "Resource":"arn:aws:s3:::*/noAppend/*"}
      """;

  @SuppressWarnings("unused")
  // Injected by MinioTestResourceLifecycleManager
  private MinioContainer minio;

  private static final Catalogs CATALOGS = new Catalogs();

  RESTCatalog catalog() {
    return CATALOGS.getCatalog(
        Map.of(
            AwsClientProperties.CLIENT_REGION,
            MinioTestResourceLifecycleManager.TEST_REGION,
            CatalogProperties.WAREHOUSE_LOCATION,
            minio.s3BucketUri(scheme(), "").toString()));
  }

  protected abstract String scheme();

  @AfterAll
  static void closeRestCatalog() throws Exception {
    CATALOGS.close();
  }

  @Test
  public void testTableWithObjectStorage() throws Exception {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    String namespace = "testTableWithObjectStorage";
    Namespace ns = Namespace.of(namespace);
    catalog.createNamespace(ns);

    TableIdentifier tableId = TableIdentifier.of(ns, "table1");

    Map<String, String> properties = singletonMap("write.object-storage.enabled", "true");
    Table originalTable = catalog.buildTable(tableId, SCHEMA).withProperties(properties).create();

    originalTable.newFastAppend().appendFile(FILE_A).commit();

    Table table = catalog.loadTable(tableId);

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
        .startsWith(namespace + '/' + tableId.name() + '_')
        .endsWith('/' + filename);

    try (PositionOutputStream out = table.io().newOutputFile(dataLocation).create()) {
      out.write("Hello World".getBytes(UTF_8));
    }
  }

  @Test
  void testCreateTable() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace ns = Namespace.of("allowedNamespace");
    catalog.createNamespace(ns);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    // Create a table exercises assume role flows.
    catalog.createTable(TableIdentifier.of(ns, "table1"), schema);
  }

  @Test
  void testCreateTableForbidden() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace ns = Namespace.of("blockedNamespace");
    catalog.createNamespace(ns);

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    // Attempts to create files blocked by the server side IAM policy, breaks the createTable() call
    assertThatThrownBy(() -> catalog.createTable(TableIdentifier.of(ns, "table1"), schema))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("S3Exception: Access Denied")
        // make sure the error comes from the Catalog Server
        .hasStackTraceContaining("org.apache.iceberg.rest.HTTPClient");
  }

  @Test
  void testAppendForbidden() {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();

    Namespace ns = Namespace.of("noAppend");
    catalog.createNamespace(ns);

    Table table = catalog.createTable(TableIdentifier.of(ns, "table2"), SCHEMA);

    // Attempts to create snapshot files are blocked by the session IAM policy
    assertThatThrownBy(() -> table.newAppend().appendFile(FILE_A).commit())
        .hasMessageContaining("Access Denied")
        // make sure the error happens on the client side
        .hasStackTraceContaining("software.amazon.awssdk.services.s3.DefaultS3Client")
        .hasStackTraceContaining("org.apache.iceberg.SnapshotProducer");
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("nessie.catalog.service.s3.default-options.request-signing-enabled", "false")
          .put("nessie.catalog.service.s3.default-options.server-iam.enabled", "true")
          .put("nessie.catalog.service.s3.default-options.server-iam.policy", IAM_POLICY)
          .put("nessie.catalog.service.s3.default-options.client-iam.enabled", "true")
          .put(
              "nessie.catalog.service.s3.default-options.client-iam.statements[0]", IAM_STATEMENT_1)
          .put(
              "nessie.catalog.service.s3.default-options.client-iam.statements[1]", IAM_STATEMENT_2)
          .put(
              "nessie.catalog.service.s3.default-options.client-iam.assume-role",
              "test-role") // Note: unused by Minio
          .put(
              "nessie.catalog.service.s3.default-options.client-iam.external-id",
              "test-external-id") // Note: unused by Minio
          .build();
    }
  }
}
