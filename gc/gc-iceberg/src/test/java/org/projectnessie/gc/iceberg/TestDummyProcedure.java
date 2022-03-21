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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.jaxrs.ext.NessieUri;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;

@NessieDbAdapterName(InmemoryDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(InmemoryTestConnectionProviderSource.class)
@ExtendWith(DatabaseAdapterExtension.class)
public class TestDummyProcedure {

  @NessieDbAdapter static DatabaseAdapter databaseAdapter;

  @RegisterExtension
  static NessieJaxRsExtension server = new NessieJaxRsExtension(() -> databaseAdapter);

  private static URI nessieUri;

  @BeforeAll
  static void setNessieUri(@NessieUri URI uri) {
    nessieUri = uri;
  }

  @TempDir File tempDir;

  static final String CATALOG_NAME = "nessie";
  static final String GC_SPARK_CATALOG = "org.projectnessie.gc.iceberg.NessieIcebergGcSparkCatalog";
  static final String GC_SPARK_SESSION_CATALOG =
      "org.projectnessie.gc.iceberg.NessieIcebergGcSparkSessionCatalog";
  static final String SPARK_CATALOG = "org.apache.iceberg.spark.SparkCatalog";
  static final String SPARK_SESSION_CATALOG = "org.apache.iceberg.spark.SparkSessionCatalog";
  static final String GC_NAMESPACE = "nessie_gc";
  static final String OTHER_NAMESPACE = "system";
  static final HashSet<String> gcCatalog =
      new HashSet<>(Arrays.asList(GC_SPARK_CATALOG, GC_SPARK_SESSION_CATALOG));

  private SparkSession getSparkSessionWithCatalogClass(String catalogClass) {
    return ProcedureTestUtil.getSessionWithGcCatalog(
        nessieUri.toString(), tempDir.toURI().toString(), catalogClass);
  }

  @ParameterizedTest
  @CsvSource({
    GC_SPARK_CATALOG + "," + GC_NAMESPACE,
    GC_SPARK_CATALOG + "," + OTHER_NAMESPACE,
    GC_SPARK_SESSION_CATALOG + "," + GC_NAMESPACE,
    GC_SPARK_SESSION_CATALOG + "," + OTHER_NAMESPACE,
    SPARK_CATALOG + "," + GC_NAMESPACE,
    SPARK_CATALOG + "," + OTHER_NAMESPACE,
    SPARK_SESSION_CATALOG + "," + GC_NAMESPACE,
    SPARK_SESSION_CATALOG + "," + OTHER_NAMESPACE
  })
  public void testDummyProcedure(String catalog, String namespace) {
    try (SparkSession sparkSession = getSparkSessionWithCatalogClass(catalog)) {
      String inputString = "sample_string";
      if (gcCatalog.contains(catalog) && namespace.equals(GC_NAMESPACE)) {
        List<Row> rows = callProcedure(sparkSession, namespace, inputString);
        assertThat(rows.size()).isEqualTo(1);
        assertThat(rows.get(0).getString(0)).isEqualTo(inputString);
      } else {
        assertThatThrownBy(() -> callProcedure(sparkSession, namespace, inputString))
            .isInstanceOf(NoSuchProcedureException.class)
            .hasMessage(String.format("Procedure %s.dummy not found", namespace));
      }
    }
  }

  private List<Row> callProcedure(SparkSession sparkSession, String namespace, String inputString) {
    // Example query:
    // CALL nessie.nessie_gc.dummy(input_string => 'sample_string')
    return sparkSession
        .sql(
            String.format(
                "CALL %s.%s.%s(input_string => '%s')",
                CATALOG_NAME,
                namespace,
                DummyProcedure.PROCEDURE_NAME,
                //
                inputString))
        .collectAsList();
  }
}
