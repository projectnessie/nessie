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
package org.projectnessie.deltalake;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.FormatMethod;
import io.delta.sql.DeltaSparkSessionExtension;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.spark.extensions.NessieSpark32SessionExtensions;

public class AbstractDeltaTest {

  @TempDir File tempFile;

  NessieApiV1 api;

  private static final int NESSIE_PORT = Integer.getInteger("quarkus.http.test-port", 19121);
  protected static SparkConf conf = new SparkConf();

  protected static SparkSession spark;
  protected static String url = String.format("http://localhost:%d/api/v1", NESSIE_PORT);

  @BeforeEach
  protected void create() {

    conf.set("spark.sql.catalog.spark_catalog.ref", "main")
        .set("spark.sql.catalog.spark_catalog.uri", url)
        .set("spark.sql.catalog.spark_catalog.warehouse", tempFile.toURI().toString())
        .set("spark.sql.catalog.spark_catalog", DeltaCatalog.class.getCanonicalName())
        .set("spark.delta.logStore.class", NessieLogStore.class.getCanonicalName())
        .set("spark.delta.logFileHandler.class", NessieLogFileMetaParser.class.getCanonicalName())
        .set(
            "spark.sql.extensions",
            String.join(
                ",",
                Arrays.asList(
                    DeltaSparkSessionExtension.class.getCanonicalName(),
                    NessieSpark32SessionExtensions.class.getCanonicalName())))
        .set(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .set("spark.testing", "true")
        .set("spark.sql.shuffle.partitions", "4");

    spark = SparkSession.builder().master("local[2]").config(conf).getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    api = HttpClientBuilder.builder().withUri(url).build(NessieApiV1.class);
  }

  @AfterEach
  void removeBranches() throws NessieConflictException, NessieNotFoundException {
    for (Reference ref : api.getAllReferences().get().getReferences()) {
      if (ref instanceof Branch) {
        api.deleteBranch().branchName(ref.getName()).hash(ref.getHash()).delete();
      }
      if (ref instanceof Tag) {
        api.deleteTag().tagName(ref.getName()).hash(ref.getHash()).delete();
      }
    }
    api.createReference().reference(Branch.of("main", null)).create();
    api.close();
    api = null;
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  protected static List<Object[]> transform(Dataset<Row> table) {

    return table.collectAsList().stream()
        .map(
            row ->
                IntStream.range(0, row.size())
                    .mapToObj(pos -> row.isNullAt(pos) ? null : row.get(pos))
                    .toArray(Object[]::new))
        .collect(Collectors.toList());
  }

  protected static void assertEquals(
      String context, List<Object[]> expectedRows, List<Object[]> actualRows) {
    assertThat(actualRows).as("%s", context).containsExactlyElementsOf(expectedRows);
  }

  @FormatMethod
  protected static List<Object[]> sql(String query, Object... args) {
    List<Row> rows = spark.sql(String.format(query, args)).collectAsList();
    if (rows.size() < 1) {
      return ImmutableList.of();
    }

    return rows.stream().map(AbstractDeltaTest::toJava).collect(Collectors.toList());
  }

  protected static Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }

              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              } else {
                return value;
              }
            })
        .toArray(Object[]::new);
  }

  /**
   * This looks weird but it gives a clear semantic way to turn a list of objects into a 'row' for
   * spark assertions.
   */
  protected static Object[] row(Object... values) {
    return values;
  }
}
