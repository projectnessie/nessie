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
package org.projectnessie.gc.tool.inttest;

import static java.lang.String.format;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.gc.tool.cli.util.RunCLI;
import org.projectnessie.minio.Minio;
import org.projectnessie.minio.MinioAccess;
import org.projectnessie.minio.MinioExtension;
import org.projectnessie.spark.extensions.SparkSqlTestBase;

/** Full Nessie GC round trip test with Nessie, Spark, Iceberg, Minio. */
@ExtendWith({MinioExtension.class, SoftAssertionsExtension.class})
public class ITSparkIcebergNessieCLI extends SparkSqlTestBase {

  public static final String JDBC_URL = "jdbc:h2:mem:gc_int_test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";

  @InjectSoftAssertions private SoftAssertions soft;

  @Minio static MinioAccess minio;

  @TempDir Path tempDir;

  @Override
  protected String warehouseURI() {
    return tempDir.toUri().toString();
  }

  @Override
  protected Map<String, String> sparkHadoop() {
    Map<String, String> r = new HashMap<>();
    r.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    r.put("fs.s3a.access.key", minio.accessKey());
    r.put("fs.s3a.secret.key", minio.secretKey());
    r.put("fs.s3a.endpoint", minio.s3endpoint());
    return r;
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    r.putAll(minio.icebergProperties());

    System.setProperty("aws.region", "us-east-1");
    System.setProperty("aws.s3.endpoint", minio.s3endpoint());
    System.setProperty("aws.s3.accessKey", minio.accessKey());
    System.setProperty("aws.s3.secretAccessKey", minio.secretKey());

    return r;
  }

  @Test
  public void fullRoundTrip(@TempDir Path workDir) throws Exception {
    String tableUri = format("s3://%s/tables/foo", minio.bucket());

    sql(
        "CREATE TABLE nessie.foo (some_int bigint, some_data string) USING iceberg LOCATION '%s'",
        tableUri);

    for (int val = 0; val < 10; val++) {
      sql("INSERT INTO nessie.foo (some_int, some_data) VALUES (%d, 'string_%d')", val, val);
    }

    Path liveSetIdFile = workDir.resolve("live-set-id.txt");

    RunCLI createTables = runCli("create-sql-schema");
    soft.assertThat(createTables.getExitCode()).isEqualTo(0);

    RunCLI identify =
        runCli(
            "identify", "--write-live-set-id-to", liveSetIdFile.toString(), "--cutoff", "main=2");
    soft.assertThat(identify.getExitCode()).as("%s", identify).isEqualTo(0);

    RunCLI show =
        runCli(
            "show",
            "--with-base-locations",
            "--with-content-references",
            "--read-live-set-id-from",
            liveSetIdFile.toString());
    soft.assertThat(show.getExitCode()).as("%s", show).isEqualTo(0);

    RunCLI expire =
        runCli("expire", "--defer-deletes", "--read-live-set-id-from", liveSetIdFile.toString());
    soft.assertThat(expire.getExitCode()).as("%s", expire).isEqualTo(0);

    RunCLI listDeferred =
        runCli("list-deferred", "--read-live-set-id-from", liveSetIdFile.toString());
    soft.assertThat(listDeferred.getExitCode()).as("%s", listDeferred).isEqualTo(0);

    RunCLI delete = runCli("deferred-deletes", "--read-live-set-id-from", liveSetIdFile.toString());
    soft.assertThat(delete.getExitCode()).as("%s", delete).isEqualTo(0);

    soft.assertThat(
            Arrays.stream(listDeferred.getOut().split("\n")).filter(l -> l.contains(tableUri)))
        .isNotEmpty();
  }

  RunCLI runCli(String command, String... options) throws Exception {
    List<String> args = new ArrayList<>();

    args.add(command);

    URI nessieUriV2 = new URI(nessieApiUri()).resolve("v2");

    switch (command) {
      case "identify":
      case "mark-live":
        args.add("--uri");
        args.add(nessieUriV2.toASCIIString());
        break;
      case "sweep":
      case "expire":
      case "deferred-deletes":
        minio
            .icebergProperties()
            .forEach(
                (k, v) -> {
                  args.add("--iceberg");
                  args.add(k + "=" + v);
                });
        break;
      default:
        break;
    }

    args.add("--jdbc-url");
    args.add(JDBC_URL);

    args.addAll(Arrays.asList(options));

    return RunCLI.run(args);
  }
}
