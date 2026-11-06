/*
 * Copyright (C) 2026 Dremio
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
import java.util.Set;
import java.util.TreeSet;
import org.apache.iceberg.CatalogProperties;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.gc.tool.cli.util.RunCLI;
import org.projectnessie.spark.extensions.SparkSqlTestBase;
import org.projectnessie.testing.floci.s3.FlociS3;
import org.projectnessie.testing.floci.s3.FlociS3Access;
import org.projectnessie.testing.floci.s3.FlociS3Extension;

/**
 * Nessie GC with the sweep options {@code --target-namespace} and {@code --storage-prefix}, end to
 * end with Nessie, Spark, Iceberg and S3.
 *
 * <p>The catalog has the tables {@code team_a.orders}, {@code team_b.events} and {@code
 * team_b.customers}. The latter was created as {@code team_a.customers} and renamed into {@code
 * team_b}, its files are still in the folder {@code team_a/customers}.
 */
@ExtendWith({FlociS3Extension.class, SoftAssertionsExtension.class})
public class ITSparkIcebergNessieCLIFilters extends SparkSqlTestBase {

  public static final String JDBC_URL =
      "jdbc:h2:mem:gc_int_test_filters;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";

  /** Only the newest commits of 'main' are live, the older versions of all tables are not. */
  private static final String CUTOFF = "main=6";

  @InjectSoftAssertions private SoftAssertions soft;

  @FlociS3 static FlociS3Access flociS3;

  @TempDir Path tempDir;

  @Override
  protected String warehouseURI() {
    return tempDir.toUri().toString();
  }

  @Override
  protected Map<String, String> sparkHadoop() {
    Map<String, String> r = new HashMap<>();
    r.put("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    r.put("fs.s3a.access.key", flociS3.accessKey());
    r.put("fs.s3a.secret.key", flociS3.secretKey());
    r.put("fs.s3a.endpoint", flociS3.s3endpoint());
    return r;
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    r.putAll(flociS3.icebergProperties());

    System.setProperty("aws.region", "us-east-1");
    System.setProperty("aws.s3.endpoint", flociS3.s3endpoint());
    System.setProperty("aws.s3.accessKey", flociS3.accessKey());
    System.setProperty("aws.s3.secretAccessKey", flociS3.secretKey());

    return r;
  }

  @Test
  public void restrictedSweeps(@TempDir Path workDir) throws Exception {
    String tables = format("s3://%s/tables", flociS3.bucket());

    sql("CREATE NAMESPACE nessie.team_a");
    sql("CREATE NAMESPACE nessie.team_b");

    createAndFill("nessie.team_a.orders", tables + "/team_a/orders", 6);
    createAndFill("nessie.team_a.customers", tables + "/team_a/customers", 6);
    createAndFill("nessie.team_b.events", tables + "/team_b/events", 6);

    // Rename customers into team_b: the files of the table stay in the folder of team_a
    sql("ALTER TABLE nessie.team_a.customers RENAME TO nessie.team_b.customers");
    for (int val = 100; val < 103; val++) {
      sql("INSERT INTO nessie.team_b.customers VALUES (%d, 'string_%d')", val, val);
    }

    soft.assertThat(runCli("create-sql-schema").getExitCode()).isEqualTo(0);

    // Without options, all tables have files that GC wants to delete
    soft.assertThat(deferredDeletes(workDir, "all"))
        .as("no option")
        .containsExactly("customers", "events", "orders");

    // team_a selects orders, and customers: its references with the key in team_a are still live
    soft.assertThat(deferredDeletes(workDir, "ns-a", "--target-namespace", "team_a"))
        .as("--target-namespace team_a")
        .containsExactly("customers", "orders");

    // team_b selects events and customers, whose files are in the folder of team_a
    soft.assertThat(deferredDeletes(workDir, "ns-b", "--target-namespace", "team_b"))
        .as("--target-namespace team_b")
        .containsExactly("customers", "events");

    soft.assertThat(deferredDeletes(workDir, "ns-c", "--target-namespace", "team_c"))
        .as("--target-namespace team_c")
        .isEmpty();

    soft.assertThat(deferredDeletes(workDir, "ns-regex", "--target-namespace", "team_(a|b)"))
        .as("--target-namespace team_(a|b)")
        .containsExactly("customers", "events", "orders");

    // The folder of team_a contains the files of orders and, still, of customers
    soft.assertThat(deferredDeletes(workDir, "prefix-a", "--storage-prefix", tables + "/team_a/"))
        .as("--storage-prefix team_a")
        .containsExactly("customers", "orders");

    soft.assertThat(
            deferredDeletes(
                workDir, "prefix-b", "--storage-prefix", tables + "/team_b/,s3://other-bucket/"))
        .as("--storage-prefix team_b")
        .containsExactly("events");

    // A prefix is matched by whole path segments
    soft.assertThat(
            deferredDeletes(workDir, "prefix-partial", "--storage-prefix", tables + "/team_"))
        .as("--storage-prefix with a partial segment")
        .isEmpty();

    // Both must accept: customers is in team_b, and its files are in the folder of team_a
    soft.assertThat(
            deferredDeletes(
                workDir,
                "both",
                "--target-namespace",
                "team_b",
                "--storage-prefix",
                tables + "/team_a/"))
        .as("--target-namespace team_b --storage-prefix team_a")
        .containsExactly("customers");

    // Delete for real, for team_b only, then check that nothing in use has been deleted
    Path liveSetIdFile = workDir.resolve("live-set-id-delete.txt");
    RunCLI identify = identify(liveSetIdFile);
    soft.assertThat(identify.getExitCode()).as("%s", identify).isEqualTo(0);
    RunCLI expire =
        runCli(
            "expire",
            "--target-namespace",
            "team_b",
            "--read-live-set-id-from",
            liveSetIdFile.toString());
    soft.assertThat(expire.getExitCode()).as("%s", expire).isEqualTo(0);

    soft.assertThat(sql("SELECT count(*) FROM nessie.team_a.orders")).containsExactly(row(6L));
    soft.assertThat(sql("SELECT count(*) FROM nessie.team_b.customers")).containsExactly(row(9L));
    soft.assertThat(sql("SELECT count(*) FROM nessie.team_b.events")).containsExactly(row(6L));
  }

  private void createAndFill(String table, String location, int numInserts) {
    sql(
        "CREATE TABLE %s (some_int bigint, some_data string) USING iceberg LOCATION '%s'",
        table, location);
    for (int val = 0; val < numInserts; val++) {
      sql("INSERT INTO %s (some_int, some_data) VALUES (%d, 'string_%d')", table, val, val);
    }
  }

  private RunCLI identify(Path liveSetIdFile) throws Exception {
    return runCli(
        "identify", "--write-live-set-id-to", liveSetIdFile.toString(), "--cutoff", CUTOFF);
  }

  /**
   * Runs the mark phase and then the sweep phase with the given options, but only records the files
   * to delete.
   *
   * @return the names of the tables that have files to delete
   */
  private Set<String> deferredDeletes(Path workDir, String name, String... sweepOptions)
      throws Exception {
    Path liveSetIdFile = workDir.resolve("live-set-id-" + name + ".txt");

    RunCLI identify = identify(liveSetIdFile);
    soft.assertThat(identify.getExitCode()).as("identify for %s: %s", name, identify).isEqualTo(0);

    List<String> expireArgs = new ArrayList<>(Arrays.asList(sweepOptions));
    expireArgs.add("--defer-deletes");
    expireArgs.add("--read-live-set-id-from");
    expireArgs.add(liveSetIdFile.toString());
    RunCLI expire = runCli("expire", expireArgs.toArray(new String[0]));
    soft.assertThat(expire.getExitCode()).as("expire for %s: %s", name, expire).isEqualTo(0);

    RunCLI listDeferred =
        runCli("list-deferred", "--read-live-set-id-from", liveSetIdFile.toString());
    soft.assertThat(listDeferred.getExitCode())
        .as("list-deferred for %s: %s", name, listDeferred)
        .isEqualTo(0);

    Set<String> tablesWithDeletes = new TreeSet<>();
    for (String line : listDeferred.getOut().split("\n")) {
      if (line.contains("/tables/team_a/orders")) {
        tablesWithDeletes.add("orders");
      } else if (line.contains("/tables/team_a/customers")) {
        tablesWithDeletes.add("customers");
      } else if (line.contains("/tables/team_b/events")) {
        tablesWithDeletes.add("events");
      }
    }
    return tablesWithDeletes;
  }

  RunCLI runCli(String command, String... options) throws Exception {
    List<String> args = new ArrayList<>();

    args.add(command);

    URI nessieUriV2 = new URI(nessieApiUri()).resolve("v2");

    switch (command) {
      case "identify", "mark-live" -> {
        args.add("--uri");
        args.add(nessieUriV2.toASCIIString());
      }
      case "sweep", "expire", "deferred-deletes" ->
          flociS3
              .icebergProperties()
              .forEach(
                  (k, v) -> {
                    args.add("--iceberg");
                    args.add(k + "=" + v);
                  });
      default -> {}
    }

    args.add("--jdbc-url");
    args.add(JDBC_URL);

    args.addAll(Arrays.asList(options));

    return RunCLI.run(args);
  }
}
