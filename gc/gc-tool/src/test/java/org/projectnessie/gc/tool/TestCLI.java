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
package org.projectnessie.gc.tool;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.BufferedReader;
import java.io.StringReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.ext.NessieUri;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.jdbc.AgroalJdbcDataSourceProvider;
import org.projectnessie.gc.contents.jdbc.JdbcPersistenceSpi;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.tool.cli.util.RunCLI;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;

@ExtendWith({DatabaseAdapterExtension.class, SoftAssertionsExtension.class})
@NessieDbAdapterName(InmemoryDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(InmemoryTestConnectionProviderSource.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestCLI {

  public static final String JDBC_URL = "jdbc:h2:mem:nessie_gc;DB_CLOSE_DELAY=-1";
  @NessieDbAdapter static DatabaseAdapter databaseAdapter;

  @InjectSoftAssertions private SoftAssertions soft;

  @RegisterExtension
  static NessieJaxRsExtension server = new NessieJaxRsExtension(() -> databaseAdapter);

  private static URI nessieUri;

  @BeforeAll
  static void setNessieUri(@NessieUri URI uri) {
    nessieUri = uri;
  }

  static Stream<Arguments> optionErrors() {
    return Stream.of(
        // missing contents-storage option
        arguments(
            singletonList("gc"),
            "Error: Missing required argument (specify one of these): ([--inmemory] | [--jdbc-url="),
        arguments(
            singletonList("mark-live"),
            "Error: Missing required argument (specify one of these): ([--inmemory] | [--jdbc-url="),
        arguments(
            asList("sweep", "--live-set-id=00000000-0000-0000-0000-000000000000"),
            "Error: Missing required argument (specify one of these): ([--inmemory] | [--jdbc-url="),
        arguments(
            asList("deferred-deletes", "--live-set-id=00000000-0000-0000-0000-000000000000"),
            "Error: Missing required argument (specify one of these): ([--inmemory] | [--jdbc-url="),
        arguments(
            asList("list-deferred", "--live-set-id=00000000-0000-0000-0000-000000000000"),
            "Error: Missing required argument (specify one of these): ([--inmemory] | [--jdbc-url="),
        // incomplete jdbc auth
        arguments(
            asList("mark-live", "--jdbc-url", "jdbc:foo//bar", "--jdbc-user", "user"),
            "Error: Missing required argument(s): --jdbc-password"),
        arguments(
            asList("mark-live", "--jdbc-url", "jdbc:foo//bar", "--jdbc-password", "pass"),
            "Error: Missing required argument(s): --jdbc-user="),
        // No live-set-id
        arguments(
            asList("sweep", "--jdbc-url", "jdbc:foo//bar"),
            "Error: Missing required argument (specify one of these): (-l=<liveSetId> | -L=<liveSetIdFile>)"),
        // in-memory with separate mark+sweep is not allowed (does not make sense!)
        arguments(
            asList("mark-live", "--inmemory"),
            "Must not use in-memory content-storage with mark-live"),
        arguments(
            asList("sweep", "--inmemory", "--live-set-id=00000000-0000-0000-0000-000000000000"),
            "Must not use in-memory content-storage with sweep"),
        arguments(
            asList("gc", "--inmemory", "--defer-deletes"),
            "Must not use in-memory content-storage with --defer-deletes"),
        // invalid cutoff-values
        arguments(
            asList("gc", "--inmemory", "--cutoff", "ABC"), "Unmatched argument at index 3: 'ABC'"),
        arguments(
            asList("gc", "--inmemory", "--cutoff", "main=ABC"),
            "Failed to parse cutoff-value 'ABC' for reference name regexpression predicate 'main'"));
  }

  @ParameterizedTest
  @MethodSource("optionErrors")
  @Order(0)
  public void optionErrors(List<String> args, String expectedMessage) throws Exception {
    RunCLI run = RunCLI.run(args);
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(2);
    soft.assertThat(run.getErr()).contains(expectedMessage);
  }

  @Test
  @Order(0)
  public void createTables() throws Exception {
    RunCLI run = RunCLI.run("create-sql-schema", "--jdbc-url", JDBC_URL);
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(0);
  }

  @Test
  @Order(0)
  public void version() throws Exception {
    RunCLI run = RunCLI.run("--version");
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(0);
    soft.assertThat(run.getOut()).startsWith(System.getProperty("expectedNessieVersion"));
  }

  @Test
  @Order(1)
  public void smokeTest() throws Exception {
    RunCLI run = RunCLI.run("gc", "--inmemory", "--uri", nessieUri.toString());
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(0);
  }

  @Test
  @Order(1)
  public void smokeTestJdbc() throws Exception {
    RunCLI run = RunCLI.run("gc", "--jdbc-url", JDBC_URL, "--uri", nessieUri.toString());
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(0);
  }

  @Test
  @Order(2)
  public void identifyBadLiveSetIdFile(@TempDir Path dir) throws Exception {
    dir = dir.resolve("some-directory");

    // Cannot create the live-set-id file
    Files.createFile(dir);

    Path liveSetIdFile = dir.resolve("some-directory").resolve("live-set-id.txt");

    RunCLI run =
        RunCLI.run(
            "mark-live",
            "--jdbc-url",
            JDBC_URL,
            "--uri",
            nessieUri.toString(),
            "--write-live-set-id-to",
            liveSetIdFile.toString());
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(1);
    soft.assertThat(run.getErr())
        .contains("java.nio.file.FileSystemException")
        .contains("Not a directory");
  }

  @Test
  @Order(2)
  public void sweepBadLiveSetIdFile(@TempDir Path dir) throws Exception {
    RunCLI run =
        RunCLI.run(
            "sweep",
            "--jdbc-url",
            JDBC_URL,
            "--read-live-set-id-from",
            dir.resolve("foo-not-there").toString());
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(1);
    soft.assertThat(run.getErr()).contains("java.nio.file.NoSuchFileException");
  }

  @Test
  @Order(3)
  public void roundTrip(@TempDir Path dir) throws Exception {
    Path liveSetIdFile = dir.resolve("some-directory").resolve("live-set-id.txt");

    RunCLI identify =
        RunCLI.run(
            "mark-live",
            "--jdbc-url",
            JDBC_URL,
            "--uri",
            nessieUri.toString(),
            "--write-live-set-id-to",
            liveSetIdFile.toString());
    soft.assertThat(identify.getExitCode()).as(identify::getErr).isEqualTo(0);
    soft.assertThat(liveSetIdFile).isRegularFile();
    soft.assertAll();

    RunCLI sweep =
        RunCLI.run(
            "sweep", "--jdbc-url", JDBC_URL, "--read-live-set-id-from", liveSetIdFile.toString());
    soft.assertThat(sweep.getExitCode()).as(sweep::getErr).isEqualTo(0);
  }

  @Test
  @Order(4)
  public void showCreateSchemaScript() throws Exception {
    RunCLI run = RunCLI.run("show-sql-create-schema-script");
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(0);
    soft.assertThat(run.getOut()).contains("CREATE TABLE ");
  }

  @Test
  @Order(4)
  public void showCreateSchemaScriptToFile(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("schema.sql");
    RunCLI run = RunCLI.run("show-sql-create-schema-script", "--output-file", file.toString());
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(0);
    soft.assertThat(run.getOut()).doesNotContain("CREATE TABLE ");
    soft.assertThat(file).content().contains("CREATE TABLE ");
  }

  @Test
  @Order(5)
  public void listLiveSets() throws Exception {
    RunCLI run = RunCLI.run("list", "--jdbc-url", JDBC_URL);
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(0);
  }

  @Test
  @Order(6)
  public void deleteLiveSets() throws Exception {
    RunCLI list = RunCLI.run("list", "--jdbc-url", JDBC_URL);
    soft.assertThat(list.getExitCode()).as(list::getErr).isEqualTo(0);
    BufferedReader sr = new BufferedReader(new StringReader(list.getOut()));
    sr.readLine(); // Time zone info
    sr.readLine(); // Heading
    String id = sr.readLine();
    id = id.substring(0, id.indexOf(' '));

    RunCLI delete = RunCLI.run("delete", "--live-set-id", id, "--jdbc-url", JDBC_URL);
    soft.assertThat(delete.getExitCode()).as(delete::getErr).isEqualTo(0);
  }

  @Test
  @Order(7)
  public void show(@TempDir Path dir) throws Exception {
    Path liveSetIdFile = dir.resolve("some-directory").resolve("live-set-id.txt");

    RunCLI gc =
        RunCLI.run(
            "gc",
            "--jdbc-url",
            JDBC_URL,
            "--uri",
            nessieUri.toString(),
            "--write-live-set-id-to",
            liveSetIdFile.toString(),
            "--defer-deletes");
    soft.assertThat(gc.getExitCode()).as(gc::getErr).isEqualTo(0);
    soft.assertThat(liveSetIdFile).isRegularFile();
    soft.assertAll();

    UUID id =
        UUID.fromString(new String(Files.readAllBytes(liveSetIdFile), StandardCharsets.UTF_8));

    URI dataLakeDir1 = dir.resolve("data-lake/dir1").toUri();
    URI dataLakeDir2 = dir.resolve("data-lake/dir2").toUri();
    DataSource dataSource =
        AgroalJdbcDataSourceProvider.builder().jdbcUrl(JDBC_URL).build().dataSource();
    try {
      JdbcPersistenceSpi persistenceSpi =
          JdbcPersistenceSpi.builder().dataSource(dataSource).build();
      persistenceSpi.addIdentifiedLiveContent(
          id,
          Stream.of(
              ContentReference.icebergTable(
                  "cid-1",
                  "12345678",
                  ContentKey.of("hello", "world"),
                  "meta://data/location1",
                  42L),
              ContentReference.icebergTable(
                  "cid-1",
                  "44444444",
                  ContentKey.of("hello", "world"),
                  "meta://data/location2",
                  42L)));
      persistenceSpi.associateBaseLocations(id, "cid-1", asList(dataLakeDir1, dataLakeDir2));
      persistenceSpi.associateBaseLocations(id, "cid-2", asList(dataLakeDir1, dataLakeDir2));
      persistenceSpi.addFileDeletions(
          id,
          Stream.of(
              FileReference.of(URI.create("file1"), dataLakeDir1, 42L),
              FileReference.of(URI.create("file2"), dataLakeDir1, 42L),
              FileReference.of(URI.create("file3"), dataLakeDir2, 88L)));
    } finally {
      ((AutoCloseable) dataSource).close();
    }

    RunCLI show =
        RunCLI.run(
            "show",
            "--jdbc-url",
            JDBC_URL,
            "--read-live-set-id-from",
            liveSetIdFile.toString(),
            "--with-deferred-deletes",
            "--with-content-references",
            "--with-base-locations");
    soft.assertThat(show.getExitCode()).as(show::getErr).isEqualTo(0);
    soft.assertThat(show.getOut())
        .contains(" file1")
        .contains(" file2")
        .contains(" file3")
        .contains("Base location: " + dataLakeDir1)
        .contains("ICEBERG_TABLE   12345678 ");
  }

  @Test
  @Order(8)
  public void deferredDeletes(@TempDir Path dir) throws Exception {
    Path liveSetIdFile = dir.resolve("some-directory").resolve("live-set-id.txt");

    RunCLI gc =
        RunCLI.run(
            "gc",
            "--jdbc-url",
            JDBC_URL,
            "--uri",
            nessieUri.toString(),
            "--write-live-set-id-to",
            liveSetIdFile.toString(),
            "--defer-deletes");
    soft.assertThat(gc.getExitCode()).as(gc::getErr).isEqualTo(0);
    soft.assertThat(liveSetIdFile).isRegularFile();
    soft.assertAll();

    UUID id =
        UUID.fromString(new String(Files.readAllBytes(liveSetIdFile), StandardCharsets.UTF_8));

    URI dataLakeDir1 = dir.resolve("data-lake/dir1").toUri();
    URI dataLakeDir2 = dir.resolve("data-lake/dir2").toUri();
    DataSource dataSource =
        AgroalJdbcDataSourceProvider.builder().jdbcUrl(JDBC_URL).build().dataSource();
    try {
      JdbcPersistenceSpi persistenceSpi =
          JdbcPersistenceSpi.builder().dataSource(dataSource).build();
      persistenceSpi.addFileDeletions(
          id,
          Stream.of(
              FileReference.of(URI.create("file1"), dataLakeDir1, 42L),
              FileReference.of(URI.create("file2"), dataLakeDir1, 42L),
              FileReference.of(URI.create("file3"), dataLakeDir2, 88L)));
    } finally {
      ((AutoCloseable) dataSource).close();
    }

    RunCLI listDeferred =
        RunCLI.run(
            "list-deferred",
            "--jdbc-url",
            JDBC_URL,
            "--read-live-set-id-from",
            liveSetIdFile.toString());
    soft.assertThat(listDeferred.getExitCode()).as(listDeferred::getErr).isEqualTo(0);
    soft.assertThat(listDeferred.getOut()).contains(" file1").contains(" file2").contains(" file3");

    RunCLI deleteDeferred =
        RunCLI.run(
            "deferred-deletes",
            "--jdbc-url",
            JDBC_URL,
            "--read-live-set-id-from",
            liveSetIdFile.toString());
    soft.assertThat(deleteDeferred.getExitCode()).as(deleteDeferred::getErr).isEqualTo(0);
    soft.assertThat(deleteDeferred.getOut())
        .contains("Deleted 2 files from " + dataLakeDir1 + ".")
        .contains("Deleted 1 files from " + dataLakeDir2 + ".");
  }

  @Test
  @Order(9)
  public void completionScript(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("script");
    RunCLI run = RunCLI.run("completion-script", "--output-file", file.toString());
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(0);

    run = RunCLI.run("completion-script", "--output-file", file.toString());
    soft.assertThat(run.getExitCode()).as(run::getErr).isEqualTo(1);
    soft.assertThat(run.getErr()).contains("File already exists.");
  }
}
