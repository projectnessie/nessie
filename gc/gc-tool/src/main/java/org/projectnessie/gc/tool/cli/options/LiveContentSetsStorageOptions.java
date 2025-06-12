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
package org.projectnessie.gc.tool.cli.options;

import java.sql.Connection;
import javax.sql.DataSource;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.contents.jdbc.JdbcPersistenceSpi;
import org.projectnessie.gc.contents.spi.PersistenceSpi;
import org.projectnessie.gc.tool.cli.Closeables;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;

public class LiveContentSetsStorageOptions {

  @CommandLine.ArgGroup(multiplicity = "1")
  ContentsStorageGroup contentsStorageOpts;

  static class ContentsStorageGroup {
    @CommandLine.ArgGroup InMemoryContentsStorageOptions inmemory;

    @CommandLine.ArgGroup(exclusive = false)
    JdbcOptions jdbc;

    static class InMemoryContentsStorageOptions {
      @CommandLine.Option(
          names = "--inmemory",
          description =
              "Flag whether to use the in-memory contents storage. Prefer a JDBC storage.")
      boolean inmemory;
    }
  }

  public void assertNotInMemory(CommandSpec commandSpec) {
    assertNotInMemory(commandSpec, commandSpec.name());
  }

  public void assertNotInMemory(CommandSpec commandSpec, String additionalMessage) {
    if (contentsStorageOpts.inmemory != null) {
      throw new ParameterException(
          commandSpec.commandLine(),
          "Must not use in-memory content-storage with " + additionalMessage);
    }
  }

  public LiveContentSetsRepository createLiveContentSetsRepository(Closeables closeables)
      throws Exception {
    return LiveContentSetsRepository.builder()
        .persistenceSpi(createPersistenceSpi(closeables))
        .build();
  }

  private PersistenceSpi createPersistenceSpi(Closeables closeables) throws Exception {
    if (contentsStorageOpts.inmemory != null) {
      return new InMemoryPersistenceSpi();
    }
    if (contentsStorageOpts.jdbc != null) {
      return createJdbcPersistenceSpi(closeables, contentsStorageOpts.jdbc);
    }

    throw new IllegalStateException("No contents-storage configured");
  }

  private PersistenceSpi createJdbcPersistenceSpi(Closeables closeables, JdbcOptions jdbc)
      throws Exception {
    DataSource dataSource = closeables.maybeAdd(jdbc.createDataSource());
    SchemaCreateStrategy schemaCreateStrategy = jdbc.getSchemaCreateStrategy();
    if (schemaCreateStrategy != null) {
      try (Connection conn = dataSource.getConnection()) {
        schemaCreateStrategy.apply(conn);
      }
    }
    return JdbcPersistenceSpi.builder().dataSource(dataSource).fetchSize(jdbc.fetchSize).build();
  }
}
