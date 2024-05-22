/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.bigtable;

import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import java.util.List;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.bigtabletests.AbstractBigTableBackendTestFactory;
import org.projectnessie.versioned.storage.bigtabletests.BigTableBackendContainerTestFactory;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.commontests.AbstractBackendRepositoryTests;
import org.projectnessie.versioned.storage.commontests.AbstractPersistTests;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@NessieBackend(BigTableBackendContainerTestFactory.class)
@ExtendWith(PersistExtension.class)
public class ITBigTablePersist extends AbstractPersistTests {

  @Nested
  public class NoAdminClientBackendRepositoryTests extends AbstractBackendRepositoryTests {
    @BeforeEach
    void noAdminClient() {
      BigTableBackend b = (BigTableBackend) backend;
      backend = new BigTableBackend(BigTableBackendConfig.builder().dataClient(b.client()).build());
    }
  }

  @Nested
  @ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
  public class TablePrefixes {
    @InjectSoftAssertions protected SoftAssertions soft;

    @NessiePersist(initializeRepo = false)
    protected BackendTestFactory factory;

    @Test
    void tablePrefix() {
      AbstractBigTableBackendTestFactory btFactory = (AbstractBigTableBackendTestFactory) factory;
      try (BigTableBackend backendA =
              (BigTableBackend)
                  btFactory.createNewBackend(
                      btFactory
                          .bigtableConfigBuilder()
                          .tablePrefix(Optional.of("instanceA"))
                          .build());
          BigTableBackend backendB =
              (BigTableBackend)
                  btFactory.createNewBackend(
                      btFactory
                          .bigtableConfigBuilder()
                          .tablePrefix(Optional.of("instanceB"))
                          .build())) {

        BigtableTableAdminClient adminClientA = requireNonNull(backendA.adminClient());
        BigtableTableAdminClient adminClientB = requireNonNull(backendB.adminClient());

        List<String> expectedTables = List.of();
        soft.assertThat(adminClientA.listTables())
            .containsExactlyInAnyOrderElementsOf(expectedTables);
        soft.assertThat(adminClientB.listTables())
            .containsExactlyInAnyOrderElementsOf(expectedTables);

        // Setup "A"

        backendA.setupSchema();

        Persist persistA = backendA.createFactory().newPersist(StoreConfig.Adjustable.empty());
        RepositoryLogic repoA = repositoryLogic(persistA);

        expectedTables = List.of("instanceA_refs", "instanceA_objs");

        soft.assertThat(adminClientA.listTables())
            .containsExactlyInAnyOrderElementsOf(expectedTables);
        soft.assertThat(adminClientB.listTables())
            .containsExactlyInAnyOrderElementsOf(expectedTables);

        soft.assertThat(repoA.repositoryExists()).isFalse();
        repoA.initialize("main");
        soft.assertThat(repoA.repositoryExists()).isTrue();

        // Setup "B"

        backendB.setupSchema();

        Persist persistB = backendB.createFactory().newPersist(StoreConfig.Adjustable.empty());
        RepositoryLogic repoB = repositoryLogic(persistB);

        expectedTables =
            List.of("instanceA_refs", "instanceA_objs", "instanceB_refs", "instanceB_objs");

        soft.assertThat(adminClientA.listTables())
            .containsExactlyInAnyOrderElementsOf(expectedTables);
        soft.assertThat(adminClientB.listTables())
            .containsExactlyInAnyOrderElementsOf(expectedTables);

        soft.assertThat(repoB.repositoryExists()).isFalse();
        repoB.initialize("main");
        soft.assertThat(repoB.repositoryExists()).isTrue();
      }
    }
  }
}
