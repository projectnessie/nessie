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

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.bigtabletests.BigTableBackendContainerTestFactory;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.PersistLoader;

@ExtendWith(SoftAssertionsExtension.class)
public class ITBigTableBackendFactory {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void productionLike() throws Exception {
    BigTableBackendContainerTestFactory testFactory = new BigTableBackendContainerTestFactory();
    testFactory.start();
    try {
      BackendFactory<BigTableBackendConfig> factory =
          PersistLoader.findFactoryByName(BigTableBackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(BigTableBackendFactory.class);
      RepositoryDescription repoDesc;

      try (BigtableDataClient dataClient = testFactory.buildNewDataClient();
          BigtableTableAdminClient tableAdminClient = testFactory.buildNewTableAdminClient()) {
        try (Backend backend =
            factory.buildBackend(
                BigTableBackendConfig.builder()
                    .dataClient(dataClient)
                    .tableAdminClient(tableAdminClient)
                    .build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(BigTableBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(BigTablePersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(BigTablePersist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          repoDesc = repositoryLogic.fetchRepositoryDescription();
          soft.assertThat(repoDesc).isNotNull();
        }
      }

      try (BigtableDataClient dataClient = testFactory.buildNewDataClient();
          BigtableTableAdminClient tableAdminClient = testFactory.buildNewTableAdminClient()) {
        try (Backend backend =
            factory.buildBackend(
                BigTableBackendConfig.builder()
                    .dataClient(dataClient)
                    .tableAdminClient(tableAdminClient)
                    .build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(BigTableBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(BigTablePersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(BigTablePersist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
        }
      }
    } finally {
      testFactory.stop();
    }
  }

  @Test
  public void backendTestFactory() throws Exception {
    BigTableBackendContainerTestFactory testFactory = new BigTableBackendContainerTestFactory();
    testFactory.start();
    try {
      BackendFactory<BigTableBackendConfig> factory =
          PersistLoader.findFactoryByName(BigTableBackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(BigTableBackendFactory.class);

      RepositoryDescription repoDesc;
      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(BigTableBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(BigTablePersistFactory.class);
        Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
        soft.assertThat(persist).isNotNull().isInstanceOf(BigTablePersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        repoDesc = repositoryLogic.fetchRepositoryDescription();
        soft.assertThat(repoDesc).isNotNull();
      }

      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(BigTableBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(BigTablePersistFactory.class);
        Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
        soft.assertThat(persist).isNotNull().isInstanceOf(BigTablePersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
      }
    } finally {
      testFactory.stop();
    }
  }
}
