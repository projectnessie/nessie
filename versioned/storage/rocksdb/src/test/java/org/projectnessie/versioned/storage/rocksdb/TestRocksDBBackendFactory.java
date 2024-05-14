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
package org.projectnessie.versioned.storage.rocksdb;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import java.nio.file.Path;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.PersistLoader;
import org.projectnessie.versioned.storage.rocksdbtests.RocksDBBackendTestFactory;

@ExtendWith(SoftAssertionsExtension.class)
public class TestRocksDBBackendFactory {
  @InjectSoftAssertions protected SoftAssertions soft;

  @TempDir protected Path rocksDir;

  @Test
  public void productionLike() throws Exception {
    BackendFactory<RocksDBBackendConfig> factory =
        PersistLoader.findFactoryByName(RocksDBBackendFactory.NAME);
    soft.assertThat(factory).isNotNull().isInstanceOf(RocksDBBackendFactory.class);

    RepositoryDescription repoDesc;

    try (Backend backend =
        factory.buildBackend(RocksDBBackendConfig.builder().databasePath(rocksDir).build())) {
      soft.assertThat(backend).isNotNull().isInstanceOf(RocksDBBackend.class);
      backend.setupSchema();
      PersistFactory persistFactory = backend.createFactory();
      soft.assertThat(persistFactory).isNotNull().isInstanceOf(RocksDBPersistFactory.class);
      Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
      soft.assertThat(persist).isNotNull().isInstanceOf(RocksDBPersist.class);

      RepositoryLogic repositoryLogic = repositoryLogic(persist);
      repositoryLogic.initialize("initializeAgain");
      repoDesc = repositoryLogic.fetchRepositoryDescription();
      soft.assertThat(repoDesc).isNotNull();
    }

    try (Backend backend =
        factory.buildBackend(RocksDBBackendConfig.builder().databasePath(rocksDir).build())) {
      soft.assertThat(backend).isNotNull().isInstanceOf(RocksDBBackend.class);
      backend.setupSchema();
      PersistFactory persistFactory = backend.createFactory();
      soft.assertThat(persistFactory).isNotNull().isInstanceOf(RocksDBPersistFactory.class);
      Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
      soft.assertThat(persist).isNotNull().isInstanceOf(RocksDBPersist.class);

      RepositoryLogic repositoryLogic = repositoryLogic(persist);
      repositoryLogic.initialize("initializeAgain");
      soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
    }
  }

  @Test
  public void testFactory() throws Exception {
    RocksDBBackendTestFactory testFactory = new RocksDBBackendTestFactory();

    RepositoryDescription repoDesc;

    testFactory.start();
    try {
      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(RocksDBBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(RocksDBPersistFactory.class);
        Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
        soft.assertThat(persist).isNotNull().isInstanceOf(RocksDBPersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        repoDesc = repositoryLogic.fetchRepositoryDescription();
        soft.assertThat(repoDesc).isNotNull();
      }

      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(RocksDBBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(RocksDBPersistFactory.class);
        Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
        soft.assertThat(persist).isNotNull().isInstanceOf(RocksDBPersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
      }
    } finally {
      testFactory.stop();
    }

    testFactory.start();
    try {
      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(RocksDBBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(RocksDBPersistFactory.class);
        Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
        soft.assertThat(persist).isNotNull().isInstanceOf(RocksDBPersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isNotEqualTo(repoDesc);
      }
    } finally {
      testFactory.stop();
    }
  }
}
