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
package org.projectnessie.versioned.storage.mongodb2;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import com.mongodb.client.MongoClient;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.PersistLoader;
import org.projectnessie.versioned.storage.mongodbtests2.MongoDB2BackendTestFactory;

@ExtendWith(SoftAssertionsExtension.class)
public class ITMongoDBBackendFactory {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void productionLike() throws Exception {
    MongoDB2BackendTestFactory testFactory = new MongoDB2BackendTestFactory();
    testFactory.start();
    try {
      BackendFactory<MongoDB2BackendConfig> factory =
          PersistLoader.findFactoryByName(MongoDB2BackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(MongoDB2BackendFactory.class);

      try (MongoClient client = testFactory.buildNewClient()) {
        String dbname = "dbname";
        RepositoryDescription repoDesc;
        try (Backend backend =
            factory.buildBackend(
                MongoDB2BackendConfig.builder().client(client).databaseName(dbname).build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(MongoDB2Backend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(MongoDB2PersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(MongoDB2Persist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          repoDesc = repositoryLogic.fetchRepositoryDescription();
          soft.assertThat(repoDesc).isNotNull();
        }

        try (Backend backend =
            factory.buildBackend(
                MongoDB2BackendConfig.builder().client(client).databaseName(dbname).build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(MongoDB2Backend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(MongoDB2PersistFactory.class);
          Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
          soft.assertThat(persist).isNotNull().isInstanceOf(MongoDB2Persist.class);

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
    MongoDB2BackendTestFactory testFactory = new MongoDB2BackendTestFactory();
    testFactory.start();
    try {
      BackendFactory<MongoDB2BackendConfig> factory =
          PersistLoader.findFactoryByName(MongoDB2BackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(MongoDB2BackendFactory.class);

      RepositoryDescription repoDesc;
      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(MongoDB2Backend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(MongoDB2PersistFactory.class);
        Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
        soft.assertThat(persist).isNotNull().isInstanceOf(MongoDB2Persist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        repoDesc = repositoryLogic.fetchRepositoryDescription();
        soft.assertThat(repoDesc).isNotNull();
      }

      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(MongoDB2Backend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(MongoDB2PersistFactory.class);
        Persist persist = persistFactory.newPersist(StoreConfig.Adjustable.empty());
        soft.assertThat(persist).isNotNull().isInstanceOf(MongoDB2Persist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
      }
    } finally {
      testFactory.stop();
    }
  }
}
