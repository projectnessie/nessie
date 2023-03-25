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
package org.projectnessie.versioned.storage.spanner;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import com.google.cloud.spanner.Spanner;
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

@ExtendWith(SoftAssertionsExtension.class)
public class ITSpannerBackendFactory {
  @InjectSoftAssertions protected SoftAssertions soft;

  static StoreConfig DEFAULT_CONFIG = StoreConfig.Adjustable.empty();

  @Test
  public void productionLike() throws Exception {
    SpannerBackendTestFactory testFactory = new SpannerBackendTestFactory();
    testFactory.start();
    try {
      BackendFactory<SpannerBackendConfig> factory =
          PersistLoader.findFactoryByName(SpannerBackendFactory.NAME);
      soft.assertThat(factory).isNotNull().isInstanceOf(SpannerBackendFactory.class);

      try (Spanner spanner = testFactory.spanner()) {
        RepositoryDescription repoDesc;
        try (Backend backend =
            factory.buildBackend(
                SpannerBackendConfig.builder()
                    .spanner(spanner)
                    .databaseId(testFactory.databaseId())
                    .build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(SpannerBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(SpannerPersistFactory.class);
          Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
          soft.assertThat(persist).isNotNull().isInstanceOf(SpannerPersist.class);

          RepositoryLogic repositoryLogic = repositoryLogic(persist);
          repositoryLogic.initialize("initializeAgain");
          repoDesc = repositoryLogic.fetchRepositoryDescription();
          soft.assertThat(repoDesc).isNotNull();
        }

        try (Backend backend =
            factory.buildBackend(
                SpannerBackendConfig.builder()
                    .spanner(spanner)
                    .databaseId(testFactory.databaseId())
                    .build())) {
          soft.assertThat(backend).isNotNull().isInstanceOf(SpannerBackend.class);
          backend.setupSchema();
          PersistFactory persistFactory = backend.createFactory();
          soft.assertThat(persistFactory).isNotNull().isInstanceOf(SpannerPersistFactory.class);
          Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
          soft.assertThat(persist).isNotNull().isInstanceOf(SpannerPersist.class);

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
    SpannerBackendTestFactory testFactory = new SpannerBackendTestFactory();
    testFactory.start();
    try {
      RepositoryDescription repoDesc;
      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(SpannerBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(SpannerPersistFactory.class);
        Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
        soft.assertThat(persist).isNotNull().isInstanceOf(SpannerPersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        repoDesc = repositoryLogic.fetchRepositoryDescription();
        soft.assertThat(repoDesc).isNotNull();
      }

      try (Backend backend = testFactory.createNewBackend()) {
        soft.assertThat(backend).isNotNull().isInstanceOf(SpannerBackend.class);
        backend.setupSchema();
        PersistFactory persistFactory = backend.createFactory();
        soft.assertThat(persistFactory).isNotNull().isInstanceOf(SpannerPersistFactory.class);
        Persist persist = persistFactory.newPersist(DEFAULT_CONFIG);
        soft.assertThat(persist).isNotNull().isInstanceOf(SpannerPersist.class);

        RepositoryLogic repositoryLogic = repositoryLogic(persist);
        repositoryLogic.initialize("initializeAgain");
        soft.assertThat(repositoryLogic.fetchRepositoryDescription()).isEqualTo(repoDesc);
      }
    } finally {
      testFactory.stop();
    }
  }
}
