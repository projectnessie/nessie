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
package org.projectnessie.gc.identify;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.gc.contents.ContentReference.icebergContent;
import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtension;

import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.repository.NessieRepositoryConnector;
import org.projectnessie.gc.repository.RepositoryConnector;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemorytests.InmemoryBackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith(PersistExtension.class)
@NessieBackend(InmemoryBackendTestFactory.class)
public class TestIdentifyLiveContents {

  @NessiePersist static Persist persist;

  @RegisterExtension static NessieJaxRsExtension server = jaxRsExtension(() -> persist);

  private NessieApiV1 nessieApi;

  @BeforeEach
  public void setUp(NessieClientFactory clientFactory) {
    nessieApi = clientFactory.make();
  }

  @AfterEach
  public void tearDown() {
    nessieApi.close();
  }

  @Test
  public void invalidParallelism() throws Exception {
    try (RepositoryConnector nessie = NessieRepositoryConnector.nessie(nessieApi)) {
      assertThatThrownBy(
              () ->
                  IdentifyLiveContents.builder()
                      .parallelism(0)
                      .contentTypeFilter(
                          new ContentTypeFilter() {
                            @Override
                            public boolean test(Content.Type type) {
                              return true;
                            }

                            @Override
                            public Set<Content.Type> validTypes() {
                              throw new UnsupportedOperationException();
                            }
                          })
                      .cutOffPolicySupplier(r -> CutoffPolicy.atTimestamp(Instant.now()))
                      .contentToContentReference(
                          (content, commitId, key) -> icebergContent(commitId, key, content))
                      .liveContentSetsRepository(
                          LiveContentSetsRepository.builder()
                              .persistenceSpi(new InMemoryPersistenceSpi())
                              .build())
                      .repositoryConnector(nessie)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Parallelism must be greater than 0");
    }
  }

  @Test
  public void preventMultipleUsages() throws Exception {
    try (RepositoryConnector nessie = NessieRepositoryConnector.nessie(nessieApi)) {
      IdentifyLiveContents identify =
          IdentifyLiveContents.builder()
              .contentTypeFilter(
                  new ContentTypeFilter() {
                    @Override
                    public boolean test(Content.Type type) {
                      return true;
                    }

                    @Override
                    public Set<Content.Type> validTypes() {
                      throw new UnsupportedOperationException();
                    }
                  })
              .cutOffPolicySupplier(r -> CutoffPolicy.atTimestamp(Instant.now()))
              .contentToContentReference(
                  (content, commitId, key) -> icebergContent(commitId, key, content))
              .liveContentSetsRepository(
                  LiveContentSetsRepository.builder()
                      .persistenceSpi(new InMemoryPersistenceSpi())
                      .build())
              .repositoryConnector(nessie)
              .build();
      identify.identifyLiveContents();

      assertThatThrownBy(identify::identifyLiveContents)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("identifyLiveContents() has already been called");
    }
  }
}
