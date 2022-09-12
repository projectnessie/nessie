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

import java.net.URI;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.repository.NessieRepositoryConnector;
import org.projectnessie.gc.repository.RepositoryConnector;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.jaxrs.ext.NessieUri;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;

@NessieDbAdapterName(InmemoryDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(InmemoryTestConnectionProviderSource.class)
@ExtendWith(DatabaseAdapterExtension.class)
public class TestIdentifyLiveContents {

  @NessieDbAdapter static DatabaseAdapter databaseAdapter;

  @RegisterExtension
  static org.projectnessie.jaxrs.ext.NessieJaxRsExtension server =
      new NessieJaxRsExtension(() -> databaseAdapter);

  private static URI nessieUri;

  @BeforeAll
  static void setNessieUri(@NessieUri URI uri) {
    nessieUri = uri;
  }

  private NessieApiV1 nessieApi;

  @BeforeEach
  public void setUp() {
    nessieApi = HttpClientBuilder.builder().withUri(nessieUri).build(NessieApiV1.class);
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
                      .contentToReference(
                          (content, commitId, key) ->
                              ContentReference.icebergTable(
                                  content.getId(),
                                  commitId,
                                  key,
                                  ((IcebergTable) content).getMetadataLocation(),
                                  ((IcebergTable) content).getSnapshotId()))
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
              .contentToReference(
                  (content, commitId, key) ->
                      ContentReference.icebergTable(
                          content.getId(),
                          commitId,
                          key,
                          ((IcebergTable) content).getMetadataLocation(),
                          ((IcebergTable) content).getSnapshotId()))
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
