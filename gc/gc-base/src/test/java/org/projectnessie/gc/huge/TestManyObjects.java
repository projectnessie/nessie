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
package org.projectnessie.gc.huge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.gc.contents.ContentReference;
import org.projectnessie.gc.contents.LiveContentSet;
import org.projectnessie.gc.contents.LiveContentSetsRepository;
import org.projectnessie.gc.expire.ExpireParameters;
import org.projectnessie.gc.expire.local.DefaultLocalExpire;
import org.projectnessie.gc.files.DeleteSummary;
import org.projectnessie.gc.identify.ContentTypeFilter;
import org.projectnessie.gc.identify.CutoffPolicy;
import org.projectnessie.gc.identify.IdentifyLiveContents;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;

// ************************************************************************************************
// ** THIS CLASS IS ONLY THERE TO PROVE THAT NESSIE-GC WORKS FINE WITH A HUGE AMOUNT OF
// ** CONTENT/SNAPSHOT/BRANCHES/ETC OBJECTS, WITH A SMALL HEAP.
// ************************************************************************************************
// ** THIS CLASS WILL GO AWAY!
// ************************************************************************************************

/**
 * This is a test that walks many objects (references, commit logs, contents, files, etc), pushes
 * the data through both Nessie GC's mark + sweep phases. Tests must complete relatively quick but
 * most importantly never cause any out-of-memory issue.
 */
public class TestManyObjects {

  static Stream<Arguments> justWalkManyReferencesAndCommits() {
    return Stream.of(arguments(5, 100), arguments(20, 200), arguments(100, 100));
  }

  @ParameterizedTest
  @MethodSource("justWalkManyReferencesAndCommits")
  public void justWalkManyReferencesAndCommits(int refCount, int commitMultiplier)
      throws Exception {
    HugeRepositoryConnector repositoryConnector =
        new HugeRepositoryConnector(refCount, commitMultiplier, 3, 1250);
    HugeFiles files = new HugeFiles();
    HugePersistenceSpi storage = new HugePersistenceSpi(repositoryConnector);

    for (int i = 0; i < repositoryConnector.refCount; i++) {
      repositoryConnector
          .allReferences()
          .flatMap(repositoryConnector::commitLog)
          .forEach(
              logEntry -> {
                List<Operation> ops = logEntry.getOperations();
                if (ops != null) {
                  for (Operation op : ops) {
                    IcebergTable table = (IcebergTable) ((Operation.Put) op).getContent();
                    files.createSnapshot(table.getId(), (int) table.getSnapshotId());
                  }
                }
              });
    }

    LiveContentSetsRepository repository =
        LiveContentSetsRepository.builder().persistenceSpi(storage).build();

    ContentTypeFilter contentTypeFilter =
        new ContentTypeFilter() {
          @Override
          public boolean test(Content.Type type) {
            return ICEBERG_TABLE == type;
          }

          @Override
          public Set<Content.Type> validTypes() {
            return Collections.singleton(ICEBERG_TABLE);
          }
        };

    IdentifyLiveContents identify =
        IdentifyLiveContents.builder()
            .contentTypeFilter(contentTypeFilter)
            .cutOffPolicySupplier(r -> CutoffPolicy.numCommits(50))
            .contentToContentReference(
                (content, commitId, key) ->
                    ContentReference.icebergTable(
                        content.getId(),
                        commitId,
                        key,
                        ((IcebergTable) content).getMetadataLocation(),
                        ((IcebergTable) content).getSnapshotId()))
            .liveContentSetsRepository(repository)
            .repositoryConnector(repositoryConnector)
            .build();

    UUID id = identify.identifyLiveContents();

    LiveContentSet liveContentSet = repository.getLiveContentSet(id);

    DefaultLocalExpire localExpire =
        DefaultLocalExpire.builder()
            .expireParameters(
                ExpireParameters.builder()
                    .liveContentSet(liveContentSet)
                    .filesLister(files)
                    .fileDeleter(files)
                    .contentToFiles(files)
                    .expectedFileCount(10_000_000)
                    .maxFileModificationTime(Instant.now())
                    .build())
            .build();

    DeleteSummary deleteSummary = localExpire.expire();
    assertThat(deleteSummary).extracting(DeleteSummary::failures).isEqualTo(0L);
  }
}
