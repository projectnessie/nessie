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
package org.projectnessie.versioned.storage.commontests;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;
import static org.projectnessie.versioned.storage.common.objtypes.StringObj.stringData;
import static org.projectnessie.versioned.storage.versionstore.RefMapping.REFS_HEADS;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.objtypes.Compression;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

/** Basic {@link Persist} tests to be run by every implementation. */
@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class AbstractBackendRepositoryTests {
  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Backend backend;
  @NessiePersist protected PersistFactory persistFactory;

  @Test
  public void createEraseRepoViaPersist() throws Exception {
    Persist repo1 = newRepo();

    RepositoryLogic repositoryLogic = repositoryLogic(repo1);
    repositoryLogic.initialize("foo-main");
    soft.assertThat(repositoryLogic.repositoryExists()).isTrue();

    int objs = 250;
    soft.assertThat(
            repo1.storeObjs(
                IntStream.range(0, objs)
                    .mapToObj(
                        x ->
                            stringData(
                                "content-type",
                                Compression.NONE,
                                "file-" + x,
                                Collections.emptyList(),
                                ByteString.copyFromUtf8("text-" + x)))
                    .toArray(Obj[]::new)))
        .hasSize(objs)
        .doesNotContain(false);
    try (CloseableIterator<Obj> scan = repo1.scanAllObjects(Set.of())) {
      soft.assertThat(scan)
          .toIterable()
          .filteredOn(
              o -> o instanceof StringObj && ((StringObj) o).contentType().equals("content-type"))
          .hasSize(objs);
    }

    repo1.erase();
    soft.assertThat(repositoryLogic.repositoryExists()).isFalse();
    try (CloseableIterator<Obj> scan = repo1.scanAllObjects(Set.of())) {
      soft.assertThat(scan).isExhausted();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3, 10})
  public void createEraseManyRepos(int numRepos) {
    List<Persist> repos =
        IntStream.range(0, numRepos).mapToObj(x -> newRepo()).collect(Collectors.toList());

    repos.forEach(r -> repositoryLogic(r).initialize("foo-meep"));
    soft.assertThat(repos).allMatch(r -> repositoryLogic(r).repositoryExists());
    backend.eraseRepositories(
        repos.stream().map(r -> r.config().repositoryId()).collect(Collectors.toSet()));
    soft.assertThat(repos).noneMatch(r -> repositoryLogic(r).repositoryExists());
    soft.assertThat(repos)
        .noneMatch(
            r -> {
              try (CloseableIterator<Obj> scan = r.scanAllObjects(Set.of())) {
                return scan.hasNext();
              }
            });
  }

  /** Check that erasing a few repos does not affect other repos. */
  @Test
  public void createEraseSomeRepos() throws ObjTooLargeException, RefAlreadyExistsException {
    List<Persist> repos =
        IntStream.range(0, 10).mapToObj(x -> newRepo()).collect(Collectors.toList());
    int refs = 25;
    int objs = 250;
    for (Persist repo : repos) {
      repositoryLogic(repo).initialize("main");
      for (int i = 0; i < refs; i++) {
        Reference reference =
            Reference.reference(
                REFS_HEADS + "reference-" + i, ObjId.randomObjId(), false, 1234L, null);
        repo.addReference(reference);
      }
      StringObj[] objects = new StringObj[objs];
      for (int i = 0; i < objs; i++) {
        StringObj stringObj =
            stringData(
                "content-type",
                Compression.NONE,
                "file-" + i,
                Collections.emptyList(),
                ByteString.copyFromUtf8("text-" + i));
        objects[i] = stringObj;
      }
      repo.storeObjs(objects);
    }
    soft.assertThat(repos).allMatch(r -> repositoryLogic(r).repositoryExists());
    List<Persist> toDelete = repos.subList(0, 5);
    List<Persist> toKeep = repos.subList(5, 10);
    backend.eraseRepositories(
        toDelete.stream().map(r -> r.config().repositoryId()).collect(Collectors.toSet()));
    soft.assertThat(toDelete).noneMatch(r -> repositoryLogic(r).repositoryExists());
    soft.assertThat(toDelete)
        .noneMatch(
            r -> {
              try (CloseableIterator<Obj> scan = r.scanAllObjects(Set.of())) {
                return scan.hasNext();
              }
            });
    for (Persist repo : toKeep) {
      soft.assertThat(repositoryLogic(repo).repositoryExists()).isTrue();
      soft.assertThat(repo.fetchReference(REFS_HEADS + "main")).isNotNull();
      for (int i = 0; i < refs; i++) {
        soft.assertThat(repo.fetchReference(REFS_HEADS + "reference-" + i)).isNotNull();
      }
      try (CloseableIterator<Obj> scan = repo.scanAllObjects(Set.of())) {
        soft.assertThat(scan)
            .toIterable()
            .filteredOn(
                o -> o instanceof StringObj && ((StringObj) o).contentType().equals("content-type"))
            .hasSize(objs);
      }
    }
  }

  private Persist newRepo() {
    return persistFactory.newPersist(
        StoreConfig.Adjustable.empty().withRepositoryId("repo-" + UUID.randomUUID()));
  }
}
