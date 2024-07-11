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
package org.projectnessie.gc.repository;

import static java.util.Collections.singletonList;
import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtension;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemorytests.InmemoryBackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
@NessieBackend(InmemoryBackendTestFactory.class)
public class TestNessieRepositoryConnector {
  public static final ImmutableSet<Content.Type> ICEBERG_CONTENT_TYPES =
      ImmutableSet.of(ICEBERG_TABLE, ICEBERG_VIEW);

  @InjectSoftAssertions SoftAssertions soft;

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
  public void allReferences() throws Exception {
    Branch defaultBranch = nessieApi.getDefaultBranch();

    List<Reference> references = new ArrayList<>();
    references.add(defaultBranch);
    for (int i = 0; i < 5; i++) {
      references.add(
          nessieApi
              .createReference()
              .reference(Branch.of("branch-" + i, defaultBranch.getHash()))
              .sourceRefName(defaultBranch.getName())
              .create());
      references.add(
          nessieApi
              .createReference()
              .reference(Tag.of("tag-" + i, defaultBranch.getHash()))
              .sourceRefName(defaultBranch.getName())
              .create());
    }

    try (RepositoryConnector nessie = NessieRepositoryConnector.nessie(nessieApi)) {
      soft.assertThat(nessie.allReferences()).containsExactlyInAnyOrderElementsOf(references);
    }
  }

  @Test
  public void commitLog() throws Exception {
    Branch defaultBranch = nessieApi.getDefaultBranch();
    List<Map.Entry<String, Map.Entry<String, List<Operation>>>> commitsOnBranch = new ArrayList<>();

    defaultBranch = prepareDefaultBranch(defaultBranch, commitsOnBranch);

    Branch branch = Branch.of("commitLog", defaultBranch.getHash());
    branch = prepareBranch(defaultBranch, commitsOnBranch, branch);

    Collections.reverse(commitsOnBranch);

    Function<List<Operation>, List<Operation>> operationsMapper =
        ops ->
            ops.stream()
                .map(
                    op -> {
                      if (op instanceof Operation.Put) {
                        return Operation.Put.of(
                            op.getKey(), ((Operation.Put) op).getContent().withId(null));
                      }
                      return op;
                    })
                .collect(Collectors.toList());

    Function<LogEntry, Entry<String, Entry<String, List<Operation>>>> entryMapper =
        entry ->
            new SimpleEntry<>(
                entry.getCommitMeta().getHash(),
                new SimpleEntry<>(
                    entry.getCommitMeta().getMessage(),
                    operationsMapper.apply(entry.getOperations())));

    try (RepositoryConnector nessie = NessieRepositoryConnector.nessie(nessieApi)) {
      soft.assertThat(nessie.commitLog(defaultBranch))
          .map(entryMapper)
          .containsExactly(commitsOnBranch.get(commitsOnBranch.size() - 1));

      soft.assertThat(nessie.commitLog(branch).map(entryMapper))
          .containsExactlyElementsOf(commitsOnBranch);
    }
  }

  @Test
  public void allContents() throws Exception {
    Branch defaultBranch = nessieApi.getDefaultBranch();
    List<Map.Entry<String, Map.Entry<String, List<Operation>>>> commitsOnBranch = new ArrayList<>();

    defaultBranch = prepareDefaultBranch(defaultBranch, commitsOnBranch);

    Branch branch = Branch.of("commitLog", defaultBranch.getHash());
    prepareBranch(defaultBranch, commitsOnBranch, branch);

    try (RepositoryConnector nessie = NessieRepositoryConnector.nessie(nessieApi)) {
      for (int i = 0; i < commitsOnBranch.size(); i++) {

        Map<ContentKey, Content> expectedContents =
            commitsOnBranch.stream()
                .limit(i + 1)
                .flatMap(e -> e.getValue().getValue().stream())
                .collect(
                    Collectors.toMap(Operation::getKey, e -> ((Operation.Put) e).getContent()));

        Entry<String, Entry<String, List<Operation>>> current = commitsOnBranch.get(i);

        try (Stream<Entry<ContentKey, Content>> contents =
            nessie.allContents(Detached.of(current.getKey()), ICEBERG_CONTENT_TYPES)) {
          soft.assertThat(contents)
              .map(e -> Maps.immutableEntry(e.getKey(), e.getValue().withId(null)))
              .containsExactlyInAnyOrderElementsOf(expectedContents.entrySet());
        }
      }
    }
  }

  private Branch prepareBranch(
      Branch defaultBranch,
      List<Entry<String, Entry<String, List<Operation>>>> commitsOnBranch,
      Branch branch)
      throws NessieNotFoundException, NessieConflictException {
    Operation op;
    nessieApi.createReference().reference(branch).sourceRefName(defaultBranch.getName()).create();

    for (int i = 1; i <= 10; i++) {
      op = Operation.Put.of(ContentKey.of("key-" + i), IcebergTable.of("meta", 42, 43, 44, 45));
      String msg = "commit-" + i;
      branch =
          nessieApi
              .commitMultipleOperations()
              .commitMeta(CommitMeta.fromMessage(msg))
              .branch(branch)
              .operation(op)
              .commit();
      commitsOnBranch.add(
          new SimpleEntry<>(branch.getHash(), new SimpleEntry<>(msg, singletonList(op))));
    }
    return branch;
  }

  private Branch prepareDefaultBranch(
      Branch defaultBranch, List<Entry<String, Entry<String, List<Operation>>>> commitsOnBranch)
      throws NessieNotFoundException, NessieConflictException {
    Operation op =
        Operation.Put.of(ContentKey.of("key-0"), IcebergTable.of("meta", 42, 43, 44, 45));
    defaultBranch =
        nessieApi
            .commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage("initial"))
            .branch(defaultBranch)
            .operation(op)
            .commit();
    commitsOnBranch.add(
        new SimpleEntry<>(
            defaultBranch.getHash(), new SimpleEntry<>("initial", singletonList(op))));
    return defaultBranch;
  }
}
