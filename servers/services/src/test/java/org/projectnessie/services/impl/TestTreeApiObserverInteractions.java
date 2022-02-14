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
package org.projectnessie.services.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.api.TreeApi;
import org.projectnessie.events.CommitEvent;
import org.projectnessie.events.Event;
import org.projectnessie.events.ReferenceCreatedEvent;
import org.projectnessie.events.ReferenceDeletedEvent;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.events.EventObserver;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.VersionStore;

@ExtendWith(MockitoExtension.class)
public class TestTreeApiObserverInteractions {
  private static final ServerConfig SERVER_CONFIG =
      new ServerConfig() {
        @Override
        public String getDefaultBranch() {
          return "main";
        }

        @Override
        public boolean sendStacktraceToClient() {
          return false;
        }
      };

  private static final String SOME_HASH = "aaaabbbbccccddddeeee";

  private static final String MAIN_BRANCH = "main";

  private static final String DEV_BRANCH = "dev";

  @Mock private VersionStore<Content, CommitMeta, Content.Type> mockVersionStore;

  private final DummyObserver theObserver = new DummyObserver();

  private TreeApi treeApi;

  @BeforeEach
  public void setup() {
    treeApi = new TreeApiImpl(SERVER_CONFIG, mockVersionStore, null, null, theObserver);
  }

  @Test
  public void observerGetsNotifiedWhenBranchIsCreated() throws Exception {
    treeApi.createReference(MAIN_BRANCH, Branch.of(DEV_BRANCH, SOME_HASH));

    assertThat(theObserver.lastEvent).isInstanceOf(ReferenceCreatedEvent.class);

    ReferenceCreatedEvent referenceCreatedEvent = (ReferenceCreatedEvent) theObserver.lastEvent;
    assertThat(referenceCreatedEvent.getReference().getName()).isEqualTo(DEV_BRANCH);
  }

  @Test
  public void observerGetsNotifiedWhenCommit() throws Exception {
    String expectedHash = "ffffeeeebbbbccccddddeeeeaaaa";
    String expectedCommitMessage = "Some commit";
    Operation expectedOperation = Operation.Delete.of(ContentKey.of("foo"));
    // For this test, we don't care about the interactions between TreeApi and the version store.
    // However, we care that we return the correct hash.
    when(mockVersionStore.commit(any(), any(), any(), any())).thenReturn(Hash.of(expectedHash));
    final Operations operations =
        ImmutableOperations.builder()
            .addOperations(expectedOperation)
            .commitMeta(CommitMeta.fromMessage(expectedCommitMessage))
            .build();

    treeApi.commitMultipleOperations(MAIN_BRANCH, SOME_HASH, operations);

    assertThat(theObserver.lastEvent).isInstanceOf(CommitEvent.class);

    CommitEvent commitEvent = (CommitEvent) theObserver.lastEvent;
    assertThat(commitEvent.getReference().getName()).isEqualTo(MAIN_BRANCH);
    assertThat(commitEvent.getReference().getHash()).isEqualTo(SOME_HASH);
    assertThat(commitEvent.getNewHash()).isEqualTo(expectedHash);
    assertThat(commitEvent.getMetadata().getMessage()).isEqualTo(expectedCommitMessage);
    assertThat(commitEvent.getOperations()).first().isEqualTo(expectedOperation);
  }

  @Test
  public void observerGetsNotifiedWhenBranchIsDeleted() throws Exception {
    treeApi.deleteReference(Reference.ReferenceType.BRANCH, MAIN_BRANCH, SOME_HASH);

    assertThat(theObserver.lastEvent).isInstanceOf(ReferenceDeletedEvent.class);

    ReferenceDeletedEvent referenceDeletedEvent = (ReferenceDeletedEvent) theObserver.lastEvent;
    assertThat(referenceDeletedEvent.getReferenceName()).isEqualTo(MAIN_BRANCH);
    assertThat(referenceDeletedEvent.getReferenceType()).isEqualTo(Reference.ReferenceType.BRANCH);
  }

  private static class DummyObserver implements EventObserver {
    private Event lastEvent;

    @Override
    public void notify(Event event) {
      this.lastEvent = event;
    }
  }
}
