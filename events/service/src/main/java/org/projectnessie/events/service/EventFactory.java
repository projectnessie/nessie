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
package org.projectnessie.events.service;

import jakarta.annotation.Nullable;
import java.security.Principal;
import java.util.Objects;
import org.projectnessie.events.api.Content;
import org.projectnessie.events.api.ContentKey;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.api.ImmutableCommitEvent;
import org.projectnessie.events.api.ImmutableCommitMeta;
import org.projectnessie.events.api.ImmutableContentRemovedEvent;
import org.projectnessie.events.api.ImmutableContentStoredEvent;
import org.projectnessie.events.api.ImmutableMergeEvent;
import org.projectnessie.events.api.ImmutableReferenceCreatedEvent;
import org.projectnessie.events.api.ImmutableReferenceDeletedEvent;
import org.projectnessie.events.api.ImmutableReferenceUpdatedEvent;
import org.projectnessie.events.api.ImmutableTransplantEvent;
import org.projectnessie.events.service.util.ReferenceMapping;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.ResultType;

/**
 * Factory for creating {@link Event}s from various version store objects, with all the boilerplate
 * code that it requires.
 *
 * <p>This class is meant to be used as a singleton, or in CDI Dependent pseudo-scope.
 */
public class EventFactory {

  protected final EventConfig config;

  public EventFactory(EventConfig config) {
    this.config = config;
  }

  protected Event newCommitEvent(
      Commit commit, BranchName targetBranch, String repositoryId, @Nullable Principal user) {
    org.projectnessie.model.CommitMeta commitMeta = commit.getCommitMeta();
    ImmutableCommitEvent.Builder builder =
        ImmutableCommitEvent.builder()
            .id(config.getIdGenerator().get())
            .eventCreationTimestamp(config.getClock().instant())
            .repositoryId(repositoryId)
            .properties(config.getStaticProperties())
            .sourceReference(ReferenceMapping.map(targetBranch)) // same as target for commits
            .targetReference(ReferenceMapping.map(targetBranch))
            .hashBefore(Objects.requireNonNull(commit.getParentHash()).asString())
            .hashAfter(commit.getHash().asString())
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer(Objects.requireNonNull(commitMeta.getCommitter()))
                    .authors(commitMeta.getAllAuthors())
                    .signOffs(commitMeta.getAllSignedOffBy())
                    .message(commitMeta.getMessage())
                    .commitTime(Objects.requireNonNull(commitMeta.getCommitTime()))
                    .authorTime(Objects.requireNonNull(commitMeta.getAuthorTime()))
                    .multiProperties(commitMeta.getAllProperties())
                    .build());
    if (user != null) {
      builder.eventInitiator(user.getName());
    }
    return builder.build();
  }

  protected Event newMergeEvent(
      MergeResult<Commit> result, String repositoryId, @Nullable Principal user) {
    assert result.getResultType() == ResultType.MERGE;
    String commonAncestorHash = Objects.requireNonNull(result.getCommonAncestor()).asString();
    ImmutableMergeEvent.Builder builder =
        ImmutableMergeEvent.builder()
            .id(config.getIdGenerator().get())
            .eventCreationTimestamp(config.getClock().instant())
            .repositoryId(repositoryId)
            .properties(config.getStaticProperties())
            .sourceReference(ReferenceMapping.map(result.getSourceRef()))
            .targetReference(ReferenceMapping.map(result.getTargetBranch()))
            .hashBefore(result.getEffectiveTargetHash().asString())
            .hashAfter(Objects.requireNonNull(result.getResultantTargetHash()).asString())
            .commonAncestorHash(commonAncestorHash);
    if (user != null) {
      builder.eventInitiator(user.getName());
    }
    return builder.build();
  }

  protected Event newTransplantEvent(
      MergeResult<Commit> result, String repositoryId, @Nullable Principal user) {
    assert result.getResultType() == ResultType.TRANSPLANT;
    ImmutableTransplantEvent.Builder builder =
        ImmutableTransplantEvent.builder()
            .id(config.getIdGenerator().get())
            .eventCreationTimestamp(config.getClock().instant())
            .repositoryId(repositoryId)
            .properties(config.getStaticProperties())
            .sourceReference(ReferenceMapping.map(result.getSourceRef()))
            .targetReference(ReferenceMapping.map(result.getTargetBranch()))
            .hashBefore(result.getEffectiveTargetHash().asString())
            .hashAfter(Objects.requireNonNull(result.getResultantTargetHash()).asString());
    if (user != null) {
      builder.eventInitiator(user.getName());
    }
    return builder.build();
  }

  protected Event newReferenceCreatedEvent(
      ReferenceCreatedResult result, String repositoryId, @Nullable Principal user) {
    ImmutableReferenceCreatedEvent.Builder builder =
        ImmutableReferenceCreatedEvent.builder()
            .id(config.getIdGenerator().get())
            .eventCreationTimestamp(config.getClock().instant())
            .repositoryId(repositoryId)
            .properties(config.getStaticProperties())
            .reference(ReferenceMapping.map(result.getNamedRef()))
            .hashAfter(result.getHash().asString());
    if (user != null) {
      builder.eventInitiator(user.getName());
    }
    return builder.build();
  }

  protected Event newReferenceUpdatedEvent(
      ReferenceAssignedResult result, String repositoryId, @Nullable Principal user) {
    ImmutableReferenceUpdatedEvent.Builder builder =
        ImmutableReferenceUpdatedEvent.builder()
            .id(config.getIdGenerator().get())
            .eventCreationTimestamp(config.getClock().instant())
            .repositoryId(repositoryId)
            .properties(config.getStaticProperties())
            .reference(ReferenceMapping.map(result.getNamedRef()))
            .hashBefore(result.getPreviousHash().asString())
            .hashAfter(result.getCurrentHash().asString());
    if (user != null) {
      builder.eventInitiator(user.getName());
    }
    return builder.build();
  }

  protected Event newReferenceDeletedEvent(
      ReferenceDeletedResult result, String repositoryId, @Nullable Principal user) {
    ImmutableReferenceDeletedEvent.Builder builder =
        ImmutableReferenceDeletedEvent.builder()
            .id(config.getIdGenerator().get())
            .eventCreationTimestamp(config.getClock().instant())
            .repositoryId(repositoryId)
            .properties(config.getStaticProperties())
            .reference(ReferenceMapping.map(result.getNamedRef()))
            .hashBefore(result.getHash().asString());
    if (user != null) {
      builder.eventInitiator(user.getName());
    }
    return builder.build();
  }

  protected Event newContentStoredEvent(
      BranchName branch,
      Hash hash,
      ContentKey contentKey,
      Content content,
      String repositoryId,
      @Nullable Principal user) {
    ImmutableContentStoredEvent.Builder builder =
        ImmutableContentStoredEvent.builder()
            .type(EventType.CONTENT_STORED)
            .id(config.getIdGenerator().get())
            .eventCreationTimestamp(config.getClock().instant())
            .repositoryId(repositoryId)
            .properties(config.getStaticProperties())
            .reference(ReferenceMapping.map(branch))
            .hash(hash.asString())
            .contentKey(contentKey)
            .content(content);
    if (user != null) {
      builder.eventInitiator(user.getName());
    }
    return builder.build();
  }

  protected Event newContentRemovedEvent(
      BranchName branch,
      Hash hash,
      ContentKey contentKey,
      String repositoryId,
      @Nullable Principal user) {
    ImmutableContentRemovedEvent.Builder builder =
        ImmutableContentRemovedEvent.builder()
            .type(EventType.CONTENT_REMOVED)
            .id(config.getIdGenerator().get())
            .eventCreationTimestamp(config.getClock().instant())
            .repositoryId(repositoryId)
            .properties(config.getStaticProperties())
            .reference(ReferenceMapping.map(branch))
            .hash(hash.asString())
            .contentKey(contentKey);
    if (user != null) {
      builder.eventInitiator(user.getName());
    }
    return builder.build();
  }
}
