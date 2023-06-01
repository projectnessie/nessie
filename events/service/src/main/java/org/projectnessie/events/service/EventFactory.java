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
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
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
    return ImmutableCommitEvent.builder()
        .id(config.getIdGenerator().get())
        .eventCreationTimestamp(config.getClock().instant())
        .eventInitiator(extractName(user))
        .repositoryId(repositoryId)
        .properties(config.getStaticProperties())
        .reference(ReferenceMapping.map(targetBranch))
        .hashBefore(Objects.requireNonNull(commit.getParentHash()).asString())
        .hashAfter(commit.getHash().asString())
        .commitMeta(
            ImmutableCommitMeta.builder()
                .committer(Objects.requireNonNull(commitMeta.getCommitter()))
                .authors(commitMeta.getAllAuthors())
                .allSignedOffBy(commitMeta.getAllSignedOffBy())
                .message(commitMeta.getMessage())
                .commitTimestamp(Objects.requireNonNull(commitMeta.getCommitTime()))
                .authorTimestamp(Objects.requireNonNull(commitMeta.getAuthorTime()))
                .allProperties(commitMeta.getAllProperties())
                .build())
        .build();
  }

  protected Event newMergeEvent(
      MergeResult<Commit> result, String repositoryId, @Nullable Principal user) {
    assert result.getResultType() == ResultType.MERGE;
    String commonAncestorHash = Objects.requireNonNull(result.getCommonAncestor()).asString();
    return ImmutableMergeEvent.builder()
        .id(config.getIdGenerator().get())
        .eventCreationTimestamp(config.getClock().instant())
        .eventInitiator(extractName(user))
        .repositoryId(repositoryId)
        .properties(config.getStaticProperties())
        .sourceReference(ReferenceMapping.map(result.getSourceRef()))
        .targetReference(ReferenceMapping.map(result.getTargetBranch()))
        .hashBefore(result.getEffectiveTargetHash().asString())
        .hashAfter(Objects.requireNonNull(result.getResultantTargetHash()).asString())
        .commonAncestorHash(commonAncestorHash)
        .build();
  }

  protected Event newTransplantEvent(
      MergeResult<Commit> result, String repositoryId, @Nullable Principal user) {
    assert result.getResultType() == ResultType.TRANSPLANT;
    return ImmutableTransplantEvent.builder()
        .id(config.getIdGenerator().get())
        .eventCreationTimestamp(config.getClock().instant())
        .eventInitiator(extractName(user))
        .repositoryId(repositoryId)
        .properties(config.getStaticProperties())
        .sourceReference(ReferenceMapping.map(result.getSourceRef()))
        .targetReference(ReferenceMapping.map(result.getTargetBranch()))
        .hashBefore(result.getEffectiveTargetHash().asString())
        .hashAfter(Objects.requireNonNull(result.getResultantTargetHash()).asString())
        .build();
  }

  protected Event newReferenceCreatedEvent(
      ReferenceCreatedResult result, String repositoryId, @Nullable Principal user) {
    return ImmutableReferenceCreatedEvent.builder()
        .id(config.getIdGenerator().get())
        .eventCreationTimestamp(config.getClock().instant())
        .eventInitiator(extractName(user))
        .repositoryId(repositoryId)
        .properties(config.getStaticProperties())
        .reference(ReferenceMapping.map(result.getNamedRef()))
        .hashAfter(result.getHash().asString())
        .build();
  }

  protected Event newReferenceUpdatedEvent(
      ReferenceAssignedResult result, String repositoryId, @Nullable Principal user) {
    return ImmutableReferenceUpdatedEvent.builder()
        .id(config.getIdGenerator().get())
        .eventCreationTimestamp(config.getClock().instant())
        .eventInitiator(extractName(user))
        .repositoryId(repositoryId)
        .properties(config.getStaticProperties())
        .reference(ReferenceMapping.map(result.getNamedRef()))
        .hashBefore(result.getPreviousHash().asString())
        .hashAfter(result.getCurrentHash().asString())
        .build();
  }

  protected Event newReferenceDeletedEvent(
      ReferenceDeletedResult result, String repositoryId, @Nullable Principal user) {
    return ImmutableReferenceDeletedEvent.builder()
        .id(config.getIdGenerator().get())
        .eventCreationTimestamp(config.getClock().instant())
        .eventInitiator(extractName(user))
        .repositoryId(repositoryId)
        .properties(config.getStaticProperties())
        .reference(ReferenceMapping.map(result.getNamedRef()))
        .hashBefore(result.getHash().asString())
        .build();
  }

  protected Event newContentStoredEvent(
      BranchName branch,
      Hash hash,
      Instant commitTimestamp,
      ContentKey contentKey,
      Content content,
      String repositoryId,
      @Nullable Principal user) {
    return ImmutableContentStoredEvent.builder()
        .type(EventType.CONTENT_STORED)
        .id(config.getIdGenerator().get())
        .eventCreationTimestamp(config.getClock().instant())
        .eventInitiator(extractName(user))
        .repositoryId(repositoryId)
        .properties(config.getStaticProperties())
        .reference(ReferenceMapping.map(branch))
        .hash(hash.asString())
        .contentKey(contentKey)
        .content(content)
        .commitCreationTimestamp(commitTimestamp)
        .build();
  }

  protected Event newContentRemovedEvent(
      BranchName branch,
      Hash hash,
      Instant commitTimestamp,
      ContentKey contentKey,
      String repositoryId,
      @Nullable Principal user) {
    return ImmutableContentRemovedEvent.builder()
        .type(EventType.CONTENT_REMOVED)
        .id(config.getIdGenerator().get())
        .eventCreationTimestamp(config.getClock().instant())
        .eventInitiator(extractName(user))
        .repositoryId(repositoryId)
        .properties(config.getStaticProperties())
        .reference(ReferenceMapping.map(branch))
        .hash(hash.asString())
        .contentKey(contentKey)
        .commitCreationTimestamp(commitTimestamp)
        .build();
  }

  private static Optional<String> extractName(@Nullable Principal user) {
    return user == null || user.getName() == null || user.getName().isEmpty()
        ? Optional.empty()
        : Optional.of(user.getName());
  }
}
