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
package org.projectnessie.versioned;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ImmutableCommitMeta;

public class DefaultMetadataRewriter implements MetadataRewriter<CommitMeta> {
  private final String committer;
  private final Instant now;
  private final CommitMeta commitMetaOverride;
  private final IntFunction<String> squashMessage;

  public DefaultMetadataRewriter(
      String committer,
      Instant now,
      CommitMeta commitMetaOverride,
      IntFunction<String> squashMessage) {
    this.committer = committer;
    this.now = now;
    this.commitMetaOverride = commitMetaOverride;
    this.squashMessage = squashMessage;
  }

  private CommitMeta buildCommitMeta(
      ImmutableCommitMeta.Builder metaBuilder, Supplier<String> defaultMessage) {

    CommitMeta pre = metaBuilder.message("").build();

    if (hasAuthors(commitMetaOverride)) {
      metaBuilder.allAuthors(emptyList());
      copyAuthors(commitMetaOverride, metaBuilder::addAllAuthors);
    } else if (!hasAuthors(pre) && committer != null) {
      metaBuilder.allAuthors(singletonList(committer));
    }

    if (commitMetaOverride != null && !commitMetaOverride.getAllSignedOffBy().isEmpty()) {
      metaBuilder.allSignedOffBy(commitMetaOverride.getAllSignedOffBy());
    }

    if (commitMetaOverride != null && commitMetaOverride.getAuthorTime() != null) {
      metaBuilder.authorTime(commitMetaOverride.getAuthorTime());
    } else if (pre.getAuthorTime() == null) {
      metaBuilder.authorTime(now);
    }

    if (commitMetaOverride != null && !commitMetaOverride.getAllProperties().isEmpty()) {
      metaBuilder.allProperties(commitMetaOverride.getAllProperties());
    }

    if (commitMetaOverride != null && !commitMetaOverride.getMessage().isEmpty()) {
      metaBuilder.message(commitMetaOverride.getMessage());
    } else {
      metaBuilder.message(defaultMessage.get());
    }

    if (committer != null) {
      metaBuilder.committer(committer);
    }

    return metaBuilder.commitTime(now).build();
  }

  @Override
  public CommitMeta rewriteSingle(CommitMeta metadata) {
    return buildCommitMeta(
        CommitMeta.builder().from(metadata).hash(null).parentCommitHashes(emptyList()),
        metadata::getMessage);
  }

  @Override
  public CommitMeta squash(List<CommitMeta> metadata, int numCommits) {
    Optional<String> msg = Optional.ofNullable(squashMessage.apply(numCommits));

    if (numCommits == 1 && !msg.isPresent()) {
      return rewriteSingle(metadata.get(0));
    }

    Map<String, String> newProperties = new HashMap<>();
    Set<String> authors = new LinkedHashSet<>();
    Instant authorTime = null;
    for (CommitMeta commitMeta : metadata) {
      newProperties.putAll(commitMeta.getProperties());
      copyAuthors(commitMeta, authors::add);

      Instant metaAuthorTime = commitMeta.getAuthorTime();
      if (authorTime == null
          || (metaAuthorTime != null && authorTime.compareTo(metaAuthorTime) < 0)) {
        authorTime = commitMeta.getAuthorTime();
      }
    }

    ImmutableCommitMeta.Builder newMetaBuilder =
        CommitMeta.builder().properties(newProperties).allAuthors(authors).authorTime(authorTime);

    return buildCommitMeta(
        newMetaBuilder,
        () ->
            msg.orElseGet(
                () -> {
                  StringBuilder newMessage = new StringBuilder();
                  for (CommitMeta commitMeta : metadata) {
                    if (newMessage.length() > 0) {
                      newMessage.append("\n---------------------------------------------\n");
                    }
                    newMessage.append(commitMeta.getMessage());
                  }
                  return newMessage.toString();
                }));
  }

  private static void copyAuthors(CommitMeta commitMeta, Consumer<String> addAuthor) {
    for (String author : commitMeta.getAllAuthors()) {
      if (author != null && !author.isEmpty()) {
        addAuthor.accept(author);
      }
    }
  }

  private static boolean hasAuthors(CommitMeta commitMeta) {
    if (commitMeta != null) {
      for (String author : commitMeta.getAllAuthors()) {
        if (author != null && !author.isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }
}
