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
package org.projectnessie.events.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/** Metadata about a commit. */
@Value.Immutable
@JsonSerialize(as = ImmutableCommitMeta.class)
@JsonDeserialize(as = ImmutableCommitMeta.class)
public interface CommitMeta {

  /** The user or account who performed this action. */
  String getCommitter();

  /** The commit first author, or empty if no author information is available. */
  @Value.Derived
  @JsonIgnore
  default Optional<String> getAuthor() {
    return getAuthors().stream().findFirst();
  }

  /** The commit authors. */
  List<String> getAuthors();

  /**
   * The commit first sign-off, or empty if no sign-off information is available. This is usually
   * the person who approved or authorized the commit.
   */
  @Value.Derived
  @JsonIgnore
  default Optional<String> getSignOff() {
    return getSignOffs().stream().findFirst();
  }

  /** The commit sign-offs. */
  List<String> getSignOffs();

  /** The commit message. */
  String getMessage();

  /** The commit time. */
  Instant getCommitTime();

  /** Original commit time. */
  Instant getAuthorTime();

  /** Single-valued properties of this commit. */
  @Value.Lazy
  @JsonIgnore
  default Map<String, String> getProperties() {
    return getMultiProperties().entrySet().stream()
        .filter(e -> !e.getValue().isEmpty())
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(0)));
  }

  /** Multivalued properties of this commit. */
  Map<String, List<String>> getMultiProperties();
}
