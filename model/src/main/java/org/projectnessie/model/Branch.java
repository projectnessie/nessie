/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.model;

import static org.projectnessie.model.Validation.validateReferenceName;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Collections;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

/** API representation of a Nessie Branch. This object is akin to a Ref in Git terminology. */
@Value.Immutable
@Schema(type = SchemaType.OBJECT, title = "Branch")
@JsonSerialize(as = ImmutableBranch.class)
@JsonDeserialize(as = ImmutableBranch.class)
@JsonTypeName("BRANCH")
public interface Branch extends Reference {
  String NUM_COMMITS_AHEAD = "numCommitsAhead";
  String NUM_COMMITS_BEHIND = "numCommitsBehind";
  String HEAD_COMMIT_META = "headCommitMeta";
  String COMMON_ANCESTOR_HASH = "commonAncestorHash";

  /**
   * Validation rule using {@link org.projectnessie.model.Validation#validateReferenceName(String)}.
   */
  @Value.Check
  default void checkName() {
    validateReferenceName(getName());
  }

  static ImmutableBranch.Builder builder() {
    return ImmutableBranch.builder();
  }

  static Branch of(String name, String hash) {
    return builder().name(name).hash(hash).build();
  }

  /**
   * Returns a map of metadata properties that describe this branch. Note that these properties
   * <b>can be added</b> by the server when a {@link Branch} instance is returned. A returned {@link
   * Branch} instance can then have the following keys:
   *
   * <ul>
   *   <li>{@value Branch#NUM_COMMITS_AHEAD}: number of commits ahead of the default branch
   *   <li>{@value Branch#NUM_COMMITS_BEHIND}: number of commits behind the default branch
   *   <li>{@value Branch#HEAD_COMMIT_META}: the commit metadata of the HEAD commit
   *   <li>{@value Branch#COMMON_ANCESTOR_HASH}: the hash of the common ancestor in relation to the
   *       default branch.
   * </ul>
   *
   * @return A map of metadata properties that describe this branch. Note that these properties
   *     <b>can be added</b> by the server when a {@link Branch} instance is returned.
   */
  @Value.Default
  @JsonInclude(Include.NON_EMPTY)
  default Map<String, Object> metadataProperties() {
    return Collections.emptyMap();
  }
}
