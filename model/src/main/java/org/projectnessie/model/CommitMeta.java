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

import java.util.Map;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableCommitMeta.class)
@JsonDeserialize(as = ImmutableCommitMeta.class)
public abstract class CommitMeta {

  /**
   * Hash of this commit.
   *
   * <p>This is not known at creation time and is only valid when reading the log.
   */
  @Nullable
  public abstract String getHash();

  /**
   * The entity performing this commit/transaction.
   *
   * <p>This is the logged in user/account who performs this action. Populated on the server.
   * Nessie will return an error if this is populated by the client side.
   */
  @Nullable
  public abstract String getCommitter();

  /**
   * The author of a commit. This is the original committer.
   */
  @Nullable
  public abstract String getAuthor();

  /**
   * Authorizer of this action.
   *
   * <p>For example if the user who did the transaction is a service account this could be populated by the person who started the job.
   */
  @Nullable
  public abstract String getSignedOffBy();

  /**
   * Message describing this commit. Should conform to Git style.
   *
   * <p>Like github if this message is in markdown it may be displayed cleanly in the UI.
   */
  public abstract String getMessage();

  /**
   * Commit time in UTC. Set by the server.
   */
  @Nullable
  public abstract Long getCommitTime();

  /**
   * Original commit time in UTC. Set by the server.
   */
  @Nullable
  public abstract Long getAuthorTime();

  /**
   * Set of properties to help further identify this commit.
   *
   * <p>examples are spark id, the client type (eg iceberg, delta etc), application or job names, hostnames etc
   */
  public abstract Map<String, String> getProperties();

  public ImmutableCommitMeta.Builder toBuilder() {
    return ImmutableCommitMeta.builder().from(this);
  }

  public static ImmutableCommitMeta.Builder builder() {
    return ImmutableCommitMeta.builder();
  }

  public static CommitMeta fromMessage(String message) {
    return ImmutableCommitMeta.builder().message(message).build();
  }
}
