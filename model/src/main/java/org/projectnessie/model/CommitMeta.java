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

import java.util.Collections;
import java.util.Map;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableCommitMeta.class)
@JsonDeserialize(as = ImmutableCommitMeta.class)
public abstract class CommitMeta {

  @Nullable
  public abstract String getHash();

  @Value.Default
  public String getCommiter() {
    return System.getProperty("user.name", "unknown");
  }

  /**
   * according to git the author == the committer unless set otherwise.
   */
  @Nullable
  public abstract String getAuthor();

  @Nullable
  public abstract String getAuthorEmail();

  @Nullable
  public abstract String getSignedOffBy();

  @Nullable
  public abstract String getSignedOffByEmail();

  @Nullable
  public abstract String getEmail();

  public abstract String getMessage();

  @Value.Default
  public Long getCommitTime() {
    return System.currentTimeMillis();
  }

  /**
   * according to git the author time == the commit time unless set otherwise.
   */
  @Nullable
  public abstract Long getAuthorTime();

  @Value.Default
  public Map<String, String> getProperties() {
    return Collections.emptyMap();
  }

  public ImmutableCommitMeta.Builder toBuilder() {
    return ImmutableCommitMeta.builder().from(this);
  }
}
