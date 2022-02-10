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
package org.projectnessie.events;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

/** A commit event. */
@Value.Immutable
@JsonSerialize(as = ImmutableCommitEvent.class)
@JsonDeserialize(as = ImmutableCommitEvent.class)
@JsonTypeName("COMMIT_EVENT")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface CommitEvent extends Event {
  /** The reference committed against. */
  Reference getReference();

  /** This commit metadata. */
  CommitMeta getMetadata();

  /** The hash of the recent commit. */
  String getNewHash();

  /** List of operations committed. */
  List<Operation> getOperations();

  static ImmutableCommitEvent.Builder builder() {
    return ImmutableCommitEvent.builder();
  }
}
