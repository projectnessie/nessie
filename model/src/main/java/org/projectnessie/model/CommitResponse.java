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
package org.projectnessie.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Schema(type = SchemaType.OBJECT, title = "Commit Response")
@Value.Immutable
@JsonSerialize(as = ImmutableCommitResponse.class)
@JsonDeserialize(as = ImmutableCommitResponse.class)
public interface CommitResponse {

  static ImmutableCommitResponse.Builder builder() {
    return ImmutableCommitResponse.builder();
  }

  /**
   * Returns updated information about the branch where the commit was applied.
   *
   * <p>Specifically, the hash of the {@link Branch} will be the hash of the applied commit.
   */
  @NotNull
  Branch getTargetBranch();
}
