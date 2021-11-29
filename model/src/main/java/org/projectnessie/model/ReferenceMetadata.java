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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Schema(
    type = SchemaType.OBJECT,
    title = "ReferenceMetadata",
    description =
        "Only returned by the server when explicitly requested by the client and contains the following information:\n\n"
            + "- numCommitsAhead (number of commits ahead of the default branch)\n\n"
            + "- numCommitsBehind (number of commits behind the default branch)\n\n"
            + "- commitMetaOfHEAD (the commit metadata of the HEAD commit)\n\n"
            + "- commonAncestorHash (the hash of the common ancestor in relation to the default branch).\n\n"
            + "- numTotalCommits (the total number of commits in this reference).\n\n")
@Value.Immutable
@JsonSerialize(as = ImmutableReferenceMetadata.class)
@JsonDeserialize(as = ImmutableReferenceMetadata.class)
@JsonTypeName("REFERENCE_METADATA")
public interface ReferenceMetadata {

  @Nullable
  Integer numCommitsAhead();

  @Nullable
  Integer numCommitsBehind();

  @Nullable
  CommitMeta commitMetaOfHEAD();

  @Nullable
  String commonAncestorHash();

  @Nullable
  Long numTotalCommits();
}
