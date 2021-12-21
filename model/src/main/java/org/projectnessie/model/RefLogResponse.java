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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Value.Immutable
@Schema(type = SchemaType.OBJECT, title = "RefLogResponse")
@JsonSerialize(as = ImmutableRefLogResponse.class)
@JsonDeserialize(as = ImmutableRefLogResponse.class)
public interface RefLogResponse extends PaginatedResponse {

  @NotNull
  List<RefLogResponseEntry> getLogEntries();

  @Value.Immutable
  @Schema(type = SchemaType.OBJECT, title = "RefLogResponseEntry")
  @JsonSerialize(as = ImmutableRefLogResponseEntry.class)
  @JsonDeserialize(as = ImmutableRefLogResponseEntry.class)
  interface RefLogResponseEntry {
    static ImmutableRefLogResponseEntry.Builder builder() {
      return ImmutableRefLogResponseEntry.builder();
    }

    /** Reflog id of the current entry. */
    @NotNull
    String getRefLogId();

    /** Reference on which current operation is executed. */
    @NotNull
    String getRefName();

    /** Reference type can be 'Branch' or 'Tag'. */
    @NotNull
    String getRefType();

    /** Output commit hash of the operation. */
    @NotNull
    String getCommitHash();

    /** Parent reflog id of the current entry. */
    @NotNull
    String getParentRefLogId();

    /** Time in microseconds since epoch. */
    @NotNull
    long getOperationTime();

    /** Operation String mapped to ENUM in {@code RefLogEntry.Operation} of 'persist.proto' file. */
    @NotNull
    String getOperation();

    /**
     * Single hash in case of MERGE. One or more hashes in case of TRANSPLANT. Empty list for other
     * operations.
     */
    @NotNull
    List<String> getSourceHashes();
  }
}
