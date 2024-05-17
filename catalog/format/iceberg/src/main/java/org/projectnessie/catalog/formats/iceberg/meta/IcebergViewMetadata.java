/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.formats.iceberg.meta;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergViewMetadata.class)
@JsonDeserialize(as = ImmutableIcebergViewMetadata.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergViewMetadata {
  String viewUuid();

  int formatVersion();

  /**
   * The Iceberg view base location, usually something like {@code
   * s3://bucket1/warehouse/ns/view_<uuid>}.
   */
  @Nullable
  String location();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> properties();

  List<IcebergSchema> schemas();

  long currentVersionId();

  List<IcebergViewVersion> versions();

  List<IcebergViewHistoryEntry> versionLog();

  static Builder builder() {
    return ImmutableIcebergViewMetadata.builder();
  }

  @Value.Lazy
  @JsonIgnore
  default IcebergViewVersion currentVersion() {
    for (IcebergViewVersion version : versions()) {
      if (version.versionId() == currentVersionId()) {
        return version;
      }
    }
    throw new IllegalStateException(
        "No matching current view version for ID "
            + currentVersionId()
            + ", has IDs "
            + versions().stream()
                .map(v -> Long.toString(v.versionId()))
                .collect(Collectors.joining(", ")));
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergViewMetadata instance);

    @CanIgnoreReturnValue
    Builder viewUuid(String viewUuid);

    @CanIgnoreReturnValue
    Builder formatVersion(int formatVersion);

    @CanIgnoreReturnValue
    Builder location(String location);

    @CanIgnoreReturnValue
    Builder putProperty(String key, String value);

    @CanIgnoreReturnValue
    Builder putProperty(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder properties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder addSchema(IcebergSchema element);

    @CanIgnoreReturnValue
    Builder addSchemas(IcebergSchema... elements);

    @CanIgnoreReturnValue
    Builder schemas(Iterable<? extends IcebergSchema> elements);

    @CanIgnoreReturnValue
    Builder addAllSchemas(Iterable<? extends IcebergSchema> elements);

    @CanIgnoreReturnValue
    Builder currentVersionId(long currentVersionId);

    @CanIgnoreReturnValue
    Builder addVersion(IcebergViewVersion element);

    @CanIgnoreReturnValue
    Builder addVersions(IcebergViewVersion... elements);

    @CanIgnoreReturnValue
    Builder versions(Iterable<? extends IcebergViewVersion> elements);

    @CanIgnoreReturnValue
    Builder addAllVersions(Iterable<? extends IcebergViewVersion> elements);

    @CanIgnoreReturnValue
    Builder addVersionLog(IcebergViewHistoryEntry element);

    @CanIgnoreReturnValue
    Builder addVersionLog(IcebergViewHistoryEntry... elements);

    @CanIgnoreReturnValue
    Builder versionLog(Iterable<? extends IcebergViewHistoryEntry> elements);

    @CanIgnoreReturnValue
    Builder addAllVersionLog(Iterable<? extends IcebergViewHistoryEntry> elements);

    IcebergViewMetadata build();
  }
}
