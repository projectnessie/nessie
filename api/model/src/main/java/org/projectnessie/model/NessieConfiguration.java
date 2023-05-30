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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;
import org.projectnessie.model.CommitMeta.InstantDeserializer;
import org.projectnessie.model.CommitMeta.InstantSerializer;
import org.projectnessie.model.ser.Views;

/** configuration object to tell a client how a server is configured. */
@Schema(
    type = SchemaType.OBJECT,
    title = "NessieConfiguration",
    description = "Configuration object to tell a client how a server is configured.")
@Value.Immutable
@JsonSerialize(as = ImmutableNessieConfiguration.class)
@JsonDeserialize(as = ImmutableNessieConfiguration.class)
public abstract class NessieConfiguration {

  /**
   * The name of the default branch that the server will use unless an explicit branch was specified
   * as an API call parameter.
   */
  @Nullable
  @jakarta.annotation.Nullable
  @Size
  @jakarta.validation.constraints.Size(min = 1)
  public abstract String getDefaultBranch();

  /**
   * The minimum API version supported by the server.
   *
   * <p>API versions are numbered sequentially, as they are developed.
   */
  @JsonView(Views.V2.class)
  @Value.Default
  public int getMinSupportedApiVersion() {
    return 1;
  }

  /**
   * The maximum API version supported by the server.
   *
   * <p>API versions are numbered sequentially, as they are developed.
   */
  public abstract int getMaxSupportedApiVersion();

  /**
   * The actual API version that was used to handle the REST request to the configuration endpoint.
   *
   * <p>If this value is 0, then the server does not support returning the actual API version.
   * Otherwise, this value is guaranteed to be between {@link #getMinSupportedApiVersion()} and
   * {@link #getMaxSupportedApiVersion()} (inclusive).
   */
  @JsonView(Views.V2.class)
  @Value.Default
  public int getActualApiVersion() {
    return 0;
  }

  /**
   * Semver version representing the behavior of the Nessie server.
   *
   * <p>Additional functionality might be added to Nessie servers within a "spec major version" in a
   * non-breaking way. Clients are encouraged to check the spec version when using such added
   * functionality.
   */
  @Schema(
      description =
          "Semver version representing the behavior of the Nessie server.\n"
              + "\nAdditional functionality might be added to Nessie servers within a \"spec major version\" "
              + "in a non-breaking way. Clients are encouraged to check the spec version when using such "
              + "added functionality.")
  @JsonView(Views.V2.class)
  @Nullable
  @jakarta.annotation.Nullable
  public abstract String getSpecVersion();

  /**
   * The so called no-ancestor-hash defines the commit-ID of the "beginning of time" in the
   * repository. The very first commit will have the value returned by this function as its parent
   * commit-ID. A commit with this value does never exist.
   */
  @JsonView(Views.V2.class)
  @JsonInclude(Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  public abstract String getNoAncestorHash();

  /**
   * Timestamp when the repository has been created.
   *
   * <p>The value is only returned, if the server supports this attribute.
   */
  @JsonView(Views.V2.class)
  @JsonInclude(Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  public abstract Instant getRepositoryCreationTimestamp();

  /**
   * Timestamp of the oldest possible commit in the repository.
   *
   * <p>For new repositories, this is likely the same as {@link #getRepositoryCreationTimestamp()}.
   * For imported repositories, this shall be the timestamp of the oldest commit.
   *
   * <p>The value is only returned, if the server supports this attribute.
   */
  @JsonView(Views.V2.class)
  @JsonInclude(Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  public abstract Instant getOldestPossibleCommitTimestamp();

  /** Additional properties, currently undefined and always empty (not present in JSON). */
  @JsonView(Views.V2.class)
  @JsonInclude(Include.NON_EMPTY)
  public abstract Map<String, String> getAdditionalProperties();

  public static NessieConfiguration getBuiltInConfig() {
    return NessieConfigurationHolder.NESSIE_API_SPEC;
  }
}
