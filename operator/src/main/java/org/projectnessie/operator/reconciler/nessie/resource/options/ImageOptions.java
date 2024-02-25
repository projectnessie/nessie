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
package org.projectnessie.operator.reconciler.nessie.resource.options;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Nullable;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.sundr.builder.annotations.Buildable;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record ImageOptions(
    @JsonPropertyDescription(
            """
            The image repository. Optional; if unspecified, a default repository will be selected \
            depending on the type of container being created.""")
        @Nullable
        @jakarta.annotation.Nullable
        String repository,
    @JsonPropertyDescription(
            """
        The image tag to use. Defaults to "latest".""")
        @Default("latest")
        String tag,
    @JsonPropertyDescription(
            """
            The image pull policy to use. Defaults to "Always" if the tag is "latest" or \
            "latest-java", otherwise to "IfNotPresent".""")
        @Nullable
        @jakarta.annotation.Nullable
        PullPolicy pullPolicy,
    @JsonPropertyDescription(
            """
            The secret to use when pulling the image from private repositories. Optional. \
            See https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod.""")
        @Nullable
        @jakarta.annotation.Nullable
        LocalObjectReference pullSecretRef) {

  public static final String DEFAULT_NESSIE_REPOSITORY = "ghcr.io/projectnessie/nessie";

  public ImageOptions() {
    this(null, null, null, null);
  }

  public ImageOptions {
    tag = tag != null ? tag : "latest";
    if (pullPolicy == null) {
      pullPolicy =
          tag.equals("latest") || tag.equals("latest-java")
              ? PullPolicy.Always
              : PullPolicy.IfNotPresent;
    }
  }

  public enum PullPolicy {
    Always,
    Never,
    IfNotPresent
  }

  @JsonIgnore
  public String fullName(String defaultRepository) {
    return (repository != null ? repository : defaultRepository) + ":" + tag;
  }
}
