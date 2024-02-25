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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.generator.annotation.Default;
import io.fabric8.generator.annotation.Required;
import io.sundr.builder.annotations.Buildable;

@Buildable(builderPackage = "io.fabric8.kubernetes.api.builder", editableEnabled = false)
@JsonInclude(Include.NON_NULL)
public record AwsCredentials(
    @JsonPropertyDescription(
            "The name of the secret to pull the value from. The secret must be in the same namespace as the Nessie resource.")
        @Required
        String secret,
    @JsonPropertyDescription("The secret key containing the access key id.")
        @Default("access_key_id")
        String accessKeyId,
    @JsonPropertyDescription("The secret key containing the secret access key.")
        @Default("secret_access_key")
        String secretAccessKey) {

  public AwsCredentials {
    secret = secret != null ? secret : "awscreds";
    accessKeyId = accessKeyId != null ? accessKeyId : "access_key_id";
    secretAccessKey = secretAccessKey != null ? secretAccessKey : "secret_access_key";
  }
}
