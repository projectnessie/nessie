/*
 * Copyright (C) 2023 Dremio
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
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.immutables.value.Value;

@Schema(type = SchemaType.OBJECT, title = "UpdateRepositoryConfigResponse")
@Tag(name = "v2")
@Value.Immutable
@JsonSerialize(as = ImmutableUpdateRepositoryConfigResponse.class)
@JsonDeserialize(as = ImmutableUpdateRepositoryConfigResponse.class)
public interface UpdateRepositoryConfigResponse {
  /**
   * The previous value of the updated repository config. If no previous value for the same
   * repository config type exists, this value will be {@code null}.
   */
  @Nullable
  @jakarta.annotation.Nullable
  @Schema(
      implementation = RepositoryConfig.class,
      title = "The previous state of the repository configuration object.",
      description =
          "When a repository configuration for the same type as in the request object did not exist, "
              + "the response object will be null. Otherwise, if the configuration was updated, the old "
              + "value will be returned.")
  RepositoryConfig getPrevious();
}
