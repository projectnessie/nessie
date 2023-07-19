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
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.immutables.value.Value;

@Schema(type = SchemaType.OBJECT, title = "UpdateRepositoryConfigRequest")
@Tag(name = "v2")
@Value.Immutable
@JsonSerialize(as = ImmutableUpdateRepositoryConfigRequest.class)
@JsonDeserialize(as = ImmutableUpdateRepositoryConfigRequest.class)
public interface UpdateRepositoryConfigRequest {
  /**
   * The previous value of the updated repository configuration object. This will be {@code null},
   * if the repository configuration was created.
   */
  RepositoryConfig getConfig();
}
