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
import javax.annotation.Nullable;
import javax.validation.constraints.Size;
import org.immutables.value.Value;

/** configuration object to tell a client how a server is configured. */
@Value.Immutable
@JsonSerialize(as = ImmutableNessieConfiguration.class)
@JsonDeserialize(as = ImmutableNessieConfiguration.class)
public abstract class NessieConfiguration {

  /**
   * The name of the default branch that the server will use unless an explicit branch was specified
   * as an API call parameter.
   */
  @Nullable
  @Size(min = 1)
  public abstract String getDefaultBranch();

  /**
   * The maximum API version supported by the server.
   *
   * <p>API versions are numbered sequentially, as they are developed.
   */
  public abstract int getMaxSupportedApiVersion();
}
