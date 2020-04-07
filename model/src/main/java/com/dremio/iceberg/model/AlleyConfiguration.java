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

package com.dremio.iceberg.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * configuration object to tell a client how a server is configured.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableAlleyConfiguration.class)
@JsonDeserialize(as = ImmutableAlleyConfiguration.class)
public abstract class AlleyConfiguration {

  @JsonIgnore
  private static final String CURRENT_VERSION = "1.0";

  @Nullable
  public abstract String getDefaultTag();

  @Value.Default
  public String getVersion() {
    return CURRENT_VERSION;
  }

}
