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

package com.dremio.nessie.model;

import java.util.Set;

import javax.annotation.Nullable;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * API representation of a User of Nessie.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableUser.class)
@JsonDeserialize(as = ImmutableUser.class)
public abstract class User implements Base {

  @Value.Redacted
  @Value.Default
  public String getPassword() {
    return "$EMPTY$";
  }

  @Value.Default
  public long getCreateMillis() {
    return Long.MIN_VALUE;
  }

  @Value.Default
  public boolean isActive() {
    return true;
  }

  @Nullable
  public abstract String getEmail();

  public abstract Set<String> getRoles();

  public abstract String getId();

  @Value.Default
  public long getUpdateMillis() {
    return Long.MIN_VALUE;
  }

}
