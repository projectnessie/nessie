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

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Api representation of an Nessie Tag/Branch. This object is akin to a Ref in Git terminology.
 */
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableBranch.class)
@JsonDeserialize(as = ImmutableBranch.class)
public abstract class Branch implements Reference {

  /**
   * Flag to denote the deletion of this branch. Has not been purged from backend.
   *
   * <p>
   *   todo remove
   * </p>
   */
  @Deprecated
  @Value.Default
  public boolean isDeleted() {
    return false;
  }

}
