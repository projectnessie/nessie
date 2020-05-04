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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.OptionalLong;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.Ref;
import org.immutables.value.Value;

/**
 * api representation of an Iceberg Alley Tag/Branch.
 */
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableBranch.class)
@JsonDeserialize(as = ImmutableBranch.class)
public abstract class Branch implements Base {

  @Value.Default
  public String getName() {
    return getId(); //todo re-add uuid
  }

  public abstract String getId();

  @Value.Default
  public long getCreateMillis() {
    return Long.MIN_VALUE;
  }

  public abstract OptionalLong getExpireMillis();

  @Value.Default
  public boolean isDeleted() {
    return false;
  }

  @Value.Default
  public long getUpdateTime() {
    return Long.MIN_VALUE;
  }

  public static Branch fromGitRef(GitRef ref) {
    return ImmutableBranch.builder()
                          .id(ref.getId())
                          .isDeleted(ref.isDeleted())
                          .updateTime(ref.getUpdateTime())
                          .build();
  }

  public static Branch fromRef(Ref ref) {
    if (ref == null) {
      return null;
    }
    return ImmutableBranch.builder()
                          .id(ref.getName().replaceFirst(Constants.R_HEADS, ""))
                          .isDeleted(false)
                          .updateTime(Long.MIN_VALUE)
                          .build();
  }
}
