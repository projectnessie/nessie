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
import java.util.Objects;
import javax.annotation.Nullable;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Ref;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableGitObject.class)
@JsonDeserialize(as = ImmutableGitObject.class)
public abstract class GitRef implements GitContainer {

  private static final byte[] EMPTY = new byte[]{-1};

  public AnyObjectId getObjectId() {
    return getRef().isSymbolic() ? Objects.requireNonNull(getTargetRef()).getObjectId()
      : getRef().getObjectId();
  }

  public byte[] getData() {
    return EMPTY;
  }

  public int getType() {
    return -1;
  }

  public abstract boolean isDeleted();

  public abstract long getUpdateTime();

  public abstract Ref getRef();

  @Nullable
  public abstract Ref getTargetRef();

  @Override
  public String getId() {
    return getRef().getName();
  }

  public boolean isRef() {
    return true;
  }
}
