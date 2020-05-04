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
import javax.annotation.Nullable;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.Ref;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableGitObject.class)
@JsonDeserialize(as = ImmutableGitObject.class)
public abstract class GitObject implements GitContainer {

  public abstract AnyObjectId getObjectId();

  public abstract byte[] getData();

  public abstract int getType();

  public abstract boolean isDeleted();

  public abstract long getUpdateTime();

  public Ref getRef() {
    return null;
  }

  public Ref getTargetRef() {
    return null;
  }

  @Override
  public String getId() {
    return getObjectId().name();
  }

  public boolean isRef() {
    return false;
  }
}
