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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Joiner;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableCommitMeta.class)
@JsonDeserialize(as = ImmutableCommitMeta.class)
public abstract class CommitMeta {

  private static final Joiner SEMI = Joiner.on(";");

  public String toMessage() {
    return SEMI.join(comment(), action().toString(), branch(), Integer.toString(changes()));
  }

  public enum Action {
    CREATE_BRANCH,
    COMMIT,
    DELETE_BRANCH,
    MERGE,
    FORCE_MERGE,
    CHERRY_PICK
  }

  public abstract String commiter();

  public abstract String email();

  public abstract String comment();

  public abstract Action action();

  public abstract String branch();

  public abstract int changes();
}
