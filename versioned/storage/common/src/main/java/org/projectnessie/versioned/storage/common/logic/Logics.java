/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.logic;

import org.projectnessie.versioned.storage.common.persist.Persist;

public final class Logics {
  private Logics() {}

  public static ReferenceLogic referenceLogic(Persist persist) {
    return new ReferenceLogicImpl(persist);
  }

  public static CommitLogic commitLogic(Persist persist) {
    return new CommitLogicImpl(persist);
  }

  public static RepositoryLogic repositoryLogic(Persist persist) {
    return new RepositoryLogicImpl(persist);
  }

  public static IndexesLogic indexesLogic(Persist persist) {
    return new IndexesLogicImpl(persist);
  }

  public static StringLogic stringLogic(Persist persist) {
    return new StringLogicImpl(persist);
  }

  public static ConsistencyLogic consistencyLogic(Persist persist) {
    return new ConsistencyLogicImpl(persist);
  }
}
