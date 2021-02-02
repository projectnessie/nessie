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

package com.dremio.nessie.tiered.builder.base;

import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.versioned.Key.Mutation;
import com.dremio.nessie.versioned.store.Id;

/**
 * Abstract implementation of {@link L1}, all methods return {@code this}.
 * <p>All {@code Abstract*} classes in this package are meant to ease consumption of values loaded
 * via {@link com.dremio.nessie.versioned.VersionStore}, so users do not have to implement every
 * method.</p>
 * <p>{@link Stream}s passed into the default method implementations are fully consumed when invoked.</p>
 */
public abstract class AbstractL1 extends AbstractBaseValue<L1> implements L1 {
  @Override
  public L1 commitMetadataId(Id id) {
    return this;
  }

  @Override
  public L1 ancestors(Stream<Id> ids) {
    ids.forEach(ignored -> {});
    return this;
  }

  @Override
  public L1 children(Stream<Id> ids) {
    ids.forEach(ignored -> {});
    return this;
  }

  @Override
  public L1 keyMutations(Stream<Mutation> keyMutations) {
    keyMutations.forEach(ignored -> {});
    return this;
  }

  @Override
  public L1 incrementalKeyList(Id checkpointId, int distanceFromCheckpoint) {
    return this;
  }

  @Override
  public L1 completeKeyList(Stream<Id> fragmentIds) {
    fragmentIds.forEach(ignored -> {});
    return this;
  }
}
