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
package com.dremio.nessie.tiered.builder;

import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Id;

/**
 * Interface to create an L1 Builder. To be implemented by each {@link Store} implementation.
 *
 * @param <T> The concrete type built by this builder.
 */
public interface L1Builder<T> {

  /**
   * Construct the expected object.
   * @return The built object.
   */
  T build();

  L1Builder<T> commitMetadataId(Id id);

  IncrementalKeyListBuilder<T> incrementalKeyList();

  CompleteKeyListBuilder<T> completeKeyList();

  ParentListBuilder<T> parentList();

  L1Builder<T> id(Id id);

  TreeBuilder<T> treeBuilder();

  interface IncrementalKeyListBuilder<T> {
    IncrementalKeyListBuilder<T> previousCheckPoint(Id id);
    IncrementalKeyListBuilder<T> distanceFromCheckpointCommit(int i);
    IncrementalKeyListBuilder<T> keyAddition(Key key);
    IncrementalKeyListBuilder<T> keyRemoval(Key key);
    L1Builder<T> build();
  }

  interface TreeBuilder<T> {
    TreeBuilder<T> set(int position, Id id);
    L1Builder<T> build();
  }

  interface CompleteKeyListBuilder<T> {
    CompleteKeyListBuilder<T> addFragment(Id id);
    CompleteKeyListBuilder<T> keyAddition(Key key);
    CompleteKeyListBuilder<T> keyRemoval(Key key);
    L1Builder<T> build();
  }

  interface ParentListBuilder<T> {
    ParentListBuilder<T> addAnscestor(Id id);
    L1Builder<T> build();
  }

}
