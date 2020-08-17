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
package com.dremio.nessie.versioned.memory;

import static java.util.Objects.requireNonNull;

import java.util.List;

import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Operation;
import com.google.common.collect.ImmutableList;

/**
 * Commit class.
 *
 * @param <ValueT> commit value
 * @param <MetadataT> commit metadata
 */
class Commit<ValueT, MetadataT> {
  public static final Hash NO_ANCESTOR = Hash.of("");

  private final Hash ancestor;
  private final MetadataT metadata;
  private final List<Operation<ValueT>> operations;

  public Commit(Hash ancestor, MetadataT metadata, List<Operation<ValueT>> operations) {
    this.ancestor = requireNonNull(ancestor);
    this.metadata = requireNonNull(metadata);
    this.operations = ImmutableList.copyOf(requireNonNull(operations));
  }

  public Hash getAncestor() {
    return ancestor;
  }

  public MetadataT getMetadata() {
    return metadata;
  }

  public List<Operation<ValueT>> getOperations() {
    return operations;
  }
}
