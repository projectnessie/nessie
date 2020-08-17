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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.WithHash;

final class CommitsIterator<ValueT, MetadataT> implements Iterator<WithHash<Commit<ValueT, MetadataT>>> {
  private final Function<Hash, Commit<ValueT, MetadataT>> commitAccessor;

  private WithHash<Commit<ValueT, MetadataT>> current;
  private Hash ancestor = Commit.NO_ANCESTOR;

  CommitsIterator(Function<Hash, Commit<ValueT, MetadataT>> commitAccessor, Hash initialHash) {
    this.commitAccessor = commitAccessor;
    this.ancestor = initialHash;
  }

  @Override
  public boolean hasNext() {
    if (current != null) {
      return true;
    }

    if (ancestor == Commit.NO_ANCESTOR) {
      return false;
    }

    final Commit<ValueT, MetadataT> commit = commitAccessor.apply(ancestor);
    if (commit == null) {
      throw new IllegalStateException("Missing entry for commit " + ancestor.asString());
    }
    current = WithHash.of(ancestor, commit);
    ancestor = current.getValue().getAncestor();

    return true;
  }

  @Override
  public WithHash<Commit<ValueT, MetadataT>> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final WithHash<Commit<ValueT, MetadataT>> result = current;
    current = null;
    return result;
  }
}
