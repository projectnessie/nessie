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

final class CommitsIterator<MetadataT> implements Iterator<WithHash<MetadataT>> {
  private final Function<Hash, Commit<?, MetadataT>> commitAccessor;

  private WithHash<MetadataT> next;
  private Commit<?, MetadataT> nextCommit;
  private Commit<?, MetadataT> previousCommit;

  CommitsIterator(Function<Hash, Commit<?, MetadataT>> commitAccessor, Hash initialHash) {
    this.commitAccessor = commitAccessor;
    this.nextCommit = commitAccessor.apply(initialHash);
    next = WithHash.of(initialHash, nextCommit.getMetadata());
  }

  @Override
  public boolean hasNext() {
    if (next != null) {
      return true;
    }

    final Hash ancestor = previousCommit.getAncestor();
    if (ancestor == Commit.NO_ANCESTOR) {
      return false;
    }

    nextCommit = commitAccessor.apply(ancestor);
    next = WithHash.of(ancestor, nextCommit.getMetadata());
    return true;
  }

  @Override
  public WithHash<MetadataT> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    final WithHash<MetadataT> result = next;
    previousCommit = nextCommit;
    next = null;
    nextCommit = null;
    return result;
  }
}
