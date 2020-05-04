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

package com.dremio.iceberg.server.jgit;

import java.io.IOException;
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.util.RefList;

public class NessieRefDatabase extends DfsRefDatabase {

  private final JGitBackend backend;
  /**
   * DfsRefDatabase used by InMemoryRepository.
   */
  boolean performsAtomicTransactions = true;

  /**
   * Initialize a new in-memory ref database.
   */
  protected NessieRefDatabase(DfsRepository repository,
                              JGitBackend backend) {
    super(repository);
    this.backend = backend;
  }

  @Override
  protected RefCache scanAllRefs() throws IOException {
    return backend.refList();
  }

  @Override
  protected boolean compareAndPut(Ref oldRef, Ref newRef) throws IOException {
    return backend.atomicSwap(oldRef, newRef);
  }

  @Override
  protected boolean compareAndRemove(Ref oldRef) throws IOException {
    return backend.atomicRemove(oldRef);
  }

  @Override
  public boolean performsAtomicTransactions() {
    return performsAtomicTransactions;
  }
}
