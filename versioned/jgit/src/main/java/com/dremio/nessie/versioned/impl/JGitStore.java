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
package com.dremio.nessie.versioned.impl;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;

import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.Repository;

import com.dremio.nessie.jgit.JgitBranchController;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.WithHash;

public class JGitStore {

  private final JgitBranchController controller;

  public JGitStore() {
    // todo add config for directory/repo type
    Repository repository = null;
    try {
      repository = new InMemoryRepository.Builder().setRepositoryDescription(new DfsRepositoryDescription()).build();
    } catch (IOException e) {
      //pass can't happen
    }
    controller = new JgitBranchController(repository);
  }

  public Hash getRef(String name) throws ReferenceNotFoundException {
    //todo add tags
    Branch branch;
    try {
      branch = controller.getBranch(name);
    } catch (IOException e) {
      branch = null;
    }
    if (branch == null) {
      throw new ReferenceNotFoundException(String.format("Unable to find reference for %s", name));
    }
    return Hash.of(branch.getId());
  }

  public void createRef(String name, String hash) throws IOException {
    //todo tags
    Optional<String> baseBranch = controller.getBranches().stream().filter(b -> b.getId().equals(hash)).findFirst().map(Branch::getName);
    controller.create(name, baseBranch.orElse(null), null);
  }

  public void delete(String name, String hash) throws IOException {
    controller.deleteBranch(name, hash, null);
  }

  public Stream<WithHash<NamedRef>> getRefs() throws IOException {
    //todo tags
    return controller.getBranches().stream().map(b -> WithHash.of(Hash.of(b.getId()), BranchName.of(b.getName())));
  }

  public void commit(String name, String hash, CommitMeta metadata, Table... operations) throws IOException {
    controller.commit(name, metadata, hash, operations);
  }

  public Table getValue(String branch, String tableName) throws ReferenceNotFoundException {
    Table table;
    try {
      table = controller.getTable(branch, tableName, false);
    } catch (IOException e) {
      table = null;
    }
    if (table == null) {
      throw new ReferenceNotFoundException(String.format("reference for branch %s and table %s not found", branch, tableName));
    }
    return table;
  }
}
