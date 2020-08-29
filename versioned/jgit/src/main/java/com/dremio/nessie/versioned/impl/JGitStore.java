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
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;

import com.dremio.nessie.backend.TableConverter;
import com.dremio.nessie.jgit.JgitBranchController;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.WithHash;

/**
 * Temporary object to bridge jgit branch controller to new version store interface.
 */
public class JGitStore<TABLE, METADATA> {

  private final JgitBranchController<TABLE, METADATA> controller;

  /**
   * Construct a JGitStore.
   */
  public JGitStore(StoreWorker<TABLE, METADATA> storeWorker) {
    // todo add config for directory/repo type
    Repository repository = null;
    try {
      repository = new InMemoryRepository.Builder().setRepositoryDescription(new DfsRepositoryDescription()).build();
    } catch (IOException e) {
      //pass can't happen
    }
    controller = new TempJGitBranchController<>(storeWorker, repository);
  }

  /**
   * get hash that ref is pointing to.
   */
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

  /**
   * Create a ref.
   */
  public void createRef(String name, String hash, TableConverter<TABLE> tableConverter) throws IOException {
    //todo tags
    Optional<String> baseBranch = controller.getBranches().stream().filter(b -> b.getId().equals(hash)).findFirst().map(Branch::getName);
    controller.create(name, baseBranch.orElse(null), null, tableConverter);
  }

  public void delete(String name, String hash) throws IOException {
    controller.deleteBranch(name, hash, null);
  }

  public Stream<WithHash<NamedRef>> getRefs() throws IOException {
    //todo tags
    return controller.getBranches().stream().map(b -> WithHash.of(Hash.of(b.getId()), BranchName.of(b.getName())));
  }

  public void commit(String name, String hash, METADATA metadata, TableConverter<TABLE> tableConverter, List<TABLE> operations)
      throws IOException {
    controller.commit(name, metadata, hash, tableConverter, (TABLE[]) operations.toArray());
  }

  /**
   * Get the value of a table on a branch.
   */
  public TABLE getValue(String branch, String tableName) throws ReferenceNotFoundException {
    TABLE table;
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

  private static class TempJGitBranchController<TABLE, METADATA> extends JgitBranchController<TABLE, METADATA> {

    private final StoreWorker<TABLE, METADATA> storeWorker;

    public TempJGitBranchController(StoreWorker<TABLE, METADATA> storeWorker, Repository repository) {
      super(storeWorker, repository);
      this.storeWorker = storeWorker;
    }

    @Override
    protected CommitBuilder fromUser(METADATA commitMeta, long now) {
      CommitBuilder commitBuilder = new CommitBuilder();
      PersonIdent person = new PersonIdent("test", "me@example.com");
      if (commitMeta != null) {
        commitBuilder.setMessage(storeWorker.getMetadataSerializer().toBytes(commitMeta).toString());
      } else {
        commitBuilder.setMessage("none");
      }
      commitBuilder.setAuthor(person);
      commitBuilder.setCommitter(person);
      return commitBuilder;
    }
  }
}
