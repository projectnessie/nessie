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
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.eclipse.jgit.errors.ConfigInvalidException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.CommitBuilder;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.UserConfig;
import org.eclipse.jgit.util.SystemReader;

import com.dremio.nessie.backend.TableConverter;
import com.dremio.nessie.jgit.JgitBranchController;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.WithHash;
import com.google.protobuf.ByteString;

/**
 * Temporary object to bridge jgit branch controller to new version store interface.
 */
public class JGitStore<TABLE, METADATA> {

  private final JgitBranchController<TABLE, METADATA> controller;
  private final StoreWorker<TABLE, METADATA> storeWorker;

  /**
   * Construct a JGitStore.
   */
  @Inject
  public JGitStore(StoreWorker<TABLE, METADATA> storeWorker) {
    this.storeWorker = storeWorker;
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
      throw new ReferenceNotFoundException(String.format("Unable to find reference for %s", name), e);
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
    //todo allow commit from arbitrary hash #53
    final String baseBranchName;
    if (hash == null) {
      baseBranchName = null;
    } else {
      Optional<String> baseBranch = controller.getBranches().stream().filter(b -> b.getId().equals(hash)).findFirst().map(Branch::getName);
      baseBranchName = baseBranch.orElseThrow(() -> new IllegalStateException(
        String.format("Unable to create a branch at hash %s as there are no other branches pointing to it.", hash)));
    }
    controller.create(name, baseBranchName, null, tableConverter);
  }

  public void delete(String name, String hash) throws IOException {
    //todo allow delete with empty hash #53
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
      throw new ReferenceNotFoundException(String.format("reference for branch %s and table %s not found", branch, tableName), e);
    }
    return table;
  }

  /**
   * assign name to targethash.
   * @param name branch/tag name to assign
   * @param expectedHash expected hash (for optimistic concurrency)
   * @param targetHash the has to which this name should point to
   */
  public void updateRef(String name, String expectedHash, String targetHash) throws IOException {
    //todo allow assign from arbitrary hash #53
    String mergeBranch = getRefs().filter(r -> r.getHash().equals(Hash.of(targetHash)))
                                  .findFirst()
                                  .map(WithHash::getValue)
                                  .map(NamedRef::getName)
                                  .orElseThrow(() -> new UnsupportedOperationException("Cant assign to ref if it isn't a hash. See #53"));
    TableConverter<TABLE> tableConverter = new TableConverter<TABLE>() {
      @Override
      public boolean isDeleted(TABLE branchTable) {
        return false; //assume not deleted
      }

      @Override
      public String getId(TABLE branchTable) {
        throw new IllegalStateException(String.format("Should not need id for table: %s while creating a ref", branchTable));
      }

      @Override
      public String getNamespace(TABLE branchTable) {
        return null; //todo ignore namespace until #53 is addressed
      }
    };
    controller.promote(name, mergeBranch, expectedHash, null, true, false, null, tableConverter);
  }

  /**
   * get commits as an ordered list from name to start of history.
   * @param name branch/hash name to start log from
   */
  public Stream<WithHash<METADATA>> getCommits(String name) throws IOException {
    Serializer<METADATA> serializer = storeWorker.getMetadataSerializer();
    return controller.log(name)
                     .map(e -> WithHash.of(Hash.of(e.commitId()),
                                           serializer.fromBytes(ByteString.copyFrom(e.message(), StandardCharsets.UTF_8))));
  }

  /**
   * return keys in a specific Branch/Tag or Hash.
   * @param ref existing ref
   * @return list of keys
   * @throws IOException if there is a problem with the git store
   */
  public List<String> getKeys(String ref) throws IOException {
    TableConverter<TABLE> converter = new TableConverter<TABLE>() {
      @Override
      public boolean isDeleted(TABLE branchTable) {
        return false; //assume not deleted
      }

      @Override
      public String getId(TABLE branchTable) {
        //todo fix this in #53
        if (branchTable instanceof Table) {
          return ((Table) branchTable).getId();
        }
        throw new IllegalStateException(String.format("Should not need id for table: %s while creating a ref", branchTable));
      }

      @Override
      public String getNamespace(TABLE branchTable) {
        return null; //todo ingore namespace until #53 is addressed
      }
    };
    return controller.getTables(ref, null, converter);
  }

  private static class TempJGitBranchController<TABLE, METADATA> extends JgitBranchController<TABLE, METADATA> {

    private final StoreWorker<TABLE, METADATA> storeWorker;

    public TempJGitBranchController(StoreWorker<TABLE, METADATA> storeWorker, Repository repository) {
      super(storeWorker, repository);
      this.storeWorker = storeWorker;
    }

    @Override
    protected CommitBuilder fromUser(METADATA commitMeta) {
      CommitBuilder commitBuilder = new CommitBuilder();
      long updateTime = ZonedDateTime.now(ZoneId.of("UTC")).toInstant().toEpochMilli();
      PersonIdent person;
      try {
        UserConfig config = SystemReader.getInstance().getUserConfig().get(UserConfig.KEY);
        person = new PersonIdent(config.getAuthorName(), config.getAuthorEmail(), updateTime, 0);
      } catch (IOException | ConfigInvalidException e) {
        //todo email can't be null but we cant find it
        person = new PersonIdent(System.getProperty("user.name"), "me@example.com");
      }
      if (commitMeta != null) {
        //todo better way to store commit metadata? Make an interface? or toString?
        commitBuilder.setMessage(storeWorker.getMetadataSerializer().toBytes(commitMeta).toStringUtf8());
      } else {
        commitBuilder.setMessage("none");
      }
      commitBuilder.setAuthor(person);
      commitBuilder.setCommitter(person);
      return commitBuilder;
    }
  }
}
