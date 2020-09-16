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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.Repository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.StringSerializer;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.VersionStoreException;
import com.dremio.nessie.versioned.tests.AbstractITVersionStore;

@Disabled
public class ITJGitVersionStore extends AbstractITVersionStore {
  private Repository repository;
  private VersionStore<String, String> store;

  private static final StoreWorker<String, String> WORKER = new StoreWorker<String, String>() {

    @Override
    public Serializer<String> getValueSerializer() {
      return StringSerializer.getInstance();
    }

    @Override
    public Serializer<String> getMetadataSerializer() {
      return StringSerializer.getInstance();
    }

    @Override
    public Stream<AssetKey> getAssetKeys(String value) {
      return Stream.of();
    }

    @Override
    public CompletableFuture<Void> deleteAsset(AssetKey key) {
      throw new UnsupportedOperationException();
    }
  };

  @BeforeEach
  void setUp() throws IOException {
    repository = new InMemoryRepository.Builder().setRepositoryDescription(new DfsRepositoryDescription()).build();
    store = new JGitVersionStore<>(repository, WORKER);
  }

  @AfterEach
  void tearDown() {
    repository.close();
  }

  @Override protected VersionStore<String, String> store() {
    return store;
  }

  @Disabled
  @Override
  protected void transplant() throws VersionStoreException {
    super.transplant();
  }

  @Disabled
  @Override
  public void forceCommitConflictingOperations() throws Exception {
    super.forceCommitConflictingOperations();
  }


}
