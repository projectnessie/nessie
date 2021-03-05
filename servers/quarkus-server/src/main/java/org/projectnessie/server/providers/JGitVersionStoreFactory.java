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
package org.projectnessie.server.providers;

import static org.projectnessie.server.config.VersionStoreConfig.VersionStoreType.JGIT;

import java.io.File;
import java.io.IOException;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.Repository;
import org.projectnessie.server.config.JGitVersionStoreConfig;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.jgit.JGitVersionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JGit version store factory.
 */
@StoreType(JGIT)
@Dependent
public class JGitVersionStoreFactory implements VersionStoreFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(JGitVersionStoreFactory.class);

  private final JGitVersionStoreConfig config;

  @Inject
  public JGitVersionStoreFactory(JGitVersionStoreConfig config) {
    this.config = config;
  }

  @Override
  public <VALUE, METADATA> VersionStore<VALUE, METADATA> newStore(StoreWorker<VALUE, METADATA> worker) throws IOException {
    final Repository repository = newRepository();
    return new JGitVersionStore<>(repository, worker);
  }


  private Repository newRepository() throws IOException {
    switch (config.getJgitStoreType()) {
      case DISK:
        LOGGER.info("JGit Version store has been configured with the file backend");
        File jgitDir = new File(config.getJgitDirectory()
            .orElseThrow(() -> new RuntimeException("Please set nessie.version.store.jgit.directory")));
        if (!jgitDir.exists()) {
          if (!jgitDir.mkdirs()) {
            throw new RuntimeException(
                String.format("Couldn't create file at %s", config.getJgitDirectory().get()));
          }
        }
        LOGGER.info("File backend is at {}", jgitDir.getAbsolutePath());
        try {
          return Git.init().setDirectory(jgitDir).call().getRepository();
        } catch (GitAPIException e) {
          throw new IOException(e);
        }

      case INMEMORY:
        LOGGER.info("JGit Version store has been configured with the in memory backend");
        return new InMemoryRepository.Builder().setRepositoryDescription(new DfsRepositoryDescription()).build();

      default:
        throw new RuntimeException(String.format("unknown jgit repo type %s", config.getJgitStoreType()));
    }
  }

}
