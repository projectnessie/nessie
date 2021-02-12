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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.stream.Stream;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.server.config.ApplicationConfig;
import org.projectnessie.server.config.ApplicationConfig.VersionStoreDynamoConfig;
import org.projectnessie.server.config.converters.VersionStoreType;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.dynamodb.DynamoStore;
import org.projectnessie.versioned.dynamodb.DynamoStoreConfig;
import org.projectnessie.versioned.impl.TieredVersionStore;
import org.projectnessie.versioned.jgit.JGitVersionStore;
import org.projectnessie.versioned.memory.InMemoryVersionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.regions.Region;

@Singleton
public class VersionStoreFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(VersionStoreFactory.class);

  private final ApplicationConfig config;

  @Inject
  public VersionStoreFactory(ApplicationConfig config) {
    this.config = config;
  }

  @ConfigProperty(name = "quarkus.dynamodb.aws.region")
  String region;
  @ConfigProperty(name = "quarkus.dynamodb.endpoint-override")
  Optional<String> endpoint;


  @Produces
  public StoreWorker<Contents, CommitMeta> worker() {
    return new TableCommitMetaStoreWorker();
  }

  /**
   * default config for lambda function.
   */
  @Produces
  @Singleton
  public VersionStore<Contents, CommitMeta> configuration(
      TableCommitMetaStoreWorker storeWorker, Repository repository, ServerConfig config) {
    VersionStore<Contents, CommitMeta> store = getVersionStore(storeWorker, repository);
    try (Stream<WithHash<NamedRef>> str = store.getNamedRefs()) {
      if (!str.findFirst().isPresent()) {
        // if this is a new database, create a branch with the default branch name.
        try {
          store.create(BranchName.of(config.getDefaultBranch()), Optional.empty());
        } catch (ReferenceNotFoundException | ReferenceAlreadyExistsException e) {
          LOGGER.warn("Failed to create default branch of {}.", config.getDefaultBranch(), e);
        }
      }
    }

    return store;
  }

  private VersionStore<Contents, CommitMeta> getVersionStore(TableCommitMetaStoreWorker storeWorker, Repository repository) {
    switch (config.getVersionStoreConfig().getVersionStoreType()) {
      case DYNAMO:
        LOGGER.info("Using Dyanmo Version store");
        return new TieredVersionStore<>(storeWorker, createDynamoConnection(), false);
      case JGIT:
        LOGGER.info("Using JGit Version Store");
        return new JGitVersionStore<>(repository, storeWorker);
      case INMEMORY:
        LOGGER.info("Using In Memory version store");
        return InMemoryVersionStore.<Contents, CommitMeta>builder()
            .metadataSerializer(storeWorker.getMetadataSerializer())
            .valueSerializer(storeWorker.getValueWorker())
            .build();
      default:
        throw new RuntimeException(String.format("unknown jgit repo type %s", config.getVersionStoreConfig().getVersionStoreType()));
    }
  }

  /**
   * create a dynamo store based on config.
   */
  private DynamoStore createDynamoConnection() {
    if (!config.getVersionStoreConfig().getVersionStoreType().equals(VersionStoreType.DYNAMO)) {
      return null;
    }

    VersionStoreDynamoConfig in = config.getVersionStoreDynamoConfig();
    DynamoStore dynamo = new DynamoStore(
        DynamoStoreConfig.builder()
          .endpoint(endpoint.map(e -> {
            try {
              return new URI(e);
            } catch (URISyntaxException uriSyntaxException) {
              throw new RuntimeException(uriSyntaxException);
            }
          }))
          .region(Region.of(region))
          .initializeDatabase(in.isDynamoInitialize())
          .tablePrefix(in.getTablePrefix())
          .enableTracing(in.enableTracing())
          .build());
    dynamo.start();
    return dynamo;
  }

  /**
   * produce a git repo based on config.
   */
  @Produces
  public Repository repository() throws IOException, GitAPIException {
    if (!config.getVersionStoreConfig().getVersionStoreType().equals(VersionStoreType.JGIT)) {
      return null;
    }
    switch (config.getVersionStoreJGitConfig().getJgitStoreType()) {
      case DISK:
        LOGGER.info("JGit Version store has been configured with the file backend");
        File jgitDir = new File(config.getVersionStoreJGitConfig().getJgitDirectory()
                                      .orElseThrow(() -> new RuntimeException("Please set nessie.version.store.jgit.directory")));
        if (!jgitDir.exists()) {
          if (!jgitDir.mkdirs()) {
            throw new RuntimeException(
              String.format("Couldn't create file at %s", config.getVersionStoreJGitConfig().getJgitDirectory().get()));
          }
        }
        LOGGER.info("File backend is at {}", jgitDir.getAbsolutePath());
        return Git.init().setDirectory(jgitDir).call().getRepository();
      case INMEMORY:
        LOGGER.info("JGit Version store has been configured with the in memory backend");
        return new InMemoryRepository.Builder().setRepositoryDescription(new DfsRepositoryDescription()).build();
      default:
        throw new RuntimeException(String.format("unknown jgit repo type %s", config.getVersionStoreJGitConfig().getJgitStoreType()));
    }
  }
}
