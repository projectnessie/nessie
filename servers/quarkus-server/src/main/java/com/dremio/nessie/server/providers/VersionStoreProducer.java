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
package com.dremio.nessie.server.providers;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

import javax.enterprise.inject.Produces;
import javax.inject.Singleton;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.dremio.nessie.backend.Backend;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.impl.DynamoStore;
import com.dremio.nessie.versioned.impl.DynamoStoreConfig;
import com.dremio.nessie.versioned.impl.DynamoVersionStore;
import com.dremio.nessie.versioned.impl.JGitVersionStore;
import com.dremio.nessie.versioned.impl.experimental.NessieRepository;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.regions.Region;

@Singleton
public class VersionStoreProducer {

  @ConfigProperty(name = "nessie.version.store.type", defaultValue = "JGIT")
  String type;
  @ConfigProperty(name = "nessie.version.store.jgit.type", defaultValue = "INMEMORY")
  String jgitType;
  @ConfigProperty(name = "nessie.version.store.jgit.directory")
  Optional<String> jgitDirectory;
  @ConfigProperty(name = "quarkus.dynamodb.aws.region")
  String region;
  @ConfigProperty(name = "quarkus.dynamodb.endpoint-override")
  Optional<String> endpoint;
  @ConfigProperty(name = "nessie.version.store.dynamo.initialize", defaultValue = "true")
  String initialize;
  @ConfigProperty(name = "nessie.version.store.dynamo.refTableName", defaultValue = "nessie_refs")
  String refTableName;
  @ConfigProperty(name = "nessie.version.store.dynamo.treeTableName", defaultValue = "nessie_trees")
  String treeTableName;
  @ConfigProperty(name = "nessie.version.store.dynamo.valueTableName", defaultValue = "nessie_values")
  String valueTableName;

  @Produces
  public StoreWorker<Table, CommitMeta> worker(ObjectMapper mapper) {
    return new TableCommitMetaStoreWorker(mapper);
  }

  /**
   * default config for lambda function.
   */
  @Produces
  @Singleton
  public VersionStore<Table, CommitMeta> configuration(TableCommitMetaStoreWorker storeWorker, DynamoStore store, Repository repository) {
    switch (type) {
      case "DYNAMO":
        return new DynamoVersionStore<>(storeWorker, store, false);
      case "JGIT":
        return new JGitVersionStore<>(repository, storeWorker);
      case "INMEMORY":
        throw new UnsupportedOperationException("Merge Inmemory PR first");
      default:
        throw new RuntimeException(String.format("unknown jgit repo type %s", jgitType));
    }
  }

  /**
   * create a dynamo store based on config.
   */
  @Produces
  public DynamoStore dyanmo() {
    if (!type.equals("DYNAMO")) {
      return null;
    }

    return new DynamoStore(DynamoStoreConfig.builder()
                                            .endpoint(endpoint.map(e -> {
                                              try {
                                                return new URI(e);
                                              } catch (URISyntaxException uriSyntaxException) {
                                                throw new RuntimeException(uriSyntaxException);
                                              }
                                            }))
                                            .region(Region.of(region))
                                            .initializeDatabase(Boolean.parseBoolean(initialize))
                                            .refTableName(refTableName)
                                            .treeTableName(treeTableName)
                                            .valueTableName(valueTableName)
                                            .build());
  }

  /**
   * produce a git repo based on config.
   */
  @Produces
  public Repository repository(Backend backend) throws IOException, GitAPIException {
    switch (jgitType) {
      case "DYNAMO":
        DfsRepositoryDescription repoDesc = new DfsRepositoryDescription();
        return new NessieRepository.Builder().setRepositoryDescription(repoDesc)
                                             .setBackend(backend.gitBackend())
                                             .setRefBackend(backend.gitRefBackend())
                                             .build();
      case "DISK":
        File jgitDir = new File(jgitDirectory.orElseThrow(() -> new RuntimeException("Please set nessie.version.store.jgit.directory")));
        if (!jgitDir.exists()) {
          if (!jgitDir.mkdirs()) {
            throw new RuntimeException(String.format("Couldn't create file at %s", jgitDirectory));
          }
        }
        return Git.init().setDirectory(jgitDir).call().getRepository();
      case "INMEMORY":
        return new InMemoryRepository.Builder().setRepositoryDescription(new DfsRepositoryDescription()).build();
      default:
        throw new RuntimeException(String.format("unknown jgit repo type %s", jgitType));
    }
  }
}
