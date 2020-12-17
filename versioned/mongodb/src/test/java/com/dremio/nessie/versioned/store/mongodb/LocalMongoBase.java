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
package com.dremio.nessie.versioned.store.mongodb;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;

import com.google.common.io.Files;

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.MongosStarter;
import de.flapdoodle.embed.mongo.config.Defaults;
import de.flapdoodle.embed.process.config.RuntimeConfig;
import de.flapdoodle.embed.process.extract.DirectoryAndExecutableNaming;
import de.flapdoodle.embed.process.extract.NoopTempNaming;
import de.flapdoodle.embed.process.extract.UUIDTempNaming;
import de.flapdoodle.embed.process.io.directories.Directory;
import de.flapdoodle.embed.process.io.directories.FixedPath;

/**
 * Base Jupiter extension for creating and configuring a flapdoodle MongoDB instance.
 */
abstract class LocalMongoBase extends TypeBasedParameterResolver<String> implements AfterAllCallback, BeforeAllCallback {
  // Resources used to manage download and extraction of MongoDB binaries.
  private static final Directory MONGO_ARTIFACT_TEMP_PATH = new FixedPath(Files.createTempDir().getPath());
  private static final Directory MONGO_ARTIFACT_EXTRACT_PATH = new FixedPath(Files.createTempDir().getPath());
  private static final Directory MONGO_ARTIFACT_STORE_PATH = new FixedPath(Files.createTempDir().getPath());

  private static final RuntimeConfig MONGOD_RUNTIME_CONFIG = Defaults.runtimeConfigFor(Command.MongoD)
      .artifactStore(Defaults.extractedArtifactStoreFor(Command.MongoD)
          .withTemp(DirectoryAndExecutableNaming.of(MONGO_ARTIFACT_TEMP_PATH, new NoopTempNaming()))
          .withExtraction(DirectoryAndExecutableNaming.of(MONGO_ARTIFACT_EXTRACT_PATH, new UUIDTempNaming()))
          .withDownloadConfig(Defaults.downloadConfigFor(Command.MongoD)
              .artifactStorePath(MONGO_ARTIFACT_STORE_PATH)
              .build()))
      .build();
  private static final RuntimeConfig MONGOS_RUNTIME_CONFIG = Defaults.runtimeConfigFor(Command.MongoS)
      .artifactStore(Defaults.extractedArtifactStoreFor(Command.MongoS)
          .withTemp(DirectoryAndExecutableNaming.of(MONGO_ARTIFACT_TEMP_PATH, new UUIDTempNaming()))
          .withExtraction(DirectoryAndExecutableNaming.of(MONGO_ARTIFACT_EXTRACT_PATH, new UUIDTempNaming()))
          .withDownloadConfig(Defaults.downloadConfigFor(Command.MongoS)
              .artifactStorePath(MONGO_ARTIFACT_STORE_PATH)
              .build()))
      .build();

  protected static final MongodStarter MONGOD_STARTER = MongodStarter.getInstance(MONGOD_RUNTIME_CONFIG);
  protected static final MongosStarter MONGOS_STARTER = MongosStarter.getInstance(MONGOS_RUNTIME_CONFIG);

  interface ServerHolder {
    /**
     * Retrieve the connection string to connect to the Mongo server.
     * @return the connection string.
     */
    String getConnectionString();

    /**
     * Start the Mongo server.
     * @throws Exception if there is an issue starting the server.
     */
    void start() throws Exception;

    /**
     * Stop the Mongo server.
     * @throws Exception if there is an issue stopping the server.
     */
    void stop() throws Exception;
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    final ServerHolder h = getHolder(extensionContext, false);
    if (h != null) {
      h.stop();
    }
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    final ServerHolder holder = getHolder(extensionContext, true);
    if (holder != null) {
      return;
    }
    final ServerHolder newHolder = createHolder();
    newHolder.start();
    getStore(extensionContext).put(getIdentifier(), newHolder);
  }

  @Override
  public String resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
    return getHolder(extensionContext, true).getConnectionString();
  }

  private ServerHolder getHolder(ExtensionContext context, boolean recursive) {
    for (; context != null; context = context.getParent().orElse(null)) {
      final ServerHolder holder = (ServerHolder) getStore(context).get(getIdentifier());
      if (holder != null) {
        return holder;
      }

      if (!recursive) {
        break;
      }
    }

    return null;
  }

  private ExtensionContext.Store getStore(ExtensionContext context) {
    return context.getStore(ExtensionContext.Namespace.create(getClass(), context));
  }

  /**
   * Get the identifier for the Holder object for the Mongo server.
   * @return the identifier for the Mongo server.
   */
  protected abstract String getIdentifier();

  /**
   * Create a new Holder object for the Mongo server.
   * @return the new holder object.
   */
  protected abstract ServerHolder createHolder();
}
