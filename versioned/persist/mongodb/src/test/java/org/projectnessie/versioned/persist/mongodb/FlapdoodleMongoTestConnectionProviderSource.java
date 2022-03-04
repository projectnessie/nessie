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
package org.projectnessie.versioned.persist.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.Defaults;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.MongodProcessOutputConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.packageresolver.Command;
import de.flapdoodle.embed.process.config.process.ProcessOutput;
import de.flapdoodle.embed.process.io.StreamProcessor;
import de.flapdoodle.embed.process.runtime.Network;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.assertj.core.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoDB test connection-provider source using a locally spawned MongoDB instance via
 * "flapdoodle".
 */
public class FlapdoodleMongoTestConnectionProviderSource extends MongoTestConnectionProviderSource {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(FlapdoodleMongoTestConnectionProviderSource.class);

  @VisibleForTesting
  static final Pattern LISTEN_ON_PORT_PATTERN =
      Pattern.compile(
          ".*NETWORK.*[Ww]aiting for connections[^\n]*port[^\n0-9]*([0-9]{2,})[^0-9].*",
          Pattern.MULTILINE | Pattern.DOTALL);

  private MongodExecutable mongo;

  @VisibleForTesting
  static class PortExtractor implements StreamProcessor {
    private final StringBuilder buffer = new StringBuilder();
    private final StreamProcessor delegate;
    private volatile int port;

    PortExtractor(StreamProcessor delegate) {
      this.delegate = delegate;
    }

    int getPort() {
      return port;
    }

    @Override
    public void process(String block) {
      if (port == 0) {
        try {
          buffer.append(block);
          Matcher matcher = LISTEN_ON_PORT_PATTERN.matcher(buffer);
          if (matcher.matches()) {
            String portString = matcher.group(1);
            port = Integer.parseInt(portString);
            LOGGER.info("Recognized port {} in mongod output", port);
            buffer.setLength(0);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      delegate.process(block);
    }

    @Override
    public void onProcessed() {
      delegate.onProcessed();
    }
  }

  @Override
  public void start() throws Exception {
    if (mongo != null) {
      throw new IllegalStateException("Already started");
    }

    ProcessOutput defaultOutput = MongodProcessOutputConfig.getDefaultInstance(Command.MongoD);
    PortExtractor portExtractor = new PortExtractor(defaultOutput.output());

    MongodStarter starter =
        MongodStarter.getInstance(
            Defaults.runtimeConfigFor(Command.MongoD)
                .processOutput(
                    ProcessOutput.builder()
                        .output(portExtractor)
                        .error(defaultOutput.error())
                        .commands(defaultOutput.commands())
                        .build())
                .build());

    MongodConfig mongodConfig =
        MongodConfig.builder()
            .version(Version.Main.PRODUCTION)
            .net(new Net(0, Network.localhostIsIPv6()))
            .build();

    mongo = starter.prepare(mongodConfig);
    mongo.start();

    assertThat(portExtractor.getPort()).isGreaterThan(0);

    String connectionString = String.format("mongodb://localhost:%d", portExtractor.getPort());
    String databaseName = "test";

    LOGGER.info("Constructed Mongo connection string '{}'", connectionString);

    configureConnectionProviderConfigFromDefaults(
        c -> c.withConnectionString(connectionString).withDatabaseName(databaseName));

    super.start();
  }

  @Override
  public void stop() throws Exception {
    try {
      super.stop();
    } finally {
      try {
        if (mongo != null) {
          mongo.stop();
        }
      } finally {
        mongo = null;
      }
    }
  }
}
