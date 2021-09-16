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

import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.Defaults;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.MongodProcessOutputConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.io.StreamProcessor;
import de.flapdoodle.embed.process.runtime.Network;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.tests.DatabaseAdapterExtension;

public class MongoDatabaseAdapterExtension extends DatabaseAdapterExtension {

  private static final Pattern LISTEN_ON_PORT_PATTERN =
      Pattern.compile(
          ".*NETWORK ([^\\n]*) waiting for connections on port ([0-9]+)\\n.*",
          Pattern.MULTILINE | Pattern.DOTALL);

  private static final AtomicInteger port = new AtomicInteger();
  private static MongodExecutable mongo;

  public MongoDatabaseAdapterExtension() {}

  @Override
  protected DatabaseAdapter createAdapter(ExtensionContext context, TestConfigurer testConfigurer) {
    ProcessOutput defaultOutput = MongodProcessOutputConfig.getDefaultInstance(Command.MongoD);
    StreamProcessor capturedStdout =
        new StreamProcessor() {
          private final StringBuilder buffer = new StringBuilder();

          @Override
          public void process(String block) {
            if (port.get() == 0) {
              buffer.append(block);
              Matcher matcher = LISTEN_ON_PORT_PATTERN.matcher(buffer);
              if (matcher.matches()) {
                String portString = matcher.group(2);
                port.set(Integer.parseInt(portString));
              }
            }
            defaultOutput.getOutput().process(block);
          }

          @Override
          public void onProcessed() {
            defaultOutput.getOutput().onProcessed();
          }
        };

    MongodStarter starter =
        MongodStarter.getInstance(
            Defaults.runtimeConfigFor(Command.MongoD)
                .processOutput(
                    new ProcessOutput(
                        capturedStdout, defaultOutput.getError(), defaultOutput.getCommands()))
                .build());

    try {
      MongodConfig mongodConfig =
          MongodConfig.builder()
              .version(Version.Main.PRODUCTION)
              .net(new Net(0, Network.localhostIsIPv6()))
              .build();

      mongo = starter.prepare(mongodConfig);
      mongo.start();

      assertThat(port).hasPositiveValue();

      return createAdapter(
          "MongoDB",
          testConfigurer,
          config ->
              ImmutableMongoDatabaseAdapterConfig.builder()
                  .from(config)
                  .connectionString(String.format("mongodb://localhost:%d", port.get()))
                  .databaseName("test")
                  .build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void afterCloseAdapter() {
    try {
      if (mongo != null) {
        mongo.stop();
      }
    } finally {
      mongo = null;
    }
  }
}
