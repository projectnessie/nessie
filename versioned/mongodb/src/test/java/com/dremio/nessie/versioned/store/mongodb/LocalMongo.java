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

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;

/** Creates and configures a non-sharded flapdoodle MongoDB instance. */
class LocalMongo extends TypeBasedParameterResolver<String>
    implements AfterAllCallback, BeforeAllCallback {
  private static final String MONGODB_INFO = "mongodbdb-local-info";

  private static class Holder {
    private final MongodExecutable mongoExec;
    private final String connectionString;

    public Holder() throws Exception {
      final int port = Network.getFreeServerPort();
      final MongodConfig config =
          MongodConfig.builder()
              .version(Version.Main.PRODUCTION)
              .net(new Net(port, Network.localhostIsIPv6()))
              .build();

      mongoExec = MongodStarter.getDefaultInstance().prepare(config);
      mongoExec.start();
      connectionString = "mongodb://localhost:" + port;
    }

    private void stop() {
      mongoExec.stop();
    }
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) {
    final Holder h = getHolder(extensionContext, false);
    if (h != null) {
      h.stop();
    }
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    final Holder holder = getHolder(extensionContext, true);
    if (holder != null) {
      return;
    }
    getStore(extensionContext).put(MONGODB_INFO, new Holder());
  }

  @Override
  public String resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext) {
    return getHolder(extensionContext, true).connectionString;
  }

  private Holder getHolder(ExtensionContext context, boolean recursive) {
    for (ExtensionContext c = context; c != null; c = c.getParent().orElse(null)) {
      final Holder holder = (Holder) getStore(c).get(MONGODB_INFO);
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
}
