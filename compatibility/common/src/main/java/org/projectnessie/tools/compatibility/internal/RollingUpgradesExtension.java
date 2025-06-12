/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tools.compatibility.internal;

import static org.projectnessie.tools.compatibility.internal.AbstractNessieApiHolder.apiInstanceForField;
import static org.projectnessie.tools.compatibility.internal.GlobalForClass.globalForClass;
import static org.projectnessie.tools.compatibility.internal.NessieServer.nessieServer;
import static org.projectnessie.tools.compatibility.internal.NessieServer.nessieServerExisting;
import static org.projectnessie.tools.compatibility.internal.Util.extensionStore;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.tools.compatibility.api.TargetVersion;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.versioned.storage.mongodbtests.MongoDBBackendTestFactory;

/**
 * Simulates a rolling-upgrade situation with one Nessie instance running an old version and another
 * instance running the current (in-tree) version.
 *
 * <ul>
 *   <li>{@link org.projectnessie.tools.compatibility.api.NessieVersion} annotated fields get the
 *       old version injected.
 *   <li>{@link org.projectnessie.tools.compatibility.api.NessieAPI} annotated fields with {@code
 *       versionType} set to {@link TargetVersion#TESTED} get the API instance to the old version
 *       injected.
 *   <li>{@link org.projectnessie.tools.compatibility.api.NessieAPI} annotated fields with {@code
 *       versionType} set to {@link TargetVersion#CURRENT} get the API instance to the current
 *       (in-tree) version injected.
 * </ul>
 *
 * <p>The repository is re-initialized for every Nessie version combination.
 */
public class RollingUpgradesExtension extends AbstractMultiVersionExtension {

  @Override
  public void beforeAll(ExtensionContext context) {
    Version version = populateNessieVersionAnnotatedFields(context, null);
    if (version == null) {
      return;
    }

    MongoHolder mongo = getMongoHolder(context);
    mongo.start();

    ServerKey serverKey = buildServerKey(version, context);
    Consumer<Object> mongoConfigure =
        c -> {
          try {
            Method setClient =
                Arrays.stream(c.getClass().getMethods())
                    .filter(m -> m.getName().equals("client"))
                    .findFirst()
                    .orElseThrow(
                        () ->
                            new IllegalStateException(
                                "No such method client(MongoClient) on " + c.getClass().getName()));

            c.getClass()
                .getMethod("databaseName", String.class)
                .invoke(c, mongo.mongo.getDatabaseName());

            ClassLoader classLoader = setClient.getParameterTypes()[0].getClassLoader();
            Object mongoClient =
                classLoader
                    .loadClass("com.mongodb.client.MongoClients")
                    .getMethod("create", String.class)
                    .invoke(null, mongo.mongo.getConnectionString());
            extensionStore(context).put("mongo-client", mongoClient);

            setClient.invoke(c, mongoClient);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };

    NessieServer nessieServer = nessieServer(context, serverKey, () -> true, mongoConfigure);
    populateNessieApiFields(context, null, version, TargetVersion.TESTED, ctx -> nessieServer);

    ServerKey serverKeyNext = buildServerKey(Version.CURRENT, context);
    NessieServer nessieServerNext =
        nessieServer(context, serverKeyNext, () -> false, mongoConfigure);
    populateNessieApiFields(
        context, null, Version.CURRENT, TargetVersion.CURRENT, ctx -> nessieServerNext);
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    Object testInstance = context.getRequiredTestInstance();
    Version version = populateNessieVersionAnnotatedFields(context, testInstance);
    if (version == null) {
      return;
    }

    ServerKey serverKey = buildServerKey(version, context);
    NessieServer nessieServer = nessieServerExisting(context, serverKey);
    populateNessieApiFields(
        context, testInstance, version, TargetVersion.TESTED, ctx -> nessieServer);

    ServerKey serverKeyNext = buildServerKey(Version.CURRENT, context);
    NessieServer nessieServerNext = nessieServerExisting(context, serverKeyNext);
    populateNessieApiFields(
        context, testInstance, Version.CURRENT, TargetVersion.CURRENT, ctx -> nessieServerNext);
  }

  private ServerKey buildServerKey(Version version, ExtensionContext context) {
    MongoHolder mongo = getMongoHolder(context);

    // Eagerly create the Nessie server instance
    Map<String, String> configuration = mongo.mongo.getQuarkusConfig();

    return ServerKey.forContext(context, version, "MongoDB", configuration);
  }

  private static MongoHolder getMongoHolder(ExtensionContext context) {
    return globalForClass(context)
        .getOrCompute("local-mongo", x -> new MongoHolder(), MongoHolder.class);
  }

  private void populateNessieApiFields(
      ExtensionContext context,
      Object instance,
      Version version,
      TargetVersion targetVersion,
      Function<ExtensionContext, NessieServer> nessieServerSupplier) {
    AnnotatedFields.populateNessieApiFields(
        context,
        instance,
        targetVersion,
        field -> apiInstanceForField(context, field, version, nessieServerSupplier));
  }

  static class MongoHolder implements AutoCloseable {
    final MongoDBBackendTestFactory mongo = new MongoDBBackendTestFactory();

    boolean started;

    synchronized void start() {
      if (!started) {
        mongo.start();
        started = true;
      }
    }

    @Override
    public void close() {
      mongo.stop();
    }
  }
}
