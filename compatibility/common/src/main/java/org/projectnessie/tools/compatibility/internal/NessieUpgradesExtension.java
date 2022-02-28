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
import static org.projectnessie.tools.compatibility.internal.AnnotatedFields.populateNessieAnnotatedFields;
import static org.projectnessie.tools.compatibility.internal.GlobalForClass.globalForClass;
import static org.projectnessie.tools.compatibility.internal.NessieServer.nessieServer;
import static org.projectnessie.tools.compatibility.internal.NessieServer.nessieServerExisting;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.tools.compatibility.api.Version;

/**
 * Populates the {@link org.projectnessie.client.api.NessieApi} type fields in test classes and
 * starts a Nessie server (in-tree version), when needed.
 *
 * <p>Instances of {@link org.projectnessie.client.api.NessieApi} look like in-tree versions to the
 * tests, but are using the old Nessie version client code. Method calls are translated using Java
 * proxies, model classes are re-serialized.
 */
public class NessieUpgradesExtension extends AbstractMultiVersionExtension {

  @Override
  public void beforeAll(ExtensionContext context) {
    Version version = populateNessieVersionAnnotatedFields(context, null);
    if (version == null) {
      return;
    }

    ServerKey serverKey = buildServerKey(version, context);

    NessieServer nessieServer =
        nessieServer(context, serverKey, initializeRepositorySupplier(context, serverKey));

    populateFields(context, null, version, ctx -> nessieServer);
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

    populateFields(context, testInstance, version, ctx -> nessieServer);
  }

  private ServerKey buildServerKey(Version version, ExtensionContext context) {
    Path tempDir =
        globalForClass(context)
            .getOrCompute(
                "temporary-directory", x -> new TemporaryDirectory(), TemporaryDirectory.class)
            .getPath();

    // Eagerly create the Nessie server instance
    String databaseAdapterName = "RocksDB";
    Map<String, String> configuration =
        Collections.singletonMap("nessie.store.db.path", tempDir.resolve("persist").toString());

    return new ServerKey(version, databaseAdapterName, configuration);
  }

  private BooleanSupplier initializeRepositorySupplier(
      ExtensionContext context, ServerKey serverKey) {
    return () ->
        globalForClass(context)
            .getOrCompute(
                "initialize-repository-" + serverKey.getDatabaseAdapterName(),
                k -> new AtomicBoolean(true),
                AtomicBoolean.class)
            .getAndSet(false);
  }

  private void populateFields(
      ExtensionContext context,
      Object instance,
      Version version,
      Function<ExtensionContext, NessieServer> nessieServerSupplier) {
    populateNessieAnnotatedFields(
        context,
        instance,
        field -> apiInstanceForField(context, field, version, nessieServerSupplier));
  }
}
