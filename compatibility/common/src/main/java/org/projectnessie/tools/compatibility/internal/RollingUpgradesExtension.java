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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.tools.compatibility.api.TargetVersion;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.versioned.persist.mongodb.LocalMongoResource;

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

    ServerKey serverKey = buildServerKey(version, context);
    NessieServer nessieServer = nessieServer(context, serverKey, () -> true);
    populateNessieApiFields(context, null, version, TargetVersion.TESTED, ctx -> nessieServer);

    ServerKey serverKeyNext = buildServerKey(Version.CURRENT, context);
    NessieServer nessieServerNext = nessieServer(context, serverKeyNext, () -> false);
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
    LocalMongoResource mongo =
        globalForClass(context)
            .getOrCompute("local-mongo", x -> new LocalMongoResource(), LocalMongoResource.class);

    // Eagerly create the Nessie server instance
    String databaseAdapterName = "MongoDB";
    Map<String, String> configuration =
        ImmutableMap.of(
            "nessie.store.connection.string",
            mongo.getConnectionString(),
            "nessie.store.database.name",
            mongo.getDatabaseName());

    return new ServerKey(version, databaseAdapterName, configuration);
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
}
