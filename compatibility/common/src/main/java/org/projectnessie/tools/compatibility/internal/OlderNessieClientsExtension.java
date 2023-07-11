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
import static org.projectnessie.tools.compatibility.internal.AnnotatedFields.populateNessieApiFields;
import static org.projectnessie.tools.compatibility.internal.NessieServer.nessieServer;
import static org.projectnessie.tools.compatibility.internal.Util.classContext;

import java.util.Collections;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.tools.compatibility.api.TargetVersion;
import org.projectnessie.tools.compatibility.api.Version;

/**
 * Populates the {@link org.projectnessie.client.api.NessieApi} type fields in test classes and
 * starts a Nessie server using the currently exercised old Nessie version.
 *
 * <p>Instances of {@link org.projectnessie.client.api.NessieApi} look like in-tree versions to the
 * tests, but are using the old Nessie version client code. Method calls are translated using Java
 * proxies, model classes are re-serialized.
 */
public class OlderNessieClientsExtension extends AbstractMultiVersionExtension {

  @Override
  public void beforeAll(ExtensionContext context) {
    populateFields(context, null);
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    populateFields(context, context.getRequiredTestInstance());
  }

  private void populateFields(ExtensionContext context, Object instance) {
    Version version = populateNessieVersionAnnotatedFields(context, instance);
    if (version == null) {
      return;
    }

    ServerKey serverKey =
        ServerKey.forContext(context, Version.CURRENT, "In-Memory", Collections.emptyMap());
    BooleanSupplier initializeRepository = () -> true;

    populateNessieApiFields(
        context,
        instance,
        TargetVersion.TESTED,
        field ->
            apiInstanceForField(
                classContext(context),
                field,
                version,
                ctx -> nessieServer(ctx, serverKey, initializeRepository, c -> {})));
  }
}
