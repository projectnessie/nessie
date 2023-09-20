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
import static org.projectnessie.tools.compatibility.internal.AnnotatedFields.populateAnnotatedFields;
import static org.projectnessie.tools.compatibility.internal.AnnotatedFields.populateNessieApiFields;
import static org.projectnessie.tools.compatibility.internal.NessieServer.nessieServer;
import static org.projectnessie.tools.compatibility.internal.Util.classContext;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.client.rest.v1.HttpApiV1;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;
import org.projectnessie.tools.compatibility.api.TargetVersion;
import org.projectnessie.tools.compatibility.api.Version;

/**
 * Populates the {@link org.projectnessie.client.api.NessieApi} type fields in test classes and
 * starts a Nessie server using the currently exercised old Nessie version.
 */
public class OlderNessieServersExtension extends AbstractMultiVersionExtension {

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

    BooleanSupplier initializeRepository = () -> true;
    ServerKey serverKey =
        ServerKey.forContext(context, version, "In-Memory", Collections.emptyMap());
    NessieServer nessieServer =
        nessieServer(classContext(context), serverKey, initializeRepository, c -> {});

    Function<Field, Object> fieldValue =
        field ->
            apiInstanceForField(classContext(context), field, Version.CURRENT, ctx -> nessieServer);

    populateNessieApiFields(context, instance, TargetVersion.TESTED, fieldValue);

    URI serverUri = nessieServer.getUri(HttpApiV1.class);
    URI baseUri = serverUri.resolve("/"); // remove possible API version suffixes
    populateAnnotatedFields(context, instance, NessieBaseUri.class, a -> true, f -> baseUri);
  }
}
