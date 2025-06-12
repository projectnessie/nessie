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

import static org.projectnessie.tools.compatibility.internal.CurrentNessieServer.currentNessieServer;
import static org.projectnessie.tools.compatibility.internal.OldNessieServer.oldNessieServer;
import static org.projectnessie.tools.compatibility.internal.Util.extensionStore;

import java.net.URI;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.tools.compatibility.api.Version;

interface NessieServer extends AutoCloseable {

  static NessieServer nessieServerExisting(ExtensionContext context, ServerKey serverKey) {
    return Objects.requireNonNull(
        extensionStore(context).get(serverKey, NessieServer.class),
        "No Nessie server for " + serverKey);
  }

  static NessieServer nessieServer(
      ExtensionContext context,
      ServerKey serverKey,
      BooleanSupplier initRepo,
      Consumer<Object> backendConfigConsumer) {
    if (Version.CURRENT.equals(serverKey.getVersion())) {
      return currentNessieServer(context, serverKey, initRepo, backendConfigConsumer);
    } else {
      return oldNessieServer(context, serverKey, initRepo, backendConfigConsumer);
    }
  }

  URI getUri(Class<? extends NessieApi> apiType);
}
