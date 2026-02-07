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
package org.projectnessie.server;

import static java.lang.String.format;

import java.net.URI;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.client.ext.NessieClientResolver;

public class QuarkusNessieClientResolver extends NessieClientResolver {

  @Override
  protected URI getBaseUri(ExtensionContext extensionContext) {
    // From https://github.com/quarkusio/quarkus/pull/51867 :
    // "This is still available in the Config with the compatibility layer, but users should start
    // moving to HttpServer#getLocalBaseUri, and @TestHTTPResource."
    //
    // The tricky issue here is that QuarkusNessieClientResolver is used as a JUnit extension and
    // there is no guaranteed order of extension-initialization.
    // We also cannot `@Inject` something into an extension, so we have to rely on the "legacy way"
    // to get the port..
    var quarkusHttpPort = ConfigProvider.getConfig().getConfigValue("quarkus.http.port");
    var httpPort = Integer.parseInt(quarkusHttpPort.getValue());
    return URI.create(format("http://localhost:%d/", httpPort));
  }
}
