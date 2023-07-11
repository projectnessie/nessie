/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.server;

import static java.util.Objects.requireNonNull;
import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TOKEN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_REF;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_REF_HASH;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;
import static org.projectnessie.model.Reference.ReferenceType.BRANCH;

import io.quarkus.runtime.Startup;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.service.spi.OAuthHandler;
import org.projectnessie.catalog.service.spi.TenantSpecific;
import org.projectnessie.catalog.service.spi.Warehouse;
import org.projectnessie.client.api.NessieApiV2;

@ApplicationScoped
public class TenantSpecificProvider {

  @Inject OAuthHandler oauthHandler;
  @Inject FileIO fileIO;
  @Inject NessieApiV2 api;
  @Inject NessieIcebergRestConfig config;

  @Produces
  @Singleton
  @Startup
  public TenantSpecific produceTenantSpecific() {
    ParsedReference defaultBranch = parsedReference("main", null, BRANCH);
    Warehouse defaultWarehouse =
        Warehouse.builder()
            .name("warehouse")
            .location(config.warehouseLocation())
            .fileIO(fileIO)
            .build();
    String commitAuthor = null;

    URI apiBaseUri =
        URI.create(
            requireNonNull(
                config.nessieClientConfig().get(CONF_NESSIE_URI),
                "nessie.iceberg.nessie-client.\"nessie.uri\" must be configured, but is not available"));
    String apiPath = apiBaseUri.getPath();
    apiBaseUri = apiBaseUri.resolve(apiPath + "/..").normalize();

    // Pass (most of) the Nessie Core client configuration to Nessie Catalog clients.
    Map<String, String> clientCoreProperties = new HashMap<>(config.nessieClientConfig());
    clientCoreProperties.remove(CONF_NESSIE_AUTH_TOKEN);
    clientCoreProperties.remove(CONF_NESSIE_REF);
    clientCoreProperties.remove(CONF_NESSIE_REF_HASH);
    clientCoreProperties.remove(CONF_NESSIE_URI);

    return new DefaultTenantSpecific(
        oauthHandler,
        defaultBranch,
        defaultWarehouse,
        api,
        apiBaseUri,
        commitAuthor,
        clientCoreProperties);
  }
}
