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
package org.projectnessie.restcatalog.service.resources;

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT;

import java.net.URI;
import javax.inject.Inject;
import javax.ws.rs.core.UriInfo;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.projectnessie.restcatalog.api.IcebergV1Config;
import org.projectnessie.restcatalog.service.Warehouse;

public class IcebergV1ConfigResource extends BaseIcebergResource implements IcebergV1Config {

  @Inject protected UriInfo uriInfo;

  @Override
  public ConfigResponse getConfig(String warehouse) {
    Warehouse w = tenantSpecific.getWarehouse(warehouse);

    ConfigResponse.Builder config = ConfigResponse.builder();

    // TODO really need a client ID
    config.withDefault(CONF_NESSIE_OAUTH2_CLIENT_ID, "nessie-catalog-core-client");
    // TODO a non-secret secret is not a secret ...
    config.withDefault(CONF_NESSIE_OAUTH2_CLIENT_SECRET, "secret");

    config.withDefaults(w.configDefaults());
    config.withOverrides(w.configOverrides());

    // The following properties are passed back to clients to automatically configure their Nessie
    // client. These properties are _not_ user configurable properties.
    config.withOverride("nessie.default-branch.name", tenantSpecific.defaultBranch().name());
    config.withOverride("nessie.is-nessie-catalog", "true");
    // Make sure that `nessie.core-base-uri` always returns a `/` terminated URI.
    config.withOverride("nessie.core-base-uri", uriInfo.getBaseUri().resolve("api/").toString());
    // Make sure that `nessie.catalog-base-uri` always returns a `/` terminated URI.
    config.withOverride(
        "nessie.catalog-base-uri", uriInfo.getBaseUri().resolve("nessie-catalog/").toString());
    config.withOverride("nessie.prefix-pattern", "{ref}|{warehouse}");

    URI oauthUri = uriInfo.getBaseUri().resolve("iceberg/v1/oauth/tokens");

    // "Just" Nessie client specific configs
    config.withOverride(CONF_NESSIE_AUTH_TYPE, "OAUTH2");
    config.withOverride(CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT, oauthUri.toString());

    return config.build();
  }
}
