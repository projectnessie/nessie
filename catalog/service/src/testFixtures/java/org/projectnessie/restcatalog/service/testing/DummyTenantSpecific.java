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
package org.projectnessie.restcatalog.service.testing;

import java.net.URI;
import java.util.Map;
import javax.enterprise.inject.Vetoed;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.restcatalog.service.TenantSpecific;
import org.projectnessie.restcatalog.service.Warehouse;
import org.projectnessie.restcatalog.service.auth.OAuthHandler;

@Vetoed
@jakarta.enterprise.inject.Vetoed
public class DummyTenantSpecific implements TenantSpecific {

  private static final String COMMIT_AUTHOR =
      "Nessie Prototype Factory <nessie-prototype-factory@projectnessie.org>";

  private final NessieApiV2 api;
  private final URI nessieApiBaseUri;
  private final ParsedReference defaultBranch;
  private final Warehouse defaultWarehouse;
  private final OAuthHandler oauthHandler;
  private final Map<String, String> clientCoreProperties;

  @SuppressWarnings("unused")
  public DummyTenantSpecific() {
    throw new UnsupportedOperationException();
  }

  public DummyTenantSpecific(
      OAuthHandler oauthHandler,
      NessieApiV2 api,
      URI nessieApiBaseUri,
      ParsedReference defaultBranch,
      Warehouse defaultWarehouse,
      Map<String, String> clientCoreProperties) {
    this.oauthHandler = oauthHandler;
    this.api = api;
    this.nessieApiBaseUri = nessieApiBaseUri;
    this.defaultBranch = defaultBranch;
    this.defaultWarehouse = defaultWarehouse;
    this.clientCoreProperties = clientCoreProperties;
  }

  @Override
  public OAuthHandler oauthHandler() {
    return oauthHandler;
  }

  @Override
  public ParsedReference defaultBranch() {
    return defaultBranch;
  }

  @Override
  public Warehouse defaultWarehouse() {
    return defaultWarehouse;
  }

  @Override
  public Warehouse getWarehouse(String warehouse) {
    if (warehouse == null || defaultWarehouse.name().equals(warehouse)) {
      return defaultWarehouse;
    }
    throw new IllegalArgumentException("Unknown warehouse " + warehouse);
  }

  @Override
  public NessieApiV2 api() {
    return api;
  }

  @Override
  public URI nessieApiBaseUri() {
    return nessieApiBaseUri;
  }

  @Override
  public String commitAuthor() {
    return COMMIT_AUTHOR;
  }

  @Override
  public Map<String, String> clientCoreProperties() {
    return clientCoreProperties;
  }
}
