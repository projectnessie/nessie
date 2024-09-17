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
package org.projectnessie.nessie.combined;

import static org.projectnessie.nessie.combined.EmptyHttpHeaders.emptyHttpHeaders;
import static org.projectnessie.services.authz.ApiContext.apiContext;

import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.ApiContext;
import org.projectnessie.services.authz.Authorizer;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.rest.RestV2ConfigResource;
import org.projectnessie.services.rest.RestV2TreeResource;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;

/**
 * Builder for a {@link NessieApiV2} client that directly accesses the {@linkplain
 * #withPersist(Persist) provided <code>Persist</code>} instance.
 */
public class CombinedClientBuilder extends NessieClientBuilder.AbstractNessieClientBuilder {
  private Persist persist;
  private RestV2ConfigResource configResource;
  private RestV2TreeResource treeResource;
  private ApiContext apiContext = apiContext("Nessie", 2);

  public CombinedClientBuilder() {}

  @Override
  public String name() {
    return "Combined";
  }

  @Override
  public int priority() {
    return 50;
  }

  public CombinedClientBuilder withConfigResource(RestV2ConfigResource configResource) {
    this.configResource = configResource;
    return this;
  }

  public CombinedClientBuilder withTreeResource(RestV2TreeResource treeResource) {
    this.treeResource = treeResource;
    return this;
  }

  public CombinedClientBuilder withPersist(Persist persist) {
    this.persist = persist;
    return this;
  }

  public CombinedClientBuilder withApiContext(ApiContext apiContext) {
    this.apiContext = apiContext;
    return this;
  }

  @Override
  public <API extends NessieApi> API build(Class<API> apiContract) {
    RestV2ConfigResource configResource = this.configResource;
    RestV2TreeResource treeResource = this.treeResource;

    if (configResource != null && treeResource != null) {
      // Optimistic cast...
      @SuppressWarnings("unchecked")
      API r = (API) new CombinedClientImpl(configResource, treeResource);
      return r;
    }

    if (configResource != null || treeResource != null) {
      throw new IllegalStateException(
          "configResource or treeResource configured - this indicates a product bug");
    }

    ServerConfig serverConfig =
        new ServerConfig() {
          @Override
          public String getDefaultBranch() {
            return "main";
          }

          @Override
          public boolean sendStacktraceToClient() {
            return true;
          }
        };

    VersionStore versionStore = new VersionStoreImpl(persist);
    Authorizer authorizer = (c, apiContext) -> AbstractBatchAccessChecker.NOOP_ACCESS_CHECKER;

    AccessContext accessContext = () -> null;

    configResource =
        new RestV2ConfigResource(serverConfig, versionStore, authorizer, accessContext);
    treeResource =
        new RestV2TreeResource(
            serverConfig, versionStore, authorizer, accessContext, emptyHttpHeaders());

    // Optimistic cast...
    @SuppressWarnings("unchecked")
    API r = (API) new CombinedClientImpl(configResource, treeResource);
    return r;
  }
}
