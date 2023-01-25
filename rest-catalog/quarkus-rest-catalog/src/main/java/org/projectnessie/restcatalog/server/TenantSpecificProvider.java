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
package org.projectnessie.restcatalog.server;

import static org.projectnessie.api.v2.params.ParsedReference.parsedReference;
import static org.projectnessie.model.Reference.ReferenceType.BRANCH;

import io.quarkus.runtime.Startup;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.restcatalog.metadata.DelegatingMetadataIO;
import org.projectnessie.restcatalog.metadata.MetadataIO;
import org.projectnessie.restcatalog.service.TenantSpecific;
import org.projectnessie.restcatalog.service.Warehouse;
import org.projectnessie.restcatalog.service.auth.OAuthHandler;

@RequestScoped
public class TenantSpecificProvider {

  @Inject OAuthHandler oauthHandler;
  @Inject FileIO fileIO;
  @Inject NessieApiV2 api;
  @Inject NessieIcebergRestConfig config;

  @Produces
  @Singleton
  @Startup
  public TenantSpecific produceTenantSpecific() {
    MetadataIO metadataIO = new DelegatingMetadataIO(fileIO);
    ParsedReference defaultBranch = parsedReference("main", null, BRANCH);
    Warehouse defaultWarehouse =
        Warehouse.builder().name("warehouse").location(config.warehouseLocation()).build();
    String commitAuthor = null;

    return new DefaultTenantSpecific(
        oauthHandler, metadataIO, defaultBranch, defaultWarehouse, api, commitAuthor);
  }
}
