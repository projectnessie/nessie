/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.server.catalog;

import static java.util.Collections.singletonMap;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.WAREHOUSE_NAME;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;

public abstract class AbstractIcebergViewCatalogTests extends ViewCatalogTests<RESTCatalog> {
  public static final String EMPTY_OBJ_ID =
      "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d";

  private static final Catalogs CATALOGS = new Catalogs();

  protected RESTCatalog catalog() {
    return CATALOGS.getCatalog(catalogOptions());
  }

  protected Map<String, String> catalogOptions() {
    return singletonMap(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE_NAME);
  }

  @AfterAll
  static void closeRestCatalog() throws Exception {
    CATALOGS.close();
  }

  @Override
  protected Catalog tableCatalog() {
    return catalog();
  }

  @AfterEach
  void cleanup() throws Exception {
    int catalogServerPort = Integer.getInteger("quarkus.http.port");

    try (NessieApiV2 api =
        NessieClientBuilder.createClientBuilderFromSystemSettings()
            .withUri(String.format("http://127.0.0.1:%d/api/v2/", catalogServerPort))
            .build(NessieApiV2.class)) {
      Reference main = null;
      for (Reference reference : api.getAllReferences().stream().toList()) {
        if (reference.getName().equals("main")) {
          main = reference;
        } else {
          api.deleteReference().reference(reference).delete();
        }
      }
      api.assignReference().reference(main).assignTo(Branch.of("main", EMPTY_OBJ_ID)).assign();
    }
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean exposesHistory() {
    return false;
  }

  @Override
  protected boolean physicalMetadataLocation() {
    return false;
  }

  @Override
  protected boolean overridesRequestedLocation() {
    // TODO Nessie Catalog should force the metadata location
    return super.overridesRequestedLocation();
  }

  @Override
  protected String temporaryLocation() {
    throw new UnsupportedOperationException("Implement in a super class");
  }
}
