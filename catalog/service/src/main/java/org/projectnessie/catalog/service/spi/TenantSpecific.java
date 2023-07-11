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
package org.projectnessie.catalog.service.spi;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Namespace;

public interface TenantSpecific {

  OAuthHandler oauthHandler();

  ParsedReference defaultBranch();

  Warehouse defaultWarehouse();

  Warehouse getWarehouse(String warehouse);

  NessieApiV2 api();

  URI nessieApiBaseUri();

  String commitAuthor();

  Map<String, String> clientCoreProperties();

  default CommitMeta buildCommitMeta(String message) {
    return CommitMeta.builder()
        .message(message)
        .authorTime(Instant.now())
        .author(commitAuthor())
        .build();
  }

  default String defaultTableLocation(
      String location, TableRef table, Branch ref, Warehouse warehouse)
      throws NessieReferenceNotFoundException {
    if (location != null) {
      return location;
    }

    Namespace ns = table.contentKey().getNamespace();
    if (!ns.isEmpty()) {
      String baseLocation = warehouse.location() + "/" + ns;
      try {
        try {
          ns = api().getNamespace().reference(ref).namespace(ns).get();
        } catch (NessieNamespaceNotFoundException e) {
          // ignore
        }

        baseLocation = ns.getProperties().getOrDefault("location", baseLocation);
      } catch (NoSuchNamespaceException e) {
        // do nothing we want the same behavior that if the location is not defined
      }
      location = baseLocation + "/" + table.contentKey().getName();
    } else {
      location = warehouse.location() + "/" + table.contentKey().getName();
    }
    // Different tables with same table name can exist across references in Nessie.
    // To avoid sharing same table path between two tables with same name, use uuid in the table
    // path.
    return location + "_" + UUID.randomUUID();
  }
}
