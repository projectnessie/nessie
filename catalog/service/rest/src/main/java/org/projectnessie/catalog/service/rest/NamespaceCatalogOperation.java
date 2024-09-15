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
package org.projectnessie.catalog.service.rest;

import java.util.List;
import java.util.Map;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.ops.CatalogOperation;
import org.projectnessie.catalog.model.ops.CatalogOperationType;
import org.projectnessie.catalog.model.ops.CatalogUpdate;
import org.projectnessie.catalog.service.rest.NamespaceCatalogOperation.NamespaceUpdate;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface NamespaceCatalogOperation extends CatalogOperation<NamespaceUpdate> {

  @Override
  CatalogOperationType getOperationType();

  @Override
  ContentKey getContentKey();

  @Value.Default
  @Override
  default Content.Type getContentType() {
    return Content.Type.NAMESPACE;
  }

  @Override
  List<NamespaceUpdate> getUpdates();

  interface NamespaceUpdate extends CatalogUpdate {

    @NessieImmutable
    interface SetProperties extends NamespaceUpdate {

      @Value.Default
      @Override
      default String getAction() {
        return "set-properties";
      }

      Map<String, String> updates();
    }

    @NessieImmutable
    interface RemoveProperties extends NamespaceUpdate {

      @Value.Default
      @Override
      default String getAction() {
        return "remove-properties";
      }

      List<String> removals();
    }
  }
}
