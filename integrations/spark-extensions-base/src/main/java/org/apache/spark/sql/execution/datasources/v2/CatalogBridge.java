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
package org.apache.spark.sql.execution.datasources.v2;

import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Reference;

public interface CatalogBridge extends AutoCloseable {
  CatalogPlugin currentCatalog();

  NessieApiV1 api();

  void setCurrentRefForSpark(Reference ref, boolean configureRefAtHash);

  Reference getCurrentRef() throws NessieReferenceNotFoundException;

  @Override
  void close();
}
