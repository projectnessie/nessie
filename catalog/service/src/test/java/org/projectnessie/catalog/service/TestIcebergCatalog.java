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
package org.projectnessie.catalog.service;

import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtension;

import java.net.URI;
import java.nio.file.Path;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.catalog.service.testing.CatalogTestHelper;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith(PersistExtension.class)
public class TestIcebergCatalog extends CatalogTests<RESTCatalog> {

  @NessiePersist protected static Persist persist;

  @RegisterExtension static NessieJaxRsExtension server = jaxRsExtension(() -> persist);

  private static CatalogTestHelper catalogTestHelper;

  @TempDir static Path tempDir;

  @BeforeAll
  public static void setup(@NessieClientUri URI nessieUri) throws Exception {
    catalogTestHelper = new CatalogTestHelper(nessieUri, tempDir);
    catalogTestHelper.start();
  }

  @AfterAll
  public static void shutdown() throws Exception {
    catalogTestHelper.close();
  }

  @BeforeEach
  public void clearRepo() throws Exception {
    catalogTestHelper.clearRepo();
  }

  @Override
  protected RESTCatalog catalog() {
    return catalogTestHelper.createCatalog();
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNamespaceProperties() {
    return true;
  }

  @Override
  protected boolean supportsServerSideRetry() {
    return true;
  }

  @Override
  protected boolean supportsNestedNamespaces() {
    return true;
  }
}
