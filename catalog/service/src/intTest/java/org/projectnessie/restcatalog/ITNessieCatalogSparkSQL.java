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
package org.projectnessie.restcatalog;

import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtension;

import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.restcatalog.service.testing.CatalogTestHelper;
import org.projectnessie.spark.extensions.AbstractNessieSparkSqlExtensionTest;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith(PersistExtension.class)
public class ITNessieCatalogSparkSQL extends AbstractNessieSparkSqlExtensionTest {
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

  // Spark stuff

  @Override
  protected String nessieApiUri() {
    return catalogTestHelper.nessieCoreRestUri().resolve("v1").toString();
  }

  @Override
  protected Map<String, String> nessieParams() {
    Map<String, String> r = new HashMap<>(super.nessieParams());
    r.remove("ref");
    r.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
    r.put(CatalogProperties.URI, catalogTestHelper.icebergRestUri().toString());
    r.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
    r.put(OAuth2Properties.CREDENTIAL, "client_id:client_secret");
    r.put(OAuth2Properties.SCOPE, "oauth-scope");
    r.put("prefix", "main");
    r.put("warehouse", "warehouse");
    return r;
  }
}
