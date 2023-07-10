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
package org.projectnessie.restcatalog.service;

import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtension;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.restcatalog.service.testing.CatalogTestHelper;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestNessieProxy {

  @NessiePersist protected static Persist persist;

  @RegisterExtension static NessieJaxRsExtension server = jaxRsExtension(() -> persist);

  @InjectSoftAssertions protected SoftAssertions soft;

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

  @ParameterizedTest
  @ValueSource(strings = {"../../foo/bar", ".."})
  public void cantTraversePathBack(String add) throws Exception {
    URI target = URI.create(catalogTestHelper.nessieCoreRestUri() + add);
    HttpURLConnection conn = (HttpURLConnection) target.toURL().openConnection();
    soft.assertThat(conn.getResponseCode()).isEqualTo(400);
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1", "v2"})
  public void nessieConfig(String version) throws Exception {
    URI target = catalogTestHelper.nessieCoreRestUri().resolve(version + "/config");
    HttpURLConnection conn = (HttpURLConnection) target.toURL().openConnection();
    soft.assertThat(conn.getResponseCode()).isEqualTo(200);
    soft.assertThat(conn.getContentType()).startsWith("application/json");
  }

  @ParameterizedTest
  @ValueSource(strings = {"v1/config", "v1/config/"})
  public void icebergConfig(String path) throws Exception {
    URI target = catalogTestHelper.icebergRestUri().resolve(path);
    HttpURLConnection conn = (HttpURLConnection) target.toURL().openConnection();
    @SuppressWarnings("unchecked")
    Map<String, ?> config = new ObjectMapper().readValue(conn.getInputStream(), Map.class);
    soft.assertThat(config)
        .containsKey("overrides")
        .extracting(c -> c.get("overrides"), map(String.class, String.class))
        .containsEntry("nessie.default-branch.name", "main")
        .containsEntry("nessie.core-base-uri", catalogTestHelper.nessieCoreRestUri().toString())
        .containsEntry(
            "nessie.catalog-base-uri", catalogTestHelper.nessieCatalogRestUri().toString())
        .containsEntry("nessie.prefix-pattern", "{ref}|{warehouse}")
        .containsEntry("nessie.is-nessie-catalog", "true");
  }
}
