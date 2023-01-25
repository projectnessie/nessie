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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Objects;
import javax.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Tag;

@QuarkusTest
@TestProfile(QuarkusNessieIcebergQuarkusProfile.class)
public class ITNessieRestCatalogQuarkus extends CatalogTests<RESTCatalog> {

  @Inject NessieIcebergRestConfig config;

  private static int icebergRestQuarkusPort() {
    return Objects.requireNonNull(
        Integer.getInteger("quarkus.http.test-port"),
        "System property not set correctly: quarkus.http.test-port");
  }

  String uri() {
    return format("http://127.0.0.1:%d/iceberg/", icebergRestQuarkusPort());
  }

  public NessieApiV2 buildApi() {
    return HttpClientBuilder.builder()
        .fromSystemProperties()
        .fromConfig(cfg -> config.nessieClientConfig().get(cfg))
        .build(NessieApiV2.class);
  }

  @BeforeEach
  public void clearRepo() throws Exception {
    try (NessieApiV2 api = buildApi()) {
      Branch main = api.getDefaultBranch();
      Branch empty =
          Branch.of(
              main.getName(), "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d");

      api.getAllReferences().stream()
          .forEach(
              ref -> {
                try {
                  if (ref instanceof Branch) {
                    if (!ref.getName().equals(main.getName())) {
                      api.deleteBranch().branch((Branch) ref).delete();
                    }
                  } else if (ref instanceof Tag) {
                    api.deleteTag().tag((Tag) ref).delete();
                  }
                } catch (NessieConflictException | NessieNotFoundException e) {
                  throw new RuntimeException(e);
                }
              });

      checkState(api.assignBranch().branch(main).assignTo(empty).assignAndGet().equals(empty));
    }
  }

  @Override
  protected RESTCatalog catalog() {
    try (NessieApiV2 api = buildApi()) {
      RESTCatalog catalog = new RESTCatalog();
      catalog.setConf(new Configuration());
      catalog.initialize(
          "nessie-iceberg-rest",
          ImmutableMap.of(
              CatalogProperties.URI,
              uri(),
              CatalogProperties.FILE_IO_IMPL,
              "org.apache.iceberg.io.ResolvingFileIO",
              OAuth2Properties.TOKEN,
              "foo-bar-token",
              OAuth2Properties.CREDENTIAL,
              "foo-bar-credential",
              "prefix",
              requireNonNull(api.getConfig().getDefaultBranch())));
      return catalog;
    }
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
