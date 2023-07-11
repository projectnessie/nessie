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
package org.projectnessie.catalog.service.testing;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.jboss.weld.environment.se.Weld.SHUTDOWN_HOOK_SYSTEM_PROPERTY;
import static org.projectnessie.api.v2.params.ReferenceResolver.resolveReferencePathElement;

import com.fasterxml.jackson.core.util.JacksonFeature;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.nio.file.Path;
import javax.ws.rs.core.Application;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.projectnessie.api.v2.params.ParsedReference;
import org.projectnessie.catalog.service.ee.javax.ContextObjectMapper;
import org.projectnessie.catalog.service.ee.javax.JavaxExceptionMapper;
import org.projectnessie.catalog.service.ee.javax.OAuthTokenRequestReader;
import org.projectnessie.catalog.service.resources.IcebergV1ApiResource;
import org.projectnessie.catalog.service.resources.IcebergV1ConfigResource;
import org.projectnessie.catalog.service.resources.IcebergV1OAuthResource;
import org.projectnessie.catalog.service.resources.javax.NessieProxyResource;
import org.projectnessie.catalog.service.spi.OAuthHandler;
import org.projectnessie.catalog.service.spi.Warehouse;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Tag;

public class CatalogTestHelper implements AutoCloseable {

  private final URI nessieApiUri;
  private final Path tempDir;
  private NessieApiV2 api;
  private Weld weld;
  private JerseyTest jerseyTest;

  public CatalogTestHelper(URI nessieApiUri, Path tempDir) {
    this.nessieApiUri = nessieApiUri;
    this.tempDir = tempDir;
  }

  public void start() throws Exception {
    api =
        HttpClientBuilder.builder()
            .withUri(nessieApiUri)
            .fromSystemProperties()
            .build(NessieApiV2.class);

    String defaultBranchName = requireNonNull(api.getConfig().getDefaultBranch());
    ParsedReference defaultBranch =
        resolveReferencePathElement(defaultBranchName, null, () -> defaultBranchName);

    String warehouseDir = tempDir.toUri().toString();
    FileIO fileIO = new LocalFileIO();
    Warehouse defaultWarehouse =
        Warehouse.builder().name("warehouse").location(warehouseDir).fileIO(fileIO).build();

    // TODO
    OAuthHandler oauthHandler =
        req -> OAuthTokenResponse.builder().withToken("tok").withTokenType("bearer").build();

    weld = new Weld();
    weld.addExtension(
        new WeldTestingExtension(
            oauthHandler, api, nessieApiUri, defaultBranch, defaultWarehouse, emptyMap()));
    weld.addPackages(true, DummyTenantSpecific.class);
    weld.addPackages(true, IcebergV1ApiResource.class);
    weld.addPackages(true, JavaxExceptionMapper.class);
    weld.addPackages(true, OAuthTokenRequestReader.class);
    weld.property(SHUTDOWN_HOOK_SYSTEM_PROPERTY, "false");
    WeldContainer weldContainer = weld.initialize();
    ForcedCDI.setCDI(weldContainer);

    jerseyTest =
        new JerseyTest() {
          @Override
          protected Application configure() {
            ResourceConfig config = new ResourceConfig();

            config.register(JacksonFeature.class);
            config.register(ContextObjectMapper.class);

            config.register(JavaxExceptionMapper.class);

            config.register(IcebergV1ConfigResource.class);
            config.register(IcebergV1ApiResource.class);
            config.register(IcebergV1OAuthResource.class);
            config.register(OAuthTokenRequestReader.class);

            config.register(NessieProxyResource.class);

            // Use a dynamically allocated port, not a static default (80/443) or statically
            // configured port.
            set(TestProperties.CONTAINER_PORT, "0");

            return config;
          }
        };
    jerseyTest.setUp();
  }

  @Override
  public void close() throws Exception {
    ForcedCDI.clear();
    try {
      try {
        if (jerseyTest != null) {
          jerseyTest.tearDown();
        }
      } finally {
        if (weld != null) {
          weld.shutdown();
        }
      }
    } finally {
      if (api != null) {
        try {
          clearRepo();
        } finally {
          api.close();
        }
      }
    }
  }

  public URI uri() {
    return jerseyTest.target().getUri();
  }

  public URI icebergRestUri() {
    return uri().resolve("iceberg/");
  }

  public URI nessieCoreRestUri() {
    return uri().resolve("api/");
  }

  public URI nessieCatalogRestUri() {
    return uri().resolve("nessie-catalog/");
  }

  public RESTCatalog createCatalog() {
    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize(
        "nessie-iceberg-api",
        ImmutableMap.of(
            CatalogProperties.URI,
            icebergRestUri().toString(),
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.io.ResolvingFileIO",
            "prefix",
            requireNonNull(api.getConfig().getDefaultBranch())));
    return catalog;
  }

  public void clearRepo() throws Exception {
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
