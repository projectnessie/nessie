/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.jaxrs.ext;

import static org.projectnessie.services.config.ServerConfigExtension.SERVER_CONFIG;

import java.net.URI;
import java.util.function.Supplier;
import javax.ws.rs.core.Application;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.services.authz.AccessCheckerExtension;
import org.projectnessie.services.config.ServerConfigExtension;
import org.projectnessie.services.impl.ConfigApiImpl;
import org.projectnessie.services.impl.TreeApiImpl;
import org.projectnessie.services.rest.ContentsKeyParamConverterProvider;
import org.projectnessie.services.rest.InstantParamConverterProvider;
import org.projectnessie.services.rest.NessieExceptionMapper;
import org.projectnessie.services.rest.NessieJaxRsJsonMappingExceptionMapper;
import org.projectnessie.services.rest.NessieJaxRsJsonParseExceptionMapper;
import org.projectnessie.services.rest.RestConfigResource;
import org.projectnessie.services.rest.RestContentsResource;
import org.projectnessie.services.rest.RestTreeResource;
import org.projectnessie.services.rest.ValidationExceptionMapper;
import org.projectnessie.versioned.PersistVersionStoreExtension;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

/** A JUnit 5 extension that starts up Weld/JerseyTest. */
public class NessieJaxRsExtension implements BeforeAllCallback, AfterAllCallback {

  private final Supplier<DatabaseAdapter> databaseAdapterSupplier;
  private Weld weld;
  private JerseyTest jerseyTest;

  public NessieJaxRsExtension() {
    throw new UnsupportedOperationException();
  }

  public NessieJaxRsExtension(Supplier<DatabaseAdapter> databaseAdapterSupplier) {
    this.databaseAdapterSupplier = databaseAdapterSupplier;
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    weld = new Weld();
    // Let Weld scan all the resources to discover injection points and dependencies
    weld.addPackages(true, TreeApiImpl.class);
    // Inject external beans
    weld.addExtension(new ServerConfigExtension());
    weld.addExtension(
        PersistVersionStoreExtension.forDatabaseAdapter(
            () -> {
              DatabaseAdapter databaseAdapter = databaseAdapterSupplier.get();
              databaseAdapter.reinitializeRepo(SERVER_CONFIG.getDefaultBranch());
              return databaseAdapter;
            }));
    weld.addExtension(new AccessCheckerExtension());
    final WeldContainer container = weld.initialize();

    jerseyTest =
        new JerseyTest() {
          @Override
          protected Application configure() {
            ResourceConfig config = new ResourceConfig();
            config.register(RestConfigResource.class);
            config.register(RestTreeResource.class);
            config.register(RestContentsResource.class);
            config.register(ConfigApiImpl.class);
            config.register(ContentsKeyParamConverterProvider.class);
            config.register(InstantParamConverterProvider.class);
            config.register(ValidationExceptionMapper.class, 10);
            config.register(NessieExceptionMapper.class);
            config.register(NessieJaxRsJsonParseExceptionMapper.class, 10);
            config.register(NessieJaxRsJsonMappingExceptionMapper.class, 10);
            return config;
          }
        };

    jerseyTest.setUp();
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    if (null != jerseyTest) {
      jerseyTest.tearDown();
    }
    if (null != weld) {
      weld.shutdown();
    }
  }

  public URI getURI() {
    if (null == jerseyTest) {
      return null;
    }
    return jerseyTest.target().getUri();
  }
}
