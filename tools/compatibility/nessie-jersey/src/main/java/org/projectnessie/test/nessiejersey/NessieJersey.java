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
package org.projectnessie.test.nessiejersey;

import static org.projectnessie.services.config.ServerConfigExtension.SERVER_CONFIG;

import java.io.Closeable;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import javax.ws.rs.core.Application;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.jboss.weld.environment.se.Weld;
import org.projectnessie.services.authz.AccessCheckerExtension;
import org.projectnessie.services.config.ServerConfigExtension;
import org.projectnessie.services.impl.TreeApiImpl;
import org.projectnessie.versioned.PersistVersionStoreExtension;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryStore;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;

public class NessieJersey implements Closeable {

  private final Weld weld;
  private final JerseyTest jerseyTest;

  private static DatabaseAdapter inmemoryDatabaseAdapter() {
    return new InmemoryDatabaseAdapter(
        ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build(),
        new InmemoryStore());
  }

  @SuppressWarnings("unused")
  // Used by dynamically generated test classes in compatibility tests,
  // see org.projectnessie.test.compatibility.NessieServerHelper.startIsolated
  public NessieJersey(ClassLoader classLoader) throws Exception {
    this(NessieJersey::inmemoryDatabaseAdapter, classLoader);
  }

  public NessieJersey(Supplier<DatabaseAdapter> databaseAdapterSupplier, ClassLoader classLoader)
      throws Exception {
    weld = new Weld();
    weld.setClassLoader(classLoader);
    // Let Weld scan all the resources to discover injection points and dependencies
    weld.addPackages(true, TreeApiImpl.class);
    // Inject external beans
    weld.addExtension(new ServerConfigExtension());
    weld.addExtension(
        PersistVersionStoreExtension.forDatabaseAdapter(
            () -> {
              DatabaseAdapter databaseAdapter = databaseAdapterSupplier.get();
              databaseAdapter.eraseRepo();
              databaseAdapter.initializeRepo(SERVER_CONFIG.getDefaultBranch());
              return databaseAdapter;
            }));
    weld.addExtension(new AccessCheckerExtension());
    weld.initialize();

    jerseyTest =
        new JerseyTest() {
          @Override
          protected Application configure() {
            ResourceConfig config = new ResourceConfig();
            for (String bindingClass : JERSEY_TEST_RESOURCE_BINDINGS) {
              try {
                config.register(classLoader.loadClass(bindingClass));
              } catch (ClassNotFoundException e) {
                // ignore
              }
            }
            for (String bindingClass : JERSEY_TEST_RESOURCE_BINDINGS_10) {
              try {
                config.register(classLoader.loadClass(bindingClass), 10);
              } catch (ClassNotFoundException e) {
                // ignore
              }
            }
            return config;
          }
        };

    jerseyTest.setUp();
  }

  private static final List<String> JERSEY_TEST_RESOURCE_BINDINGS =
      Arrays.asList(
          "org.projectnessie.services.rest.RestConfigResource",
          "org.projectnessie.services.rest.RestTreeResource",
          "org.projectnessie.services.rest.RestContentResource",
          "org.projectnessie.services.rest.RestDiffResource",
          "org.projectnessie.services.rest.RestRefLogResource",
          "org.projectnessie.services.impl.ConfigApiImpl",
          "org.projectnessie.services.rest.ContentKeyParamConverterProvider",
          "org.projectnessie.services.rest.InstantParamConverterProvider",
          "org.projectnessie.services.rest.NessieExceptionMapper",
          "org.glassfish.jersey.server.filter.EncodingFilter",
          "org.glassfish.jersey.message.GZipEncoder",
          "org.glassfish.jersey.message.DeflateEncoder");

  private static final List<String> JERSEY_TEST_RESOURCE_BINDINGS_10 =
      Arrays.asList(
          "org.projectnessie.services.rest.ValidationExceptionMapper",
          "org.projectnessie.services.rest.NessieJaxRsJsonParseExceptionMapper",
          "org.projectnessie.services.rest.NessieJaxRsJsonMappingExceptionMapper");

  @Override
  public void close() {
    try {
      jerseyTest.tearDown();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    weld.shutdown();
  }

  public URI getUri() {
    return jerseyTest.target().getUri();
  }
}
