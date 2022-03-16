/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tools.compatibility.jersey;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.enterprise.inject.spi.Extension;
import javax.ws.rs.core.Application;
import org.glassfish.jersey.message.DeflateEncoder;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.jboss.weld.environment.se.Weld;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.impl.ConfigApiImpl;
import org.projectnessie.services.impl.TreeApiImpl;
import org.projectnessie.services.rest.ContentKeyParamConverterProvider;
import org.projectnessie.services.rest.InstantParamConverterProvider;
import org.projectnessie.services.rest.NessieExceptionMapper;
import org.projectnessie.services.rest.NessieJaxRsJsonMappingExceptionMapper;
import org.projectnessie.services.rest.NessieJaxRsJsonParseExceptionMapper;
import org.projectnessie.services.rest.RestConfigResource;
import org.projectnessie.services.rest.RestContentResource;
import org.projectnessie.services.rest.RestTreeResource;
import org.projectnessie.services.rest.ValidationExceptionMapper;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

/**
 * Jersey server implementation, which uses reflection to get Nessie related classes.
 *
 * <p>This class and its companion classes in this package are used from {@code
 * org.projectnessie.tools.compatibility.internal.OldNessieServer} via {@code
 * org.projectnessie.tools.compatibility.internal.JerseyForOldServerClassLoader} and have access to
 * an old Nessie server's class path.
 */
public class JerseyServer implements AutoCloseable {
  private final Weld weld;
  private final JerseyTest jerseyTest;

  public JerseyServer(Supplier<DatabaseAdapter> databaseAdapterSupplier) throws Exception {
    weld = new Weld();
    // Let Weld scan all the resources to discover injection points and dependencies
    weld.addPackages(true, TreeApiImpl.class);
    // Inject external beans
    weld.addExtension(new ServerConfigExtension());
    weld.addExtension(PersistVersionStoreExtension.forDatabaseAdapter(databaseAdapterSupplier));
    weld.addExtension(authzExtension());
    weld.initialize();

    jerseyTest =
        new JerseyTest() {
          @Override
          protected Application configure() {
            ResourceConfig config = new ResourceConfig();
            config.register(RestConfigResource.class);
            config.register(RestTreeResource.class);
            config.register(RestContentResource.class);
            withClass("org.projectnessie.services.rest.RestDiffResource", config::register);
            withClass("org.projectnessie.services.rest.RestRefLogResource", config::register);
            withClass("org.projectnessie.services.rest.RestNamespaceResource", config::register);
            config.register(ConfigApiImpl.class);
            config.register(ContentKeyParamConverterProvider.class);
            withClass(
                "org.projectnessie.services.rest.ReferenceTypeParamConverterProvider",
                config::register);
            config.register(InstantParamConverterProvider.class);
            config.register(ValidationExceptionMapper.class, 10);
            withClass(
                "org.projectnessie.services.rest.ConstraintViolationExceptionMapper",
                c -> config.register(c, 10));
            withClass(
                "org.projectnessie.services.rest.NamespaceParamConverterProvider",
                config::register);
            config.register(NessieExceptionMapper.class);
            config.register(NessieJaxRsJsonParseExceptionMapper.class, 10);
            config.register(NessieJaxRsJsonMappingExceptionMapper.class, 10);
            config.register(EncodingFilter.class);
            config.register(GZipEncoder.class);
            config.register(DeflateEncoder.class);

            // Use a dynamically allocated port, not a static default (80/443) or statically
            // configured port.
            set(TestProperties.CONTAINER_PORT, "0");

            return config;
          }
        };

    jerseyTest.setUp();
  }

  private Extension authzExtension() {
    try {
      return new AuthorizerExtension().setAccessCheckerSupplier(this::createNewChecker);
    } catch (NoClassDefFoundError e) {
      try {
        return (Extension)
            Class.forName("org.projectnessie.services.authz.AccessCheckerExtension")
                .getConstructor()
                .newInstance();
      } catch (InstantiationException
          | IllegalAccessException
          | InvocationTargetException
          | ClassNotFoundException
          | NoSuchMethodException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public URI getUri() {
    return jerseyTest.target().getUri();
  }

  private static void withClass(String className, Consumer<Class<?>> whenClassExists) {
    try {
      Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
      whenClassExists.accept(clazz);
    } catch (ClassNotFoundException e) {
      // ignore
    }
  }

  private BatchAccessChecker createNewChecker(AccessContext context) {
    return AbstractBatchAccessChecker.NOOP_ACCESS_CHECKER;
  }

  @Override
  public void close() throws Exception {
    jerseyTest.tearDown();
    weld.shutdown();
  }
}
