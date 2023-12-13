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

import static org.jboss.weld.environment.se.Weld.SHUTDOWN_HOOK_SYSTEM_PROPERTY;

import jakarta.enterprise.inject.spi.Extension;
import jakarta.ws.rs.core.Application;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
import org.projectnessie.services.rest.RestConfigResource;
import org.projectnessie.services.rest.RestContentResource;
import org.projectnessie.services.rest.RestTreeResource;
import org.projectnessie.versioned.storage.common.persist.Persist;

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

  public JerseyServer(Supplier<Persist> persistSupplier) throws Exception {
    weld = new Weld();
    // Let Weld scan all the resources to discover injection points and dependencies
    weld.addPackages(true, RestConfigResource.class);
    weld.addPackages(true, TreeApiImpl.class);
    // Inject external beans
    weld.addExtension(new PrincipalExtension());
    weld.addExtension(new ServerConfigExtension());
    weld.addExtension(VersionStoreImplExtension.forPersist(persistSupplier));

    weld.addExtension(authzExtension());
    weld.property(SHUTDOWN_HOOK_SYSTEM_PROPERTY, "false");
    weld.initialize();

    jerseyTest =
        new JerseyTest() {
          @Override
          protected Application configure() {
            ResourceConfig config = new ResourceConfig();
            withClass("org.projectnessie.services.rest.RestV2ConfigResource", config::register);
            withClass("org.projectnessie.services.rest.RestV2TreeResource", config::register);
            config.register(RestConfigResource.class);
            config.register(RestTreeResource.class);
            config.register(RestContentResource.class);
            withClass("org.projectnessie.services.rest.RestDiffResource", config::register);
            withClass("org.projectnessie.services.rest.RestRefLogResource", config::register);
            withClass("org.projectnessie.services.rest.RestNamespaceResource", config::register);
            config.register(ConfigApiImpl.class);
            withEEClass(
                "org.projectnessie.services.restjavax.ContentKeyParamConverterProvider",
                config::register,
                true);
            withEEClass(
                // Present since 0.19.0
                "org.projectnessie.services.restjavax.ReferenceTypeParamConverterProvider",
                config::register,
                false);
            withEEClass(
                "org.projectnessie.services.restjavax.ValidationExceptionMapper",
                c -> config.register(c, 10),
                true);
            withEEClass(
                // Present since 0.46.5
                "org.projectnessie.services.restjavax.ConstraintViolationExceptionMapper",
                c -> config.register(c, 10),
                false);
            withEEClass(
                // Present since 0.23.1
                "org.projectnessie.services.restjavax.NamespaceParamConverterProvider",
                config::register,
                false);
            withEEClass(
                "org.projectnessie.services.restjavax.NessieExceptionMapper",
                config::register,
                true);
            withEEClass(
                "org.projectnessie.services.restjakarta.AccessCheckExceptionMapper",
                c -> config.register(c, 10),
                false);
            withEEClass(
                "org.projectnessie.services.restjakarta.BackendLimitExceededExceptionMapper",
                c -> config.register(c, 10),
                false);
            withEEClass(
                "org.projectnessie.services.rest.exceptions.NotSupportedExceptionMapper",
                c -> config.register(c, 10),
                false);
            withEEClass(
                "org.projectnessie.services.restjavax.NessieJaxRsJsonParseExceptionMapper",
                c -> config.register(c, 10),
                true);
            withEEClass(
                "org.projectnessie.services.restjavax.NessieJaxRsJsonMappingExceptionMapper",
                c -> config.register(c, 10),
                true);

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
        return Class.forName("org.projectnessie.services.authz.AccessCheckerExtension")
            .asSubclass(Extension.class)
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

  private static void withEEClass(
      String eeClassName, Consumer<Class<?>> whenClassExists, boolean mandatory) {
    Iterator<String> classNames =
        List.of(
                eeClassName.replace("restjavax.", "rest.converters."),
                eeClassName.replace("restjavax.", "rest.exceptions."),
                eeClassName.replace("restjavax.", "rest."),
                eeClassName)
            .iterator();
    while (classNames.hasNext()) {
      String className = classNames.next();
      try {
        Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
        whenClassExists.accept(clazz);
        break;
      } catch (NoClassDefFoundError | ClassNotFoundException e) {
        if (!classNames.hasNext()) {
          if (mandatory) {
            throw new RuntimeException(e);
          }
          return;
        }
      }
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
