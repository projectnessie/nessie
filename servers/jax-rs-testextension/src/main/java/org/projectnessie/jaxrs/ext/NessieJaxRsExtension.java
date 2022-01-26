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

import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.SecurityContext;
import org.glassfish.jersey.message.DeflateEncoder;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;
import org.glassfish.jersey.test.JerseyTest;
import org.jboss.weld.environment.se.Weld;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.projectnessie.services.authz.AccessCheckerExtension;
import org.projectnessie.services.config.ServerConfigExtension;
import org.projectnessie.services.impl.ConfigApiImpl;
import org.projectnessie.services.impl.TreeApiImpl;
import org.projectnessie.services.rest.ContentKeyParamConverterProvider;
import org.projectnessie.services.rest.InstantParamConverterProvider;
import org.projectnessie.services.rest.NessieExceptionMapper;
import org.projectnessie.services.rest.NessieJaxRsJsonMappingExceptionMapper;
import org.projectnessie.services.rest.NessieJaxRsJsonParseExceptionMapper;
import org.projectnessie.services.rest.ReferenceTypeParamConverterProvider;
import org.projectnessie.services.rest.RestConfigResource;
import org.projectnessie.services.rest.RestContentResource;
import org.projectnessie.services.rest.RestDiffResource;
import org.projectnessie.services.rest.RestRefLogResource;
import org.projectnessie.services.rest.RestTreeResource;
import org.projectnessie.services.rest.ValidationExceptionMapper;
import org.projectnessie.versioned.PersistVersionStoreExtension;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;

/** A JUnit 5 extension that starts up Weld/JerseyTest. */
public class NessieJaxRsExtension
    implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(NessieJaxRsExtension.class);

  private final Supplier<DatabaseAdapter> databaseAdapterSupplier;

  public NessieJaxRsExtension() {
    throw new UnsupportedOperationException();
  }

  public NessieJaxRsExtension(Supplier<DatabaseAdapter> databaseAdapterSupplier) {
    this.databaseAdapterSupplier = databaseAdapterSupplier;
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) {
    // Put EnvHolder into the top-most context handled by this exception. Nested contexts will reuse
    // the same value to minimize Jersey restarts. EnvHolder will initialize on first use and close
    // when its owner context is destroyed.
    // Note: we also use EnvHolder.class as a key to the map of stored values.
    extensionContext
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            EnvHolder.class,
            key -> {
              try {
                return new EnvHolder(databaseAdapterSupplier);
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) throws Exception {
    EnvHolder env = extensionContext.getStore(NAMESPACE).get(EnvHolder.class, EnvHolder.class);
    env.resetSecurityContext();
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    EnvHolder env = extensionContext.getStore(NAMESPACE).get(EnvHolder.class, EnvHolder.class);
    env.resetSecurityContext();
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterContext.isAnnotated(NessieUri.class)
        || parameterContext.isAnnotated(NessieSecurityContext.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    EnvHolder env = extensionContext.getStore(NAMESPACE).get(EnvHolder.class, EnvHolder.class);
    if (env == null) {
      throw new ParameterResolutionException(
          "Nessie JaxRs env. is not initialized in " + extensionContext.getUniqueId());
    }

    if (parameterContext.isAnnotated(NessieUri.class)) {
      return env.jerseyTest.target().getUri();
    }

    if (parameterContext.isAnnotated(NessieSecurityContext.class)) {
      return (Consumer<SecurityContext>) env::setSecurityContext;
    }

    throw new ParameterResolutionException(
        "Unsupported annotation on parameter "
            + parameterContext.getParameter()
            + " on "
            + parameterContext.getTarget());
  }

  private static class EnvHolder implements CloseableResource {
    private final Weld weld;
    private final JerseyTest jerseyTest;
    private SecurityContext securityContext;

    void resetSecurityContext() {
      this.securityContext = null;
    }

    void setSecurityContext(SecurityContext securityContext) {
      this.securityContext = securityContext;
    }

    public EnvHolder(Supplier<DatabaseAdapter> databaseAdapterSupplier) throws Exception {
      weld = new Weld();
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
              config.register(RestConfigResource.class);
              config.register(RestTreeResource.class);
              config.register(RestContentResource.class);
              config.register(RestDiffResource.class);
              config.register(RestRefLogResource.class);
              config.register(ConfigApiImpl.class);
              config.register(ContentKeyParamConverterProvider.class);
              config.register(ReferenceTypeParamConverterProvider.class);
              config.register(InstantParamConverterProvider.class);
              config.register(ValidationExceptionMapper.class, 10);
              config.register(NessieExceptionMapper.class);
              config.register(NessieJaxRsJsonParseExceptionMapper.class, 10);
              config.register(NessieJaxRsJsonMappingExceptionMapper.class, 10);
              config.register(EncodingFilter.class);
              config.register(GZipEncoder.class);
              config.register(DeflateEncoder.class);
              config.register(
                  (ContainerRequestFilter)
                      requestContext -> {
                        if (securityContext != null) {
                          requestContext.setSecurityContext(securityContext);
                        }
                      });
              return config;
            }
          };

      jerseyTest.setUp();
    }

    @Override
    public void close() throws Throwable {
      jerseyTest.tearDown();
      weld.shutdown();
    }
  }
}
