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

import static org.jboss.weld.environment.se.Weld.SHUTDOWN_HOOK_SYSTEM_PROPERTY;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import jakarta.enterprise.inject.spi.Extension;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.SecurityContext;
import java.net.URI;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.glassfish.jersey.message.DeflateEncoder;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.filter.EncodingFilter;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.jboss.weld.environment.se.Weld;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.projectnessie.client.ext.NessieClientResolver;
import org.projectnessie.services.authz.AbstractBatchAccessChecker;
import org.projectnessie.services.authz.AccessContext;
import org.projectnessie.services.authz.BatchAccessChecker;
import org.projectnessie.services.impl.ConfigApiImpl;
import org.projectnessie.services.impl.TreeApiImpl;
import org.projectnessie.services.rest.AccessCheckExceptionMapper;
import org.projectnessie.services.rest.BackendLimitExceededExceptionMapper;
import org.projectnessie.services.rest.RestConfigResource;
import org.projectnessie.services.rest.RestContentResource;
import org.projectnessie.services.rest.RestDiffResource;
import org.projectnessie.services.rest.RestNamespaceResource;
import org.projectnessie.services.rest.RestTreeResource;
import org.projectnessie.services.rest.RestV2ConfigResource;
import org.projectnessie.services.rest.RestV2TreeResource;
import org.projectnessie.services.rest.converters.ContentKeyParamConverterProvider;
import org.projectnessie.services.rest.converters.NamespaceParamConverterProvider;
import org.projectnessie.services.rest.converters.ReferenceTypeParamConverterProvider;
import org.projectnessie.services.rest.exceptions.ConstraintViolationExceptionMapper;
import org.projectnessie.services.rest.exceptions.NessieExceptionMapper;
import org.projectnessie.services.rest.exceptions.NessieJaxRsJsonMappingExceptionMapper;
import org.projectnessie.services.rest.exceptions.NessieJaxRsJsonParseExceptionMapper;
import org.projectnessie.services.rest.exceptions.NotSupportedExceptionMapper;
import org.projectnessie.services.rest.exceptions.ValidationExceptionMapper;
import org.projectnessie.versioned.storage.common.logic.RepositoryLogic;
import org.projectnessie.versioned.storage.common.persist.Persist;

/** A JUnit 5 extension that starts up Weld/JerseyTest. */
public class NessieJaxRsExtension extends NessieClientResolver
    implements BeforeAllCallback,
        BeforeEachCallback,
        AfterEachCallback,
        AfterTestExecutionCallback,
        ParameterResolver {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(NessieJaxRsExtension.class);

  private final Supplier<Persist> persistSupplier;

  public NessieJaxRsExtension() {
    throw new UnsupportedOperationException();
  }

  public NessieJaxRsExtension(Supplier<Persist> persistSupplier) {
    this.persistSupplier = persistSupplier;
  }

  public static NessieJaxRsExtension jaxRsExtension(Supplier<Persist> persistSupplier) {
    return new NessieJaxRsExtension(persistSupplier);
  }

  private EnvHolder getEnv(ExtensionContext extensionContext) {
    EnvHolder env = extensionContext.getStore(NAMESPACE).get(EnvHolder.class, EnvHolder.class);
    if (env == null) {
      throw new ParameterResolutionException(
          "Nessie JaxRs env. is not initialized in " + extensionContext.getUniqueId());
    }
    return env;
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
                Extension versionStoreExtension =
                    VersionStoreImplExtension.forPersist(
                        () -> {
                          Persist persist = persistSupplier.get();
                          persist.erase();
                          RepositoryLogic repositoryLogic = repositoryLogic(persist);
                          repositoryLogic.initialize("main");
                          return persist;
                        });

                return new EnvHolder(versionStoreExtension);
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
  }

  @Override
  public void afterEach(ExtensionContext extensionContext) {
    getEnv(extensionContext).reset();
  }

  @Override
  public void afterTestExecution(ExtensionContext extensionContext) {
    getEnv(extensionContext).reset();
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    getEnv(extensionContext).reset();
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return super.supportsParameter(parameterContext, extensionContext)
        || parameterContext.isAnnotated(NessieSecurityContext.class)
        || parameterContext.isAnnotated(NessieAccessChecker.class);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (super.supportsParameter(parameterContext, extensionContext)) {
      return super.resolveParameter(parameterContext, extensionContext);
    }

    EnvHolder env = getEnv(extensionContext);

    if (parameterContext.isAnnotated(NessieSecurityContext.class)) {
      return (Consumer<SecurityContext>) env::setSecurityContext;
    }

    if (parameterContext.isAnnotated(NessieAccessChecker.class)) {
      return (Consumer<Function<AccessContext, BatchAccessChecker>>) env::setAccessChecker;
    }

    throw new ParameterResolutionException(
        "Unsupported annotation on parameter "
            + parameterContext.getParameter()
            + " on "
            + parameterContext.getTarget());
  }

  @Override
  protected URI getBaseUri(ExtensionContext extensionContext) {
    return getEnv(extensionContext).jerseyTest.target().getUri();
  }

  private static class EnvHolder implements AutoCloseable {
    private final Weld weld;
    private final JerseyTest jerseyTest;
    private SecurityContext securityContext;
    private Function<AccessContext, BatchAccessChecker> accessChecker;

    void reset() {
      this.securityContext = null;
      this.accessChecker = null;
    }

    void setSecurityContext(SecurityContext securityContext) {
      this.securityContext = securityContext;
    }

    void setAccessChecker(Function<AccessContext, BatchAccessChecker> accessChecker) {
      this.accessChecker = accessChecker;
    }

    @SuppressWarnings("resource")
    public EnvHolder(Extension versionStoreExtension) throws Exception {
      weld = new Weld();
      // Let Weld scan all the resources to discover injection points and dependencies
      weld.addPackages(true, RestConfigResource.class);
      weld.addPackages(true, TreeApiImpl.class);
      // Inject external beans
      weld.addExtension(new ContextPrincipalExtension(() -> securityContext));
      weld.addExtension(new ServerConfigExtension());
      weld.addExtension(versionStoreExtension);
      weld.addExtension(new AuthorizerExtension().setAccessCheckerSupplier(this::createNewChecker));
      weld.property(SHUTDOWN_HOOK_SYSTEM_PROPERTY, "false");
      weld.initialize();

      jerseyTest =
          new JerseyTest() {
            @Override
            protected Application configure() {
              ResourceConfig config = new ResourceConfig();
              config.register(RestV2ConfigResource.class);
              config.register(RestV2TreeResource.class);
              config.register(RestConfigResource.class);
              config.register(RestTreeResource.class);
              config.register(RestContentResource.class);
              config.register(RestDiffResource.class);
              config.register(RestNamespaceResource.class);
              config.register(ConfigApiImpl.class);
              config.register(ContentKeyParamConverterProvider.class);
              config.register(NamespaceParamConverterProvider.class);
              config.register(ReferenceTypeParamConverterProvider.class);
              config.register(ValidationExceptionMapper.class, 10);
              config.register(AccessCheckExceptionMapper.class, 10);
              config.register(ConstraintViolationExceptionMapper.class, 10);
              config.register(BackendLimitExceededExceptionMapper.class, 10);
              config.register(NotSupportedExceptionMapper.class, 10);
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

    private BatchAccessChecker createNewChecker(AccessContext context) {
      if (accessChecker == null) {
        return AbstractBatchAccessChecker.NOOP_ACCESS_CHECKER;
      }
      return accessChecker.apply(context);
    }

    @Override
    public void close() throws Exception {
      jerseyTest.tearDown();
      weld.shutdown();
    }
  }
}
