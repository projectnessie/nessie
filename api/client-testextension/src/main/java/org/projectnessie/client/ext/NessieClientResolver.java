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
package org.projectnessie.client.ext;

import static org.projectnessie.client.NessieClientBuilder.createClientBuilderFromSystemSettings;
import static org.projectnessie.client.ext.MultiVersionApiTest.apiVersion;

import java.io.Serializable;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.commons.util.AnnotationUtils;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.config.NessieClientConfigSource;
import org.projectnessie.client.config.NessieClientConfigSources;
import org.projectnessie.client.http.HttpResponseFactory;
import org.projectnessie.client.http.NessieHttpClientBuilder;

/**
 * A base class for extensions that manage a Nessie test execution environment. This class injects
 * Nessie java client instances and/or URIs based on the specific environment details provided by
 * concrete subclasses.
 *
 * @see NessieClientUri
 * @see NessieClientFactory
 */
public abstract class NessieClientResolver implements ParameterResolver {

  protected abstract URI getBaseUri(ExtensionContext extensionContext);

  private URI resolvedNessieUri(ExtensionContext extensionContext) {
    NessieClientUriResolver resolver =
        extensionContext
            .getTestInstance()
            .filter(t -> t instanceof NessieClientUriResolver)
            .map(NessieClientUriResolver.class::cast)
            .orElse(NessieApiVersion::resolve);

    URI base = getBaseUri(extensionContext);
    NessieApiVersion apiVersion = apiVersion(extensionContext);
    return resolver.resolve(apiVersion, base);
  }

  private boolean isNessieUri(ParameterContext parameterContext) {
    return parameterContext.isAnnotated(NessieClientUri.class);
  }

  private boolean isNessieClient(ParameterContext parameterContext) {
    return parameterContext.getParameter().getType().isAssignableFrom(NessieClientFactory.class);
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return isNessieUri(parameterContext) || isNessieClient(parameterContext);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (isNessieUri(parameterContext)) {
      return resolvedNessieUri(extensionContext);
    }

    if (isNessieClient(parameterContext)) {
      return clientFactoryForContext(extensionContext);
    }

    throw new IllegalStateException("Unsupported parameter: " + parameterContext);
  }

  private NessieClientFactory clientFactoryForContext(ExtensionContext extensionContext) {
    NessieClientConfigSource mainConfigSource =
        extensionContext
            .getTestInstance()
            .filter(t -> t instanceof NessieClientNameResolver)
            .map(NessieClientNameResolver.class::cast)
            .map(NessieClientNameResolver::mainNessieClientConfigMap)
            .map(NessieClientConfigSources::mapConfigSource)
            .orElseGet(NessieClientConfigSources::emptyConfigSource);

    NessieApiVersion apiVersion = apiVersion(extensionContext);
    URI uri = resolvedNessieUri(extensionContext);
    List<NessieClientCustomizer> customizers =
        extensionContext
            .getTestInstances()
            .map(
                i ->
                    i.getAllInstances().stream()
                        .filter(ti -> ti instanceof NessieClientCustomizer)
                        .map(ti -> (NessieClientCustomizer) ti)
                        .collect(Collectors.toList()))
            .orElse(Collections.emptyList());

    Class<? extends HttpResponseFactory> responseFactoryClass =
        extensionContext
            .getTestClass()
            .flatMap(cl -> AnnotationUtils.findAnnotation(cl, NessieClientResponseFactory.class))
            .map(NessieClientResponseFactory::value)
            .orElse(null);

    if (!customizers.isEmpty()) {
      NessieClientCustomizer testCustomizer =
          (builder, version) -> {
            for (NessieClientCustomizer customizer : customizers) {
              builder = customizer.configure(builder, version);
            }
            return builder;
          };

      return new ClientFactory(uri, mainConfigSource, apiVersion, responseFactoryClass) {
        @Nonnull
        @jakarta.annotation.Nonnull
        @Override // Note: this object is not serializable
        public NessieApiV1 make(NessieClientCustomizer customizer) {
          return super.make(
              (builder, version) ->
                  customizer.configure(testCustomizer.configure(builder, version), version));
        }
      };
    }

    // We use a serializable impl. here as a workaround for @QuarkusTest instances, whose parameters
    // are deep-cloned by the Quarkus test extension.
    return new ClientFactory(uri, mainConfigSource, apiVersion, responseFactoryClass);
  }

  private static class ClientFactory implements NessieClientFactory, Serializable {

    private final URI resolvedUri;
    private final NessieClientConfigSource mainConfigSource;
    private final NessieApiVersion apiVersion;
    private final Class<? extends HttpResponseFactory> responseFactoryClass;

    private ClientFactory(
        URI nessieUri,
        NessieClientConfigSource mainConfigSource,
        NessieApiVersion apiVersion,
        Class<? extends HttpResponseFactory> responseFactoryClass) {
      this.resolvedUri = nessieUri;
      this.mainConfigSource = mainConfigSource;
      this.apiVersion = apiVersion;
      this.responseFactoryClass = responseFactoryClass;
    }

    @Override
    public NessieApiVersion apiVersion() {
      return apiVersion;
    }

    @Nonnull
    @jakarta.annotation.Nonnull
    @Override
    public NessieApiV1 make(NessieClientCustomizer customizer) {
      NessieClientBuilder clientBuilder =
          createClientBuilderFromSystemSettings(mainConfigSource).withUri(resolvedUri);
      if (responseFactoryClass != null) {
        try {
          clientBuilder =
              clientBuilder
                  .asInstanceOf(NessieHttpClientBuilder.class)
                  .withResponseFactory(responseFactoryClass.getDeclaredConstructor().newInstance());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      NessieClientBuilder builder = customizer.configure(clientBuilder, apiVersion);
      return apiVersion.build(builder);
    }
  }
}
