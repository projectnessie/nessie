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

import java.io.Serializable;
import java.net.URI;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;

/**
 * A base class for extensions that manage a Nessie test execution environment. This class injects
 * Nessie java client instances and/or URIs based on the specific environment details provided by
 * concrete subclasses.
 *
 * @see NessieUri
 * @see NessieApiProvider
 */
public abstract class NessieClientResolver implements ParameterResolver {

  protected abstract URI findBaseUri(ExtensionContext extensionContext);

  private boolean isNessieUri(ParameterContext parameterContext) {
    return parameterContext.isAnnotated(NessieUri.class);
  }

  private boolean isNessieClient(ParameterContext parameterContext) {
    return parameterContext.getParameter().getType().isAssignableFrom(NessieApiProvider.class);
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
      return findBaseUri(extensionContext);
    }

    if (isNessieClient(parameterContext)) {
      return makeClient(extensionContext);
    }

    throw new IllegalStateException("Unsupported parameter: " + parameterContext);
  }

  private NessieApiProvider makeClient(ExtensionContext extensionContext) {
    URI uri = findBaseUri(extensionContext);
    Object testInstance = extensionContext.getTestInstance().orElse(null);
    if (testInstance instanceof NessieClientCustomizer) {
      NessieClientCustomizer testCustomizer = (NessieClientCustomizer) testInstance;
      return new ApiProvider(uri) {
        @Override // Note: this object is not serializable
        public NessieApiV1 get(NessieClientCustomizer customizer) {
          return super.get(builder -> customizer.configure(testCustomizer.configure(builder)));
        }
      };
    }

    // We use a serializable impl. here as a workaround for @QuarkusTest instances, whose parameters
    // are deep-cloned by the Quarkus test extension.
    return new ApiProvider(uri);
  }

  private static class ApiProvider implements NessieApiProvider, Serializable {
    private final URI uri;

    private ApiProvider(URI uri) {
      this.uri = uri;
    }

    @Override
    public NessieApiV1 get(NessieClientCustomizer customizer) {
      return customizer
          .configure(HttpClientBuilder.builder().withUri(uri))
          .build(NessieApiV1.class);
    }
  }
}
