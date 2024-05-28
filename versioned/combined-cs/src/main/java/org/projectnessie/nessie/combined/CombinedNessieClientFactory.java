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
package org.projectnessie.nessie.combined;

import static org.projectnessie.client.NessieClientBuilder.createClientBuilder;

import jakarta.annotation.Nonnull;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.platform.engine.UniqueId;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieClientCustomizer;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.Logics;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendConfig;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendFactory;

/**
 * JUnit-extension providing a {@link org.projectnessie.client.api.NessieApiV2} instance that
 * directly accesses an in-memory {@link Persist} instance.
 */
public class CombinedNessieClientFactory
    implements Extension, ParameterResolver, BeforeEachCallback {
  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(CombinedNessieClientFactory.class);

  public static final String API_VERSION_SEGMENT_TYPE = "nessie-api";
  private static final NessieApiVersion DEFAULT_API_VERSION = NessieApiVersion.V2;

  private boolean isNessieClient(ParameterContext parameterContext) {
    return parameterContext.getParameter().getType().isAssignableFrom(NessieClientFactory.class);
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return isNessieClient(parameterContext);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (isNessieClient(parameterContext)) {
      return clientFactoryForContext(extensionContext);
    }

    throw new IllegalStateException("Unsupported parameter: " + parameterContext);
  }

  private NessieClientFactory clientFactoryForContext(ExtensionContext extensionContext) {
    NessieApiVersion apiVersion = apiVersion(extensionContext);
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

    Persist persist = persist(extensionContext);

    if (!customizers.isEmpty()) {
      NessieClientCustomizer testCustomizer =
          (builder, version) -> {
            for (NessieClientCustomizer customizer : customizers) {
              builder = customizer.configure(builder, version);
            }
            return builder;
          };

      return new ClientFactory(apiVersion, persist) {
        @Override // Note: this object is not serializable
        @Nonnull
        public NessieApiV1 make(NessieClientCustomizer customizer) {
          return super.make(
              (builder, version) ->
                  customizer.configure(testCustomizer.configure(builder, version), version));
        }
      };
    }

    // We use a serializable impl. here as a workaround for @QuarkusTest instances, whose parameters
    // are deep-cloned by the Quarkus test extension.
    return new ClientFactory(apiVersion, persist);
  }

  private Persist persist(ExtensionContext extensionContext) {
    return extensionContext
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            "persistSupplier",
            x -> {
              PersistFactory persistFactory = backend(extensionContext).createFactory();
              return persistFactory.newPersist(StoreConfig.Adjustable.empty());
            },
            Persist.class);
  }

  private Backend backend(ExtensionContext extensionContext) {
    return extensionContext
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            "backendSupplier",
            x -> {
              BackendFactory<InmemoryBackendConfig> backendFactory = new InmemoryBackendFactory();
              return backendFactory.buildBackend(backendFactory.newConfigInstance());
            },
            Backend.class);
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) {
    Logics.repositoryLogic(persist(extensionContext)).initialize("main");
  }

  static NessieApiVersion apiVersion(ExtensionContext context) {
    return UniqueId.parse(context.getUniqueId()).getSegments().stream()
        .filter(s -> API_VERSION_SEGMENT_TYPE.equals(s.getType()))
        .map(UniqueId.Segment::getValue)
        .findFirst()
        .map(NessieApiVersion::valueOf)
        .orElse(DEFAULT_API_VERSION);
  }

  private static class ClientFactory implements NessieClientFactory, Serializable {
    private final NessieApiVersion apiVersion;
    private final Persist persist;

    private ClientFactory(NessieApiVersion apiVersion, Persist persist) {
      this.apiVersion = apiVersion;
      this.persist = persist;
    }

    @Override
    public NessieApiVersion apiVersion() {
      return apiVersion;
    }

    @Override
    @Nonnull
    public NessieApiV1 make(NessieClientCustomizer customizer) {
      NessieClientBuilder clientBuilder =
          createClientBuilder("Combined", null)
              .asInstanceOf(CombinedClientBuilder.class)
              .withPersist(persist);

      NessieClientBuilder builder = customizer.configure(clientBuilder, apiVersion);
      return apiVersion.build(builder);
    }
  }
}
