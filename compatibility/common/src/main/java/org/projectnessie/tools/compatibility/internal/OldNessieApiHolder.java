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
package org.projectnessie.tools.compatibility.internal;

import static org.projectnessie.tools.compatibility.internal.DependencyResolver.resolveToClassLoader;
import static org.projectnessie.tools.compatibility.internal.Util.extensionStore;

import com.google.common.annotations.VisibleForTesting;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.DependencyCollectionException;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.tools.compatibility.api.Version;

/**
 * Provides a {@link NessieApi} instance that "looks new", but uses old Nessie client
 * implementation, API interfaces and model classes.
 */
final class OldNessieApiHolder extends AbstractNessieApiHolder {
  private final TranslatingVersionNessieApi translatingApiInstance;

  OldNessieApiHolder(ExtensionContext extensionContext, ClientKey clientKey) {
    super(clientKey);
    ClassLoader oldVersionClassLoader = nessieVersionClassLoader(extensionContext);
    this.translatingApiInstance =
        new TranslatingVersionNessieApi(
            createNessieClient(oldVersionClassLoader, clientKey),
            clientKey.getType(),
            oldVersionClassLoader);
  }

  /**
   * Returns an instance of {@link NessieApi} that uses the interfaces and model classes using the
   * current code tree backed by the implementation of the old Nessie version.
   */
  @Override
  public NessieApi getApiInstance() {
    return translatingApiInstance.getNessieApi();
  }

  @VisibleForTesting
  TranslatingVersionNessieApi getTranslatingApiInstance() {
    return translatingApiInstance;
  }

  private ClassLoader nessieVersionClassLoader(ExtensionContext context) {
    return extensionStore(context.getRoot())
        .getOrComputeIfAbsent(classloaderKey(), k -> createClassLoader(), ClassLoader.class);
  }

  private String classloaderKey() {
    return String.format("class-loader-v%s", clientKey.getVersion());
  }

  private ClassLoader createClassLoader() {
    if (clientKey.getVersion() == Version.CURRENT) {
      return Thread.currentThread().getContextClassLoader();
    }

    Artifact nessieClientArtifact =
        new DefaultArtifact(
            "org.projectnessie", "nessie-client", "jar", clientKey.getVersion().toString());
    try {
      return resolveToClassLoader(clientKey.getVersion().toString(), nessieClientArtifact, null);
    } catch (DependencyCollectionException | DependencyResolutionException e) {
      throw new RuntimeException(
          "Failed to resolve dependencies for Nessie client version " + clientKey.getVersion());
    }
  }
}
