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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.resolution.DependencyResult;
import org.eclipse.aether.supplier.RepositorySystemSupplier;
import org.eclipse.aether.util.repository.SimpleArtifactDescriptorPolicy;
import org.eclipse.aether.util.repository.SimpleResolutionErrorPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves dependencies via Aether from the local repository and Maven Central.
 *
 * <p>The path to the local repository is taken from the system property {@code localRepository},
 * defaults to {@code ~/.m2/repository}.
 */
final class DependencyResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(DependencyResolver.class);

  private DependencyResolver() {}

  static ClassLoader toClassLoader(
      String info, Stream<Artifact> artifacts, ClassLoader parentClassLoader) {
    if (parentClassLoader == null) {
      parentClassLoader = ClassLoader.getSystemClassLoader().getParent();
    }
    return asIndependentClassLoader(info, toUrls(artifacts), parentClassLoader);
  }

  static Stream<URL> toUrls(Stream<Artifact> artifacts) {
    return artifacts
        .map(Artifact::getFile)
        .map(File::toURI)
        .map(
            u -> {
              try {
                return u.toURL();
              } catch (MalformedURLException e) {
                throw new RuntimeException(e);
              }
            });
  }

  static ClassLoader asIndependentClassLoader(
      String info, Stream<URL> classpath, ClassLoader parentClassLoader) {
    return new VersionClassLoader(info, classpath.toArray(URL[]::new), parentClassLoader);
  }

  static final class VersionClassLoader extends URLClassLoader {

    private final String info;

    VersionClassLoader(String info, URL[] urls, ClassLoader parent) {
      super(urls, parent);
      this.info = info;
    }

    @Override
    public String toString() {
      return String.format("VersionClassLoader for %s (%s)", info, super.toString());
    }
  }

  public static Stream<Artifact> resolve(Consumer<CollectRequest> collect)
      throws DependencyResolutionException {

    RepositorySystem repositorySystem = new RepositorySystemSupplier().get();
    RepositorySystemSession repositorySystemSession = newSession(repositorySystem);

    List<RemoteRepository> repositories =
        Collections.singletonList(
            new RemoteRepository.Builder(
                    "maven-central", "default", "https://repo1.maven.org/maven2/")
                .build());

    // Note: 'collectDependencies' takes a couple 100ms :(
    CollectRequest collectRequest = new CollectRequest().setRepositories(repositories);
    collect.accept(collectRequest);
    collectRequest.setRequestContext("project");

    DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, null);
    DependencyResult dependencyResult =
        repositorySystem.resolveDependencies(repositorySystemSession, dependencyRequest);

    List<Exception> exceptions = dependencyResult.getCollectExceptions();
    if (!exceptions.isEmpty()) {
      RuntimeException e = new RuntimeException("Failed to resolve " + collectRequest);
      exceptions.forEach(e::addSuppressed);
      throw e;
    }

    LOGGER.debug(
        "Resolved artifact '{}' to {} artifact results",
        collectRequest.getRoot().getArtifact(),
        dependencyResult.getArtifactResults().size());

    return dependencyResult.getArtifactResults().stream().map(ArtifactResult::getArtifact);
  }

  private static RepositorySystemSession newSession(RepositorySystem system) {
    DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();

    String localRepository = System.getProperty("localRepository");
    if (localRepository == null) {
      localRepository = String.format("%s/.m2/repository", System.getProperty("user.home"));
    }

    LocalRepository localRepo = new LocalRepository(localRepository);
    session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

    // Let Maven "bark" for all kinds of dependency issues - otherwise dependency errors are
    // silently dropped on the floor resulting in wrong dependency resolutions.
    session.setArtifactDescriptorPolicy(new SimpleArtifactDescriptorPolicy(0));
    session.setResolutionErrorPolicy(new SimpleResolutionErrorPolicy(0));

    // Add properties that Maven profile activation or pom's need.
    // java.version --> Maven profile activation based on Java version
    // java.version --> pom's resolving a tools.jar :facepalm:
    System.getProperties()
        .forEach(
            (k, v) -> {
              String key = k.toString();
              if (key.startsWith("java.")
                  || key.startsWith("file.")
                  || key.startsWith("os.")
                  || key.endsWith(".separator")
                  || key.endsWith(".encoding")
                  || key.startsWith("sun.")
                  || key.startsWith("user.")) {
                session.setSystemProperty(key, v.toString());
              }
            });

    return session;
  }
}
