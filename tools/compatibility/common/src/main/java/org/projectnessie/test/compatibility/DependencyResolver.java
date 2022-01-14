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
package org.projectnessie.test.compatibility;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.collection.CollectResult;
import org.eclipse.aether.collection.DependencyCollectionException;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.ArtifactResult;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.eclipse.aether.resolution.DependencyResult;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DependencyResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(DependencyResolver.class);

  private DependencyResolver() {}

  static List<Artifact> resolve(Artifact artifact)
      throws DependencyCollectionException, DependencyResolutionException {
    LOGGER.debug("Resolving artifact '{}'", artifact);

    RepositorySystem repositorySystem = buildRepositorySystem();
    RepositorySystemSession repositorySystemSession = newSession(repositorySystem);

    Dependency rootDependency = new Dependency(artifact, "runtime");

    List<RemoteRepository> repositories =
        Collections.singletonList(
            new RemoteRepository.Builder(
                    "maven-central", "default", "https://repo1.maven.org/maven2/")
                .build());

    CollectRequest collectRequest = new CollectRequest();
    collectRequest.setRoot(rootDependency);
    collectRequest.setRepositories(repositories);
    CollectResult collectResult =
        repositorySystem.collectDependencies(repositorySystemSession, collectRequest);

    DependencyRequest dependencyRequest = new DependencyRequest(collectResult.getRoot(), null);
    DependencyResult dependencyResult =
        repositorySystem.resolveDependencies(repositorySystemSession, dependencyRequest);

    List<Exception> exceptions = dependencyResult.getCollectExceptions();
    if (!exceptions.isEmpty()) {
      RuntimeException e = new RuntimeException("Failed to resolve artifact " + artifact);
      exceptions.forEach(e::addSuppressed);
      throw e;
    }

    LOGGER.debug(
        "Resolved artifact '{}' to {} artifact results",
        artifact,
        dependencyResult.getArtifactResults().size());

    return dependencyResult.getArtifactResults().stream()
        .map(ArtifactResult::getArtifact)
        .collect(Collectors.toList());
  }

  private static RepositorySystem buildRepositorySystem() {
    DefaultServiceLocator locator = MavenRepositorySystemUtils.newServiceLocator();
    locator.addService(RepositoryConnectorFactory.class, BasicRepositoryConnectorFactory.class);
    locator.addService(TransporterFactory.class, FileTransporterFactory.class);
    locator.addService(TransporterFactory.class, HttpTransporterFactory.class);

    return locator.getService(RepositorySystem.class);
  }

  private static RepositorySystemSession newSession(RepositorySystem system) {
    DefaultRepositorySystemSession session = MavenRepositorySystemUtils.newSession();

    String localRepository = System.getProperty("localRepository");
    if (localRepository == null) {
      localRepository = String.format("%s/.m2/repository", System.getProperty("user.home"));
    }

    LocalRepository localRepo = new LocalRepository(localRepository);
    session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

    return session;
  }
}
