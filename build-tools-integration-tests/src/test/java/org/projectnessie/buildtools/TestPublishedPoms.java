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

package org.projectnessie.buildtools;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.DefaultRepositorySystemSession;
import org.eclipse.aether.RepositorySystem;
import org.eclipse.aether.RepositorySystemSession;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.connector.basic.BasicRepositoryConnectorFactory;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.impl.DefaultServiceLocator;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.RemoteRepository;
import org.eclipse.aether.resolution.DependencyRequest;
import org.eclipse.aether.resolution.DependencyResult;
import org.eclipse.aether.spi.connector.RepositoryConnectorFactory;
import org.eclipse.aether.spi.connector.transport.TransporterFactory;
import org.eclipse.aether.transport.file.FileTransporterFactory;
import org.eclipse.aether.transport.http.HttpTransporterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Resolves all published Nessie jar artifacts and lets Maven fail on any issue in a pom. */
public class TestPublishedPoms {

  static RepositorySystem repositorySystem;
  static RepositorySystemSession repositorySystemSession;
  static List<RemoteRepository> repositories;
  static String nessieVersion;

  @BeforeAll
  static void setup() {
    nessieVersion = System.getProperty("nessie.version");
    assertThat(nessieVersion).as("System property nessie.version").isNotNull();
    repositorySystem = buildRepositorySystem();
    repositorySystemSession = newSession(repositorySystem);
    repositories = new ArrayList<>();
    repositories.add(new RemoteRepository.Builder(
      "maven-central", "default", "https://repo1.maven.org/maven2/")
      .build());
    repositories.add(new RemoteRepository.Builder(
      "nessie-maven", "default", "https://storage.googleapis.com/nessie-maven")
      .build());
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "nessie-client",
    "nessie-compatibility-common",
    "nessie-compatibility-jersey",
    "nessie-compatibility-tests",
    "nessie-content-generator",
    "nessie-jaxrs",
    "nessie-jaxrs-testextension",
    "nessie-jaxrs-tests",
    "nessie-lambda",
    "nessie-model",
    "nessie-quarkus",
    "nessie-quarkus-cli",
    "nessie-quarkus-common",
    "nessie-quarkus-tests",
    "nessie-rest-services",
    "nessie-server-store",
    "nessie-services",
    "nessie-ui",
    "nessie-versioned-spi",
    "nessie-versioned-persist-adapter",
    "nessie-versioned-persist-in-memory",
    "nessie-versioned-persist-dynamodb",
    "nessie-versioned-persist-mongodb",
    "nessie-versioned-persist-rocks",
    "nessie-versioned-persist-non-transactional",
    "nessie-versioned-persist-transactional",
    "nessie-versioned-persist-store",
    "nessie-versioned-persist-serialize",
    "nessie-versioned-persist-tests",
    "nessie-versioned-persist-bench",
    //
    "iceberg-views",
    "nessie-deltalake",
    "nessie-spark-antlr-runtime",
    "nessie-spark-extensions-grammar",
    "nessie-spark-extensions-3.1_2.12",
    "nessie-spark-extensions-3.2_2.12",
    "nessie-spark-extensions-3.2_2.13",
    "nessie-spark-extensions-3.3_2.12",
    "nessie-spark-extensions-3.3_2.13"
  })
  void checkPom(String artifactId) throws Exception {
    // Note: 'collectDependencies' takes a couple 100ms :(
    CollectRequest collectRequest = new CollectRequest().setRepositories(repositories);
    collectRequest.setRoot(
      new Dependency(new DefaultArtifact("org.projectnessie", artifactId, "jar", nessieVersion),
        "runtime"));

    DependencyRequest dependencyRequest = new DependencyRequest(collectRequest, null);
    DependencyResult dependencyResult =
      repositorySystem.resolveDependencies(repositorySystemSession, dependencyRequest);

    List<Exception> exceptions = dependencyResult.getCollectExceptions();
    if (!exceptions.isEmpty()) {
      RuntimeException e = new RuntimeException("Failed to resolve " + collectRequest);
      exceptions.forEach(e::addSuppressed);
      throw e;
    }
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

    // Fail on every error. Default is to ignore issues in poms and continue with wrong or
    // completely unresolved poms.
    session.setArtifactDescriptorPolicy(null);

    String localRepository = System.getProperty("localRepository");
    if (localRepository == null) {
      localRepository = String.format("%s/.m2/repository", System.getProperty("user.home"));
    }

    LocalRepository localRepo = new LocalRepository(localRepository);
    session.setLocalRepositoryManager(system.newLocalRepositoryManager(session, localRepo));

    return session;
  }
}
