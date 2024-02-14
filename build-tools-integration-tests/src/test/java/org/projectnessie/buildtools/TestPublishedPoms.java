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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.eclipse.aether.util.repository.SimpleArtifactDescriptorPolicy;
import org.eclipse.aether.util.repository.SimpleResolutionErrorPolicy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Resolves all published Nessie jar artifacts and lets Maven fail on any issue in a pom.
 */
public class TestPublishedPoms {

  static RepositorySystem repositorySystem;
  static RepositorySystemSession repositorySystemSession;
  static List<RemoteRepository> repositories;
  static String nessieVersion;

  @BeforeAll
  static void setup() {
    nessieVersion = System.getProperty("nessie.version");
    if (nessieVersion == null) {
      try {
        nessieVersion = new String(Files.readAllBytes(Paths.get("../version.txt")),
          StandardCharsets.UTF_8).trim();
      } catch (IOException e) {
        throw new RuntimeException(
          "System property nessie.version is not set and no version.txt file in the parent directory",
          e);
      }
    }
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
    "nessie-model",
    "nessie-quarkus",
    "nessie-quarkus-cli",
    "nessie-quarkus-common",
    "nessie-quarkus-tests",
    "nessie-rest-services",
    "nessie-server-store",
    "nessie-services",
    "nessie-versioned-spi",
    "nessie-versioned-storage-cache",
    "nessie-versioned-storage-common",
    "nessie-versioned-storage-common-proto",
    "nessie-versioned-storage-common-serialize",
    "nessie-versioned-storage-common-tests",
    "nessie-versioned-storage-inmemory",
    "nessie-versioned-storage-mongodb",
    "nessie-versioned-storage-rocksdb",
    "nessie-versioned-storage-store",
    "nessie-versioned-storage-testextension",
  })
  void checkMainPom(String artifactId) throws Exception {
    checkPom("org.projectnessie.nessie", artifactId);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "nessie-spark-antlr-runtime",
    "nessie-spark-extensions-grammar",
    "nessie-spark-extensions-3.2_2.12",
    "nessie-spark-extensions-3.2_2.13",
    "nessie-spark-extensions-3.3_2.12",
    "nessie-spark-extensions-3.3_2.13",
    "nessie-spark-extensions-3.4_2.12",
    "nessie-spark-extensions-3.4_2.13",
    "nessie-spark-extensions-3.5_2.12",
    "nessie-spark-extensions-3.5_2.13"
  })
  void checkIntegrationsPom(String artifactId) throws Exception {
    checkPom("org.projectnessie.nessie-integrations", artifactId);
  }

  void checkPom(String groupId, String artifactId) throws Exception {
    // Note: 'collectDependencies' takes a couple 100ms :(
    CollectRequest collectRequest = new CollectRequest().setRepositories(repositories);
    collectRequest.setRoot(
      new Dependency(new DefaultArtifact(groupId, artifactId, "jar", nessieVersion),
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
          if (key.startsWith("java.")) {
            session.setSystemProperty(key, v.toString());
          }
        });

    return session;
  }
}
