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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestClassesGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestClassesGenerator.class);

  public static Collection<Class<?>> resolveTestClasses(
      String nessieVersion, String versionInClass, String classNameSuffix) {
    Artifact rawArtifact =
        new DefaultArtifact("org.projectnessie", "nessie-jaxrs-tests", "jar", nessieVersion);
    try {
      List<Artifact> artifacts = DependencyResolver.resolve(rawArtifact);
      URL[] classpathUrls =
          artifacts.stream()
              .map(Artifact::getFile)
              .map(File::toURI)
              .map(
                  u -> {
                    try {
                      return u.toURL();
                    } catch (MalformedURLException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .toArray(URL[]::new);

      return ClassBuilder.createTestClasses(
          nessieVersion, versionInClass, classNameSuffix, classpathUrls);
    } catch (Exception e) {
      LOGGER.error("Failed to resolve dependencies of '{}'", rawArtifact, e);
      throw new RuntimeException(
          String.format("Failed to resolve dependencies of '%s'", rawArtifact), e);
    }
  }
}
