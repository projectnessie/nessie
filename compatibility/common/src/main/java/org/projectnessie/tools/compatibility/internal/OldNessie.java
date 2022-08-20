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

import static java.util.Arrays.asList;
import static org.projectnessie.tools.compatibility.internal.DependencyResolver.resolve;
import static org.projectnessie.tools.compatibility.internal.DependencyResolver.toClassLoader;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.CollectRequest;
import org.eclipse.aether.graph.Dependency;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.projectnessie.tools.compatibility.api.Version;

final class OldNessie {
  private OldNessie() {}

  /**
   * Resolve a Nessie artifact, its dependencies and returns a {@link java.lang.ClassLoader} for it.
   * This function handles the special cases for Nessie 0.40.0..0.41.0 which have a few issues in
   * the published poms.
   */
  static ClassLoader oldNessieClassLoader(Version version, String mainArtifactId)
      throws DependencyResolutionException {

    Function<String, Dependency> nessieDep =
        artifactId ->
            new Dependency(
                new DefaultArtifact("org.projectnessie", artifactId, "jar", version.toString()),
                "compile");

    Dependency mainDependency = nessieDep.apply(mainArtifactId);

    Consumer<CollectRequest> collect = r -> r.setRoot(mainDependency);

    if (Version.parseVersion("0.40.0").isLessThanOrEqual(version)
        && Version.parseVersion("0.41.0").isGreaterThanOrEqual(version)) {
      // Need to align the io.opentracing dependencies to the correct version.
      // Nessie versions 0.40.0 up to 0.41.0 used _different_ versions for
      // opentracing-noop (0.30.0) + opentracing-api (0.33.0), which are unfortunately
      // not compatible with each other.
      // Blindly replace remove all dependencies from io.opentracing with the specific version
      // 0.33.0. Using the opentracing-mock artifact for simplicity (transitive dependencies
      // to the required artifacts).
      String opentracingVersion = "0.33.0";

      // Sadly, Nessie versions 0.40.0 up to 0.41.0 also do not have the jackson-bom in
      // nessie-client.pom. Using nessie-client as the root dependency when resolving the
      // dependencies breaks the whole dependency-resolve-process and only the root dependency,
      // which is nessie-client, is returned. To work around this issue, the below code uses
      // nessie-model as the root dependency (it has the jackson-bom) and nessie-client as an
      // additional dependency.

      collect =
          r -> {
            if ("nessie-client".equals(mainArtifactId)) {
              r.setRoot(nessieDep.apply("nessie-model"));
            } else {
              r.setRoot(mainDependency);
            }
            r.addDependency(mainDependency);

            asList("opentracing-api", "opentracing-util", "opentracing-noop")
                .forEach(
                    artifactId ->
                        r.addManagedDependency(
                            new Dependency(
                                new DefaultArtifact(
                                    "io.opentracing", artifactId, "jar", opentracingVersion),
                                "runtime")));
          };
    }

    Stream<Artifact> resolvedArtifacts = resolve(collect);
    return toClassLoader(version.toString(), resolvedArtifacts, null);
  }
}
