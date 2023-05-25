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

import static org.projectnessie.tools.compatibility.internal.DependencyResolver.resolve;
import static org.projectnessie.tools.compatibility.internal.DependencyResolver.toClassLoader;

import java.util.List;
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
  static ClassLoader oldNessieClassLoader(Version version, List<String> artifactIds)
      throws DependencyResolutionException {

    Function<String, Dependency> nessieDep =
        artifactId ->
            new Dependency(
                new DefaultArtifact(
                    "org.projectnessie.nessie", artifactId, "jar", version.toString()),
                "compile");

    String mainArtifactId = artifactIds.get(0);

    Dependency mainDependency = nessieDep.apply(mainArtifactId);

    Consumer<CollectRequest> collect =
        r -> {
          r.setRoot(mainDependency);
          for (int i = 1; i < artifactIds.size(); i++) {
            r.addDependency(nessieDep.apply(artifactIds.get(i)));
          }
        };

    Stream<Artifact> resolvedArtifacts = resolve(collect);
    return toClassLoader(version.toString(), resolvedArtifacts, null);
  }
}
