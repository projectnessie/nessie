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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.junit.jupiter.api.Test;

public class TestDependencyResolver {
  @Test
  void testDependencyResolver() throws Exception {
    Artifact artifact =
        new DefaultArtifact("org.projectnessie", "nessie-jaxrs-tests", "jar", "0.17.0");

    List<Artifact> artifacts = DependencyResolver.resolve(artifact);
    assertThat(artifacts)
        .map(a -> a.getGroupId() + ":" + a.getArtifactId() + ":" + artifact.getVersion())
        .contains(
            "org.projectnessie:nessie-jaxrs-tests:0.17.0",
            "org.projectnessie:nessie-client:0.17.0");
  }
}
