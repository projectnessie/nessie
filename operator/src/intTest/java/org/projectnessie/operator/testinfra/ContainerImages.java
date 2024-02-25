/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.operator.testinfra;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Suppliers;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;
import org.testcontainers.utility.DockerImageName;

public enum ContainerImages {
  K3S,
  BIGTABLE,
  DYNAMO,
  MONGO,
  POSTGRES,
  CASSANDRA;

  private final Supplier<DockerImageName> image;

  ContainerImages() {
    this.image = Suppliers.memoize(() -> dockerImage(name().toLowerCase(Locale.ROOT)));
  }

  public DockerImageName image() {
    return image.get();
  }

  private static DockerImageName dockerImage(String name) {
    URL resource =
        ContainerImages.class.getResource(
            "/org/projectnessie/operator/it/docker/Dockerfile-" + name + "-tests-version");
    try (InputStream in = Objects.requireNonNull(resource).openConnection().getInputStream()) {
      String[] imageTag =
          Arrays.stream(new String(in.readAllBytes(), UTF_8).split("\n"))
              .map(String::trim)
              .filter(l -> l.startsWith("FROM "))
              .map(l -> l.substring(5).trim().split(":"))
              .findFirst()
              .orElseThrow();
      String imageName = imageTag[0];
      String version = System.getProperty("it.nessie.container." + name + ".tag", imageTag[1]);
      return DockerImageName.parse(imageName + ':' + version);
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }
}
