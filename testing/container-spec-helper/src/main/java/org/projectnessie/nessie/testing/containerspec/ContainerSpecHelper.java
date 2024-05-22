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
package org.projectnessie.nessie.testing.containerspec;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.immutables.value.Value.Style.ImplementationVisibility.PACKAGE;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Locale;
import java.util.Objects;
import org.immutables.value.Value;
import org.testcontainers.shaded.com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.testcontainers.utility.DockerImageName;

@Value.Immutable
@Value.Style(visibility = PACKAGE)
public abstract class ContainerSpecHelper {
  public static Builder builder() {
    return ImmutableContainerSpecHelper.builder();
  }

  public interface Builder {
    @CanIgnoreReturnValue
    Builder containerClass(Class<?> containerClass);

    @CanIgnoreReturnValue
    Builder name(String name);

    ContainerSpecHelper build();
  }

  public abstract String name();

  public abstract Class<?> containerClass();

  public DockerImageName dockerImageName(String explicitImageName) {
    if (explicitImageName != null) {
      return DockerImageName.parse(explicitImageName);
    }

    String dockerfile = format("Dockerfile-%s-version", name());
    URL resource = containerClass().getResource(dockerfile);
    Objects.requireNonNull(resource, dockerfile + " not found");

    String systemPropPrefix1 = "it.nessie.container." + name() + ".";
    String systemPropPrefix2 = "nessie.testing." + name() + ".";
    String envPrefix = name().toUpperCase(Locale.ROOT).replaceAll("-", "_") + "_DOCKER_";

    String explicitImage = System.getProperty(systemPropPrefix1 + "image");
    if (explicitImage == null) {
      explicitImage = System.getProperty(systemPropPrefix2 + "image");
    }
    if (explicitImage == null) {
      explicitImage = System.getenv(envPrefix + "IMAGE");
    }
    String explicitTag = System.getProperty(systemPropPrefix1 + "tag");
    if (explicitTag == null) {
      explicitTag = System.getProperty(systemPropPrefix2 + "tag");
    }
    if (explicitTag == null) {
      explicitTag = System.getenv(envPrefix + "TAG");
    }

    if (explicitImage != null && explicitTag != null) {
      return DockerImageName.parse(explicitImage + ':' + explicitTag);
    }

    try (InputStream in = resource.openConnection().getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, UTF_8))) {
      String fullImageName = null;
      String ln;
      while ((ln = reader.readLine()) != null) {
        ln = ln.trim();
        if (ln.startsWith("FROM ")) {
          fullImageName = ln.substring(5).trim();
          break;
        }
      }

      if (fullImageName == null) {
        throw new IllegalStateException(
            "Dockerfile " + dockerfile + " does not contain a line starting with 'FROM '");
      }

      if (explicitImage != null || explicitTag != null) {
        throw new IllegalArgumentException(
            "Must specify either BOTH, image name AND tag via system properties or environment  or omit and leave it to the default "
                + fullImageName
                + " from "
                + dockerfile);
      }

      return DockerImageName.parse(fullImageName);
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract tag from " + resource, e);
    }
  }
}
