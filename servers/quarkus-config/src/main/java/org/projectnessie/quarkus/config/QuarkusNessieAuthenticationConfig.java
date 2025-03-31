/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.quarkus.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.util.Optional;
import java.util.Set;

/** Configuration for Nessie authentication settings. */
@ConfigMapping(prefix = "nessie.server.authentication")
public interface QuarkusNessieAuthenticationConfig {

  /** Enable Nessie authentication. */
  @WithName("enabled")
  @WithDefault("false")
  boolean enabled();

  /**
   * Returns the set of HTTP URL paths that are permitted to be serviced without authentication.
   * Anonymous access is granted, if the URI path is equal to any of the elements}.
   *
   * @hidden Not present in docs on web-site.
   */
  @WithName("anonymous-paths")
  Optional<Set<String>> anonymousPaths();

  /**
   * Returns the set of HTTP URL path <em>prefixes</em> that are permitted to be serviced without
   * authentication. Anonymous access is granted, if the URI path is equal to any of the elements or
   * if the path starts with any of the elements, when each element is appended with a {@code /}.
   *
   * @hidden Not present in docs on web-site.
   */
  @WithName("anonymous-path-prefixes")
  Optional<Set<String>> anonymousPathPrefixes();
}
