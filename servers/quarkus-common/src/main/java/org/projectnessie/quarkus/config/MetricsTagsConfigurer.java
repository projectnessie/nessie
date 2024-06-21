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

package org.projectnessie.quarkus.config;

import io.micrometer.core.instrument.Tags;
import io.quarkus.micrometer.runtime.HttpServerMetricsTagsContributor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class MetricsTagsConfigurer implements HttpServerMetricsTagsContributor {

  @Inject
  @ConfigProperty(name = "nessie.metrics.tags")
  Map<String, String> tags;

  @Override
  public Tags contribute(Context context) {
    if (tags == null) {
      return Tags.empty();
    }
    return tags.entrySet().stream()
        .map(e -> Tags.of(e.getKey(), e.getValue()))
        .reduce(Tags::and)
        .orElse(Tags.empty());
  }
}
