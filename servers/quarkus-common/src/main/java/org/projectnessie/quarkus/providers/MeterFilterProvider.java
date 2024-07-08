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
package org.projectnessie.quarkus.providers;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.inject.ConfigProperty;

public class MeterFilterProvider {

  @Inject
  @ConfigProperty(name = "nessie.metrics.tags")
  Map<String, String> tags;

  @Produces
  @Singleton
  public MeterFilter produceGlobalMeterFilter() {
    return MeterFilter.commonTags(
        this.tags.entrySet().stream()
            .map(e -> Tag.of(e.getKey(), e.getValue()))
            .collect(Collectors.toSet()));
  }
}
