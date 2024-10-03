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
package org.projectnessie.events.ri.messaging.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithConverter;
import io.smallrye.config.WithParentName;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.spi.Converter;
import org.projectnessie.events.api.EventType;

/** Configuration for event subscribers. */
@ConfigMapping(prefix = "subscribers-config")
public interface MessagingEventSubscribersConfig {

  @WithParentName
  Map<String, EventSubscriberConfig> subscribers();

  interface EventSubscriberConfig {

    EventSubscriberConfig EMPTY =
        new EventSubscriberConfig() {
          @Override
          public Optional<Set<String>> repositoryIds() {
            return Optional.empty();
          }

          @Override
          public Optional<Set<EventType>> eventTypes() {
            return Optional.empty();
          }
        };

    /** The repository IDs to watch. If empty, all repositories are watched. */
    Optional<Set<@WithConverter(RepoIdConverter.class) String>> repositoryIds();

    /** The event types to watch. If empty, all event types are watched. */
    Optional<Set<EventType>> eventTypes();
  }

  class RepoIdConverter implements Converter<String> {

    @Override
    public String convert(String s) {
      return s == null ? null : s.trim();
    }
  }
}
