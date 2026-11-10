/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.gc.expire;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.projectnessie.storage.uri.StorageUri;

/**
 * Identifies the base locations that must not be expired, because the files in those can belong to
 * more than one content: base locations that are used by multiple content IDs and base locations
 * that contain, or are contained in, another base location.
 */
public final class SharedBaseLocations {

  /** Normalized base locations to the IDs of the contents using them. */
  private final Map<String, Set<String>> contentIdsByLocation = new TreeMap<>();

  private final Set<String> shared = new TreeSet<>();

  /**
   * Identifies the shared base locations.
   *
   * @param baseLocations base locations of all contents of a live-content-set, by content ID
   */
  public SharedBaseLocations(Map<String, ? extends Collection<StorageUri>> baseLocations) {
    baseLocations.forEach((contentId, locations) ->
        locations.forEach(location ->
            contentIdsByLocation.merge(
                normalize(location),
                Set.of(contentId),
                (ids, contentIds) -> {
                  Set<String> merged = new TreeSet<>(ids);
                  merged.addAll(contentIds);
                  return merged;
                })));

    // Locations iterate in their natural order, a location is directly followed by the locations
    // that it contains. 'containing' holds the locations that contain the current one.
    Deque<String> containing = new ArrayDeque<>();
    contentIdsByLocation.forEach((location, contentIds) -> {
      while (!containing.isEmpty() && !location.startsWith(containing.peek())) {
        containing.pop();
      }
      if (contentIds.size() > 1) {
        shared.add(location);
      }
      if (!containing.isEmpty()) {
        shared.add(location);
        shared.addAll(containing);
      }
      containing.push(location);
    });
  }

  /**
   * Whether files in the given base location can belong to more than one content. Unknown base
   * locations are considered shared.
   */
  public boolean isShared(StorageUri baseLocation) {
    String location = normalize(baseLocation);
    return shared.contains(location) || !contentIdsByLocation.containsKey(location);
  }

  /** The IDs of the contents using the given base location. */
  public Set<String> contentIds(StorageUri baseLocation) {
    return contentIdsByLocation.getOrDefault(normalize(baseLocation), Set.of());
  }

  /** The number of shared base locations. */
  public int size() {
    return shared.size();
  }

  /**
   * Equivalent locations yield the same string: schemes are lower-cased, equivalent schemes like
   * {@code s3a} and {@code s3} are mapped to one scheme and the result always ends with a {@code
   * /}.
   */
  static String normalize(StorageUri location) {
    StringBuilder sb = new StringBuilder();
    String scheme = location.scheme();
    if (scheme != null) {
      scheme = scheme.toLowerCase(Locale.ROOT);
      sb.append(
              switch (scheme) {
                case "s3a", "s3n" -> "s3";
                case "abfss" -> "abfs";
                case "wasbs" -> "wasb";
                default -> scheme;
              })
          .append("://");
    }
    if (location.authority() != null) {
      sb.append(location.authority());
    }
    String path = location.path();
    if (path != null) {
      if (!path.startsWith("/")) {
        sb.append('/');
      }
      sb.append(path);
    }
    if (sb.isEmpty() || sb.charAt(sb.length() - 1) != '/') {
      sb.append('/');
    }
    return sb.toString();
  }
}
