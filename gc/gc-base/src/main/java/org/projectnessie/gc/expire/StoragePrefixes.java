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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.projectnessie.storage.uri.StorageUri;

/** Matches storage locations against storage prefixes, used to restrict the sweep phase. */
public final class StoragePrefixes {

  private StoragePrefixes() {}

  /**
   * Returns a predicate that accepts a storage location if it is located at or below at least one
   * of the given prefixes.
   *
   * <p>Locations are compared by scheme, authority and whole path segments: the prefix {@code
   * s3://bucket/team_a} accepts {@code s3://bucket/team_a} and {@code s3://bucket/team_a/orders/},
   * but neither {@code s3://bucket/team_ab/} nor the parent {@code s3://bucket/}.
   */
  public static Predicate<StorageUri> atOrBelowAnyOf(Collection<StorageUri> prefixes) {
    List<StorageUri> copy = List.copyOf(prefixes);
    return location -> copy.stream().anyMatch(prefix -> isAtOrBelow(prefix, location));
  }

  /** Whether {@code location} is {@code prefix} itself or located below {@code prefix}. */
  static boolean isAtOrBelow(StorageUri prefix, StorageUri location) {
    if (!Objects.equals(prefix.scheme(), location.scheme())
        || !Objects.equals(prefix.authority(), location.authority())) {
      return false;
    }
    // Compare with a trailing separator, so that '/a/b' is neither below '/a/b/c' nor '/a/bc'
    // below '/a/b'. A missing path is the root of the bucket/file-system.
    String prefixPath = prefix.withTrailingSeparator().path();
    String locationPath = location.withTrailingSeparator().path();
    return locationPath.startsWith(prefixPath);
  }
}
