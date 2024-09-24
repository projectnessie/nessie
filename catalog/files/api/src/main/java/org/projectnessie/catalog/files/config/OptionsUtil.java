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
package org.projectnessie.catalog.files.config;

import java.util.Map;
import java.util.Optional;
import org.projectnessie.storage.uri.StorageUri;

final class OptionsUtil {
  private OptionsUtil() {}

  static <B extends PerBucket> Optional<B> resolveSpecializedBucket(
      StorageUri uri, Map<String, B> buckets) {
    String bucketName = uri.requiredAuthority();
    String path = uri.pathWithoutLeadingTrailingSlash();

    B specific = null;
    int matchLen = -1;
    for (Map.Entry<String, B> entry : buckets.entrySet()) {
      String key = entry.getKey();
      B bucket = entry.getValue();

      String authority =
          bucket.authority().isPresent() ? bucket.authority().get() : bucket.name().orElse(key);

      if (!authority.equals(bucketName)) {
        continue;
      }
      String bucketPathPrefix = bucket.pathPrefix().orElse("");
      if (!path.startsWith(bucketPathPrefix)) {
        continue;
      }
      int len = bucketPathPrefix.length();
      if (matchLen == -1 || matchLen < len) {
        matchLen = len;
        specific = bucket;
      }
    }

    return Optional.ofNullable(specific);
  }
}
