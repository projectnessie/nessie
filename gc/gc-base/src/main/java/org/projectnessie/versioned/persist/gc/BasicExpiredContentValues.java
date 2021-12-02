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
package org.projectnessie.versioned.persist.gc;

import java.util.HashSet;
import java.util.Set;
import org.projectnessie.model.Content;

public final class BasicExpiredContentValues extends ExpiredContentValues {

  private final Set<Content> expiredContents = new HashSet<>();
  private final Set<Content> liveContents = new HashSet<>();

  @Override
  protected void addValue(Content content, boolean isExpired) {
    if (isExpired) {
      // Move to expired list only if the content is not live from other walked references.
      if (!liveContents.contains(content)) {
        expiredContents.add(content);
      }
    } else {
      liveContents.add(content);
      // If content is found live, remove it from expired set if exists.
      expiredContents.remove(content);
    }
  }

  Set<Content> getExpiredContents() {
    return expiredContents;
  }

  public Set<Content> getLiveContents() {
    return liveContents;
  }
}
