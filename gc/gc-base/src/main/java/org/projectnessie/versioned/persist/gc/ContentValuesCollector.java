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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Reference;

/** Global commit-log scanning state and result. */
public final class ContentValuesCollector<GC_CONTENT_VALUES extends ExpiredContentValues> {

  private final Supplier<GC_CONTENT_VALUES> newContentValues;

  /** Content per content-id. */
  final Map<String, GC_CONTENT_VALUES> contentValues = new ConcurrentHashMap<>();

  public ContentValuesCollector(Supplier<GC_CONTENT_VALUES> newContentValues) {
    this.newContentValues = newContentValues;
  }

  public GC_CONTENT_VALUES contentValues(String contentId) {
    return contentValues.computeIfAbsent(contentId, k -> newContentValues.get());
  }

  public void gotValue(
      Content content, Reference reference, ContentKey contentKey, boolean isExpired) {
    contentValues(content.getId()).gotValue(content, reference, contentKey, isExpired);
  }
}
