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

public class GCResult<GC_CONTENT_VALUES extends ExpiredContentValues> {
  private final Map<String, GC_CONTENT_VALUES> contentValues;

  public GCResult(Map<String, GC_CONTENT_VALUES> contentValues) {
    this.contentValues = contentValues;
  }

  public Map<String, GC_CONTENT_VALUES> getContentValues() {
    return contentValues;
  }
}
