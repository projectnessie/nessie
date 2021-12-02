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
package org.projectnessie.gc.base;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.model.Content;

/**
 * Output of Identify GC action {@link GCImpl#identifyExpiredContents(SparkSession)}. Contains a map
 * of {@link ContentValues} which contains the globally expired contents per content id per
 * reference.
 */
public class IdentifiedResult implements Serializable {

  private static final long serialVersionUID = 900739110223735484L;
  // ContentValues per reference per content id.
  private final Map<String, Map<String, ContentValues>> contentValues = new ConcurrentHashMap<>();

  public void addContent(String refName, Content content) {
    contentValues
        .computeIfAbsent(refName, k -> new HashMap<>())
        .computeIfAbsent(content.getId(), k -> new ContentValues())
        .gotValue(content);
  }

  public Map<String, Map<String, ContentValues>> getContentValues() {
    return contentValues;
  }

  public Map<String, ContentValues> getContentValuesForReference(String refName) {
    return contentValues.getOrDefault(refName, Collections.emptyMap());
  }
}
