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
package org.projectnessie.catalog.formats.iceberg.metrics;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import java.util.Locale;

public enum IcebergUnit {
  UNDEFINED("undefined"),
  BYTES("bytes"),
  COUNT("count"),
  ;

  private final String key;

  IcebergUnit(String key) {
    this.key = key;
  }

  @JsonValue
  public String getKey() {
    return this.key;
  }

  public static IcebergUnit fromString(String key) {
    Preconditions.checkArgument(null != key, "Invalid unit: null");

    try {
      return valueOf(key.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Invalid unit: %s", key), e);
    }
  }
}
