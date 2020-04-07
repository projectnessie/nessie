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

package com.dremio.iceberg.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * configuration object to tell a client how a server is configured.
 */
public final class AlleyConfiguration {

  @JsonIgnore
  private static final String CURRENT_VERSION = "1.0";
  private final String defaultTag;
  private final String version;

  public AlleyConfiguration() {
    this(null, null);
  }

  public AlleyConfiguration(String defaultTag,
                            String version
  ) {
    this.defaultTag = defaultTag;
    this.version = version;
  }

  public static AlleyConfiguration alleyConfiguration(String defaultTag) {
    return new AlleyConfiguration(defaultTag, CURRENT_VERSION);
  }

  public String getDefaultTag() {
    return defaultTag;
  }

  public String getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AlleyConfiguration that = (AlleyConfiguration) o;
    return Objects.equals(defaultTag, that.defaultTag)
      && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(defaultTag, version);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", AlleyConfiguration.class.getSimpleName() + "[", "]")
      .add("defaultTag='" + defaultTag + "'")
      .add("version='" + version + "'")
      .toString();
  }
}
