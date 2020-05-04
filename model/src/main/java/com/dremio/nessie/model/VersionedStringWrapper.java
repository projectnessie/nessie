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

package com.dremio.nessie.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * wrap object with a version id. For eTag & Match-If
 */
public class VersionedStringWrapper<T> extends VersionedWrapper<T> {

  private String versionString;

  public VersionedStringWrapper(T obj, Long version, String versionString) {
    super(obj, version);
    this.versionString = versionString;
  }

  @JsonIgnore
  public VersionedStringWrapper<T> updateString(String obj) {
    versionString = obj;
    return this;
  }

  public String getVersionString() {
    return versionString;
  }
}
