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
import java.util.OptionalLong;

/**
 * wrap object with a version id. For eTag & Match-If
 */
public class VersionedWrapper<T> {

  private T obj;
  private Long version;

  public VersionedWrapper() {
    this(null, null);
  }

  public VersionedWrapper(T obj, Long version) {
    this.obj = obj;
    this.version = version;
  }

  public VersionedWrapper(T obj) {
    this(obj, null);
  }

  public T getObj() {
    return obj;
  }

  public OptionalLong getVersion() {
    return version == null ? OptionalLong.empty() : OptionalLong.of(version);
  }

  @JsonIgnore
  public VersionedWrapper<T> increment() {
    this.version = (version == null ? 0 : version) + 1;
    return this;
  }

  @JsonIgnore
  public VersionedWrapper<T> update(T obj) {
    this.obj = obj;
    return this;
  }
}
