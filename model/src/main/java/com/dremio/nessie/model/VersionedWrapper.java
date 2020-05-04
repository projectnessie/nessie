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

  public Long getVersion() {
    return version;
  }

  /**
   * increment version internally.
   *
   * <p>
   *   Typically this is done by the backend database rather than the server.
   * </p>
   * @return this with incremented version
   */
  public VersionedWrapper<T> increment() {
    version = version == null ? 0 : version;
    version++;
    return this;
  }

  public VersionedWrapper<T> update(T obj) {
    this.obj = obj;
    return this;
  }
}
