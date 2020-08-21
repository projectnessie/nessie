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
package com.dremio.nessie.versioned.impl;

public class Pointer<T extends HasId> {
  private T value;
  private boolean dirty = false;

  public Pointer(){}

  public Pointer(T value){
    this.value = value;
  }

  public Id apply(Transform<T> transform) {
    this.value = transform.apply(value);
    this.dirty = true;
    return value.getId();
  }

  public interface Transform<T> {
    T apply(T t);
  }

  public boolean isDirty() {
    return dirty;
  }

  public boolean isEmpty() {
    return value == null;
  }

  public void set(T value) {
    this.value = value;
  }

  T get() {
    return value;
  }
}
