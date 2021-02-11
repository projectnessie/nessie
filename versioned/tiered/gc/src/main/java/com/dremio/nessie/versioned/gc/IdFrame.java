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
package com.dremio.nessie.versioned.gc;

import java.util.Arrays;

import com.dremio.nessie.versioned.store.Id;

/**
 * simple container to hold an Id in its byte[] form. Useful as a container in a Spark Row.
 */
public class IdFrame {

  private byte[] id;

  public byte[] getId() {
    return id;
  }

  public void setId(byte[] id) {
    this.id = id;
  }

  public IdFrame() {
  }

  public IdFrame(byte[] id) {
    this.id = id;
  }

  public static IdFrame of(Id id) {
    return new IdFrame(id.toBytes());
  }

  @Override
  public String toString() {
    return Id.of(id).toString();
  }

  public boolean equalsId(Id id) {
    return Arrays.equals(this.id, id.toBytes());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(id);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof IdFrame)) {
      return false;
    }
    IdFrame other = (IdFrame) obj;
    return Arrays.equals(id, other.id);
  }


}
