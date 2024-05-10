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
package org.projectnessie.catalog.model.id;

import java.nio.ByteBuffer;

final class NessieIdEmpty implements NessieId {
  static final NessieIdEmpty INSTANCE = new NessieIdEmpty();

  private NessieIdEmpty() {}

  @Override
  public int size() {
    return 0;
  }

  @Override
  public byte byteAt(int index) {
    throw new IllegalArgumentException();
  }

  @Override
  public long longAt(int index) {
    throw new IllegalArgumentException();
  }

  @Override
  public ByteBuffer id() {
    return ByteBuffer.allocate(0);
  }

  @Override
  public String idAsString() {
    return "";
  }

  @Override
  public byte[] idAsBytes() {
    return new byte[0];
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof NessieId)) {
      return false;
    }
    return ((NessieId) obj).size() == 0;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return "";
  }
}
