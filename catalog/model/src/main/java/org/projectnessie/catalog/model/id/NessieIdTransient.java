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

/**
 * A form of {@link NessieId} that does not contain any data and cannot be compared of serialized.
 * This {@link NessieId} implementation should be used only in transient objects.
 */
final class NessieIdTransient implements NessieId {
  private static final NessieId INSTANCE = new NessieIdTransient();

  private NessieIdTransient() {}

  static NessieId nessieIdTransient() {
    return INSTANCE;
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public byte byteAt(int index) {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public long longAt(int index) {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public ByteBuffer id() {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public String idAsString() {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public byte[] idAsBytes() {
    throw new UnsupportedOperationException("Transient ID");
  }

  @Override
  public boolean equals(Object obj) {
    return obj == this;
  }

  @Override
  public int hashCode() {
    return 2;
  }
}
