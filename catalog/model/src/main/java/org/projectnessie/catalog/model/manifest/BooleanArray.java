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
package org.projectnessie.catalog.model.manifest;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.IOException;
import java.util.Objects;

/**
 * Compressed-encoded array holding nullable {@link Boolean}s using 2 bits to encode the three
 * possible values {@code null}, {@code Boolean.FALSE} and {@code Boolean.TRUE}.
 *
 * <p>This implementation
 */
@JsonSerialize(using = BooleanArray.BooleanArraySerializer.class)
@JsonDeserialize(using = BooleanArray.BooleanArrayDeserializer.class)
public final class BooleanArray {
  private final byte[] data;

  public BooleanArray(byte[] data) {
    this.data = Objects.requireNonNull(data);
  }

  public BooleanArray(int capacity) {
    int arrayLength = (capacity >> 2) + ((capacity & 3) != 0 ? 1 : 0);
    this.data = new byte[arrayLength];
  }

  public byte[] bytes() {
    return data;
  }

  public void set(int index, Boolean value) {
    int i = arrayIndex(index);
    if (index < 0 || i >= data.length) {
      throw new ArrayIndexOutOfBoundsException();
    }
    int mask = maskForIndex(index);
    byte encoded = encodedForIndex(index, value);
    data[i] = (byte) ((data[i] & ~mask) | encoded);
  }

  public Boolean get(int index) {
    int i = arrayIndex(index);
    if (index < 0 || i >= data.length) {
      throw new ArrayIndexOutOfBoundsException();
    }
    byte el = data[i];
    return decodedForIndex(index, el);
  }

  static int arrayIndex(int index) {
    return index >> 2;
  }

  static byte maskForIndex(int index) {
    int shift = shiftForIndex(index);
    return (byte) (3 << shift);
  }

  static byte encodedForIndex(int index, Boolean value) {
    int shift = shiftForIndex(index);
    byte encoded = encodeBoolean(value);
    return (byte) (encoded << shift);
  }

  static Boolean decodedForIndex(int index, byte el) {
    int shift = shiftForIndex(index);
    return decodeBoolean((byte) ((el >> shift) & 3));
  }

  static byte encodeBoolean(Boolean value) {
    if (value == null) {
      return 0;
    }
    return (byte) (value ? 2 : 1);
  }

  static Boolean decodeBoolean(byte b) {
    switch (b) {
      case 0:
        return null;
      case 1:
        return Boolean.FALSE;
      case 2:
        return Boolean.TRUE;
      default:
        throw new IllegalArgumentException("" + b);
    }
  }

  static int shiftForIndex(int index) {
    return (index & 3) << 1;
  }

  public BooleanArray nullIfAllElementsNull() {
    for (byte b : data) {
      if (b != 0) {
        return this;
      }
    }
    return null;
  }

  public static final class BooleanArrayDeserializer extends JsonDeserializer<BooleanArray> {
    @Override
    public BooleanArray deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return new BooleanArray(p.getBinaryValue());
    }
  }

  public static final class BooleanArraySerializer extends JsonSerializer<BooleanArray> {
    @Override
    public void serialize(BooleanArray value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      gen.writeBinary(value.data);
    }
  }
}
