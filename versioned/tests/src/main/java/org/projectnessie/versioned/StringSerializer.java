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
package org.projectnessie.versioned;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.spy;

import com.google.protobuf.ByteString;
import javax.annotation.Nonnull;

/**
 * ValueWorker implementation for {@code String class}. Can also be used as simple {@link
 * Serializer}.
 */
public class StringSerializer implements SerializerWithPayload<String, StringSerializer.TestEnum> {
  private static final SerializerWithPayload<String, TestEnum> INSTANCE =
      spy(new StringSerializer());

  public enum TestEnum {
    YES,
    NO,
    NULL;
  }

  private StringSerializer() {}

  /**
   * Get a instance of a string serializer.
   *
   * @return the instance
   */
  @Nonnull
  public static SerializerWithPayload<String, TestEnum> getInstance() {
    return INSTANCE;
  }

  @Override
  public String fromBytes(ByteString bytes) {
    return bytes.toString(UTF_8);
  }

  @Override
  public ByteString toBytes(String value) {
    return ByteString.copyFrom(value, UTF_8);
  }

  @Override
  public Byte getPayload(String value) {
    return 0;
  }

  @Override
  public TestEnum getType(Byte payload) {
    if (payload == null) {
      return TestEnum.NULL;
    }
    return payload > 60 ? TestEnum.YES : TestEnum.NO;
  }
}
