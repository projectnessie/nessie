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
package com.dremio.nessie.versioned;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.dremio.nessie.versioned.AssetKey.NoOpAssetKey;
import com.google.common.hash.Funnel;
import com.google.protobuf.ByteString;

/**
 * Serializer and ValueHelper implementation for {@code String class}.
 */
public final class StringSerializer implements ValueWorker<String, NoOpAssetKey> {
  private static final ValueWorker<String, NoOpAssetKey> INSTANCE = new StringSerializer();

  private StringSerializer() {
  }

  /**
   * Get a instance of a string serializer.
   * @return the instance
   */
  @Nonnull
  public static ValueWorker<String, NoOpAssetKey> getInstance() {
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
  public Stream<String> getAssetKeys(String value) {
    return Stream.of();
  }

  @Override
  public Funnel<String> getFunnel() {
    return (val, sink) -> sink.putString(val, StandardCharsets.UTF_8);
  }

  @Override
  public Serializer<NoOpAssetKey> getAssetKeySerializer() {
    return NoOpAssetKey.SERIALIZER;
  }
}
