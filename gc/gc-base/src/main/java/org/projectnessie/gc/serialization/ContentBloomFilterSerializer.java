/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.projectnessie.gc.base.ContentBloomFilter;

public class ContentBloomFilterSerializer extends Serializer<ContentBloomFilter> {
  @Override
  @SuppressWarnings("UnstableApiUsage")
  public void write(Kryo kryo, Output output, ContentBloomFilter contentBloomFilter) {
    try {
      output.writeBoolean(contentBloomFilter.wasMerged());
      contentBloomFilter.getFilter().writeTo(output);
    } catch (IOException e) {
      throw new RuntimeException("Problem when writing to output", e);
    }
  }

  @Override
  @SuppressWarnings("UnstableApiUsage")
  public ContentBloomFilter read(Kryo kryo, Input input, Class<ContentBloomFilter> classType) {

    try {
      return new ContentBloomFilter(
          input.readBoolean(),
          BloomFilter.readFrom(input, Funnels.stringFunnel(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new RuntimeException("Problem when reading from input", e);
    }
  }
}
