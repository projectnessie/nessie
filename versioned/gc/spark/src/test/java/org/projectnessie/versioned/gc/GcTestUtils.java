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
package org.projectnessie.versioned.gc;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.projectnessie.versioned.Serializer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

public final class GcTestUtils {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private GcTestUtils() {

  }

  public static class JsonSerializer<T> implements Serializer<T>, Serializable {

    private static final long serialVersionUID = 4052464280276785753L;

    private final Class<T> clazz;

    public JsonSerializer(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public ByteString toBytes(T value) {
      try {
        return ByteString.copyFrom(MAPPER.writer().writeValueAsBytes(value));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public T fromBytes(ByteString bytes) {
      try {
        return MAPPER.reader().readValue(bytes.toByteArray(), clazz);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

  }

  // annoying games w/ Java generics.
  public static class DummyAssetKeySerializer implements Serializer<AssetKey>, Serializable {
    private final Serializer<DummyAsset> delegate = new JsonSerializer<>(DummyAsset.class);

    @Override
    public ByteString toBytes(AssetKey value) {
      return delegate.toBytes((DummyAsset) value);
    }

    @Override
    public AssetKey fromBytes(ByteString bytes) {
      return delegate.fromBytes(bytes);
    }
  }





  public static class DummyAsset extends AssetKey {

    private int id;

    @JsonCreator
    public DummyAsset(@JsonProperty("id") int id) {
      this.id = id;
    }

    public DummyAsset() {
    }


    public int getId() {
      return id;
    }

    @Override
    public CompletableFuture<Boolean> delete() {
      return CompletableFuture.completedFuture(true);
    }

    @Override
    public List<String> toReportableName() {
      return Arrays.asList(Integer.toString(id));
    }

    @Override
    public byte[] toUniqueKey() {
      return Integer.toString(id).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object other) {
      return other != null && other instanceof DummyAsset && ((DummyAsset)other).id == id;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(id);
    }

  }

}
