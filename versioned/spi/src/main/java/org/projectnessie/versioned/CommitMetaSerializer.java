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
package org.projectnessie.versioned;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;
import java.io.IOException;
import java.io.InputStream;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ser.Views;

public class CommitMetaSerializer implements Serializer<CommitMeta> {

  public static final Serializer<CommitMeta> METADATA_SERIALIZER = new CommitMetaSerializer();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public ByteString toBytes(CommitMeta value) {
    try (Output out = ByteString.newOutput()) {
      // Store commit metadata using v1 format. This is to allow rolling upgrades to server
      // versions with v2 support.
      MAPPER.writerWithView(Views.V1.class).writeValue(out, value);
      return out.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Couldn't serialize commit meta %s", value), e);
    }
  }

  @Override
  public CommitMeta fromBytes(ByteString bytes) {
    try (InputStream in = bytes.newInput()) {
      return MAPPER.readValue(in, CommitMeta.class);
    } catch (IOException e) {
      return ImmutableCommitMeta.builder()
          .message("unknown")
          .committer("unknown")
          .hash("unknown")
          .build();
    }
  }
}
