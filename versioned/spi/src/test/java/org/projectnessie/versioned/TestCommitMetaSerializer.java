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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ser.Views;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

public class TestCommitMetaSerializer {

  @Test
  void testCommitSerde() throws JsonProcessingException {
    CommitMeta expectedCommit =
        ImmutableCommitMeta.builder()
            .commitTime(Instant.now())
            .authorTime(Instant.now())
            .author("bill")
            .committer("ted")
            .hash("xyz")
            .message("commit msg")
            .build();

    ByteString expectedBytes =
        ByteString.copyFrom(
            new ObjectMapper().writerWithView(Views.V1.class).writeValueAsBytes(expectedCommit));
    CommitMeta actualCommit = CommitMetaSerializer.METADATA_SERIALIZER.fromBytes(expectedBytes);
    assertThat(actualCommit).isEqualTo(expectedCommit);
    ByteString actualBytes = CommitMetaSerializer.METADATA_SERIALIZER.toBytes(expectedCommit);
    assertThat(actualBytes).isEqualTo(expectedBytes);
    actualBytes = CommitMetaSerializer.METADATA_SERIALIZER.toBytes(expectedCommit);
    assertThat(CommitMetaSerializer.METADATA_SERIALIZER.fromBytes(actualBytes))
        .isEqualTo(expectedCommit);
    actualCommit = CommitMetaSerializer.METADATA_SERIALIZER.fromBytes(expectedBytes);
    assertThat(CommitMetaSerializer.METADATA_SERIALIZER.toBytes(actualCommit))
        .isEqualTo(expectedBytes);
  }
}
