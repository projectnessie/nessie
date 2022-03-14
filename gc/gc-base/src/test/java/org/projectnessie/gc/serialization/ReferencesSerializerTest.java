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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import org.junit.jupiter.api.Test;
import org.projectnessie.gc.base.ImmutableGCParams;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableReferenceMetadata;
import org.projectnessie.model.ImmutableTag;
import org.projectnessie.model.Reference;

class ReferencesSerializerTest {

  @Test
  public void shouldSerializerAndDeserializerImmutableBranch() {
    // given
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    properties.put("key2", "value2");

    Kryo kryo = new Kryo();
    new ReferencesKryoRegistrator().registerClasses(kryo);
    ImmutableBranch immutableBranch =
        ImmutableBranch.builder()
            .hash("0a62a2d020df785dfb3d34e9fb64d965e6e65220")
            .name("name")
            .metadata(
                ImmutableReferenceMetadata.builder()
                    .numTotalCommits(10L)
                    .commonAncestorHash("0a62a2d020df785dfb3d34e9fb64d965e6e65223")
                    .numCommitsAhead(1)
                    .numCommitsBehind(5)
                    .commitMetaOfHEAD(
                        ImmutableCommitMeta.builder()
                            .author("author_1")
                            .message("msg")
                            .committer("committer")
                            .hash("0a62a2d020df785dfb3d34e9fb64d965e6e65226")
                            .signedOffBy("author_signed")
                            .commitTime(Instant.now())
                            .authorTime(Instant.now().plusSeconds(10))
                            .properties(properties)
                            .build())
                    .build())
            .build();

    // when
    Output output = createOutput();
    kryo.writeObject(output, immutableBranch);

    // then
    ImmutableBranch immutableBranchDeserialized =
        kryo.readObject(new Input(output.toBytes()), ImmutableBranch.class);
    assertThat(immutableBranch).isEqualTo(immutableBranchDeserialized);
  }

  @Test
  public void shouldSerializerAndDeserializerImmutableTag() {
    // given
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    properties.put("key2", "value2");

    Kryo kryo = new Kryo();
    new ReferencesKryoRegistrator().registerClasses(kryo);
    ImmutableTag immutableTag =
        ImmutableTag.builder()
            .hash("0a62a2d020df785dfb3d34e9fb64d965e6e65220")
            .name("name")
            .metadata(
                ImmutableReferenceMetadata.builder()
                    .numTotalCommits(10L)
                    .commonAncestorHash("0a62a2d020df785dfb3d34e9fb64d965e6e65223")
                    .numCommitsAhead(1)
                    .numCommitsBehind(5)
                    .commitMetaOfHEAD(
                        ImmutableCommitMeta.builder()
                            .author("author_1")
                            .message("msg")
                            .committer("committer")
                            .hash("0a62a2d020df785dfb3d34e9fb64d965e6e65226")
                            .signedOffBy("author_signed")
                            .commitTime(Instant.now())
                            .authorTime(Instant.now().plusSeconds(10))
                            .properties(properties)
                            .build())
                    .build())
            .build();

    // when
    Output output = createOutput();
    kryo.writeObject(output, immutableTag);

    // then
    ImmutableTag immutableTagDeserialized =
        kryo.readObject(new Input(output.toBytes()), ImmutableTag.class);
    assertThat(immutableTag).isEqualTo(immutableTagDeserialized);
  }

  @Test
  public void shouldSerializeAndDeserializeCollectionOfReferences() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    properties.put("key2", "value2");

    Kryo kryo = new Kryo();
    new ReferencesKryoRegistrator().registerClasses(kryo);
    ImmutableBranch immutableBranch =
        ImmutableBranch.builder()
            .hash("0a62a2d020df785dfb3d34e9fb64d965e6e65220")
            .name("name")
            .metadata(
                ImmutableReferenceMetadata.builder()
                    .numTotalCommits(10L)
                    .commonAncestorHash("0a62a2d020df785dfb3d34e9fb64d965e6e65223")
                    .numCommitsAhead(1)
                    .numCommitsBehind(5)
                    .commitMetaOfHEAD(
                        ImmutableCommitMeta.builder()
                            .author("author_1")
                            .message("msg")
                            .committer("committer")
                            .hash("0a62a2d020df785dfb3d34e9fb64d965e6e65226")
                            .signedOffBy("author_signed")
                            .commitTime(Instant.now())
                            .authorTime(Instant.now().plusSeconds(10))
                            .properties(properties)
                            .build())
                    .build())
            .build();

    // when
    Output output = createOutput();
    Map<Reference, Instant> map = new LinkedHashMap<>();
    map.put(immutableBranch, Instant.now());
    kryo.writeObject(output, map);

    // then
    Map<Reference, Instant> mapDeserialized =
        kryo.readObject(new Input(output.toBytes()), LinkedHashMap.class);
    assertThat(map).isEqualTo(mapDeserialized);
  }

  @Test
  public void shouldSerializeAndDeserializeGCParams() {
    // given
    Map<String, String> nessieConfig = new HashMap<>();
    nessieConfig.put("k1", "v1");

    Map<String, Instant> cutoffTimestamp = new HashMap<>();
    cutoffTimestamp.put("k1", Instant.now());

    Kryo kryo = new Kryo();
    new ReferencesKryoRegistrator().registerClasses(kryo);
    ImmutableGCParams gcParams =
        ImmutableGCParams.builder()
            .nessieClientConfigs(nessieConfig)
            .deadReferenceCutOffTimeStamp(Instant.now().plusSeconds(60))
            .bloomFilterExpectedEntries(10L)
            .defaultCutOffTimestamp(Instant.now())
            .commitProtectionDuration(Duration.ofDays(1))
            .bloomFilterFpp(0.3)
            .cutOffTimestampPerRef(cutoffTimestamp)
            .sparkPartitionsCount(36)
            .build();

    // when
    Output output = createOutput();
    kryo.writeObject(output, gcParams);

    // then
    ImmutableGCParams immutableGCParamsDeserialized =
        kryo.readObject(new Input(output.toBytes()), ImmutableGCParams.class);
    assertThat(gcParams).isEqualTo(immutableGCParamsDeserialized);
  }

  private Output createOutput() {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(16384);
    DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream);
    return new Output(deflaterOutputStream);
  }
}
