/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.store.types;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.store.DefaultStoreWorker;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCustomContentSerialization {

  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void reSerialize() {
    CustomTestContent testContent =
        ImmutableCustomTestContent.builder().someLong(42L).someString("blah").build();

    ByteString serialized = DefaultStoreWorker.instance().toStoreOnReferenceState(testContent);
    soft.assertThat(serialized)
        .asInstanceOf(type(ByteString.class))
        .extracting(ByteString::toStringUtf8)
        .isEqualTo("blah|42|");

    Content deserialized =
        DefaultStoreWorker.instance()
            .valueFromStore(CustomTestContentSerializer.PAYLOAD, serialized);
    soft.assertThat(deserialized).isEqualTo(testContent);
  }

  @Test
  void reSerializeWithId() {
    String id = randomUUID().toString();
    CustomTestContent testContent =
        ImmutableCustomTestContent.builder().someLong(42L).someString("blah").id(id).build();

    ByteString serialized = DefaultStoreWorker.instance().toStoreOnReferenceState(testContent);
    soft.assertThat(serialized)
        .asInstanceOf(type(ByteString.class))
        .extracting(ByteString::toStringUtf8)
        .isEqualTo("blah|42|" + id);

    Content deserialized =
        DefaultStoreWorker.instance()
            .valueFromStore(CustomTestContentSerializer.PAYLOAD, serialized);
    soft.assertThat(deserialized).isEqualTo(testContent);
  }
}
