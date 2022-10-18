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
package org.projectnessie.versioned.storage.common.objtypes;

import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.capitalizeName;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.newCommitHeaders;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeadersImpl.lowerCase;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCommitHeaders {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void nonExistingHeader() {
    CommitHeaders header = newCommitHeaders().add("Foo", "bar").build();

    soft.assertThat(header.getFirst("Baz")).isNull();
    soft.assertThat(header.getAll("Baz")).isNull();

    soft.assertThat(header.keySet()).containsExactlyInAnyOrder("foo");
  }

  @Test
  public void caseInsensivity() {
    CommitHeaders header =
        newCommitHeaders().add("Foo", "bar").add("foo", "baz").add("fOO", "meep").build();

    soft.assertThat(header.getFirst("Foo")).isEqualTo("bar");
    soft.assertThat(header.getAll("Foo")).containsExactly("bar", "baz", "meep");
    soft.assertThat(header.getFirst("foo")).isEqualTo("bar");
    soft.assertThat(header.getAll("foo")).containsExactly("bar", "baz", "meep");
    soft.assertThat(header.getFirst("fOO")).isEqualTo("bar");
    soft.assertThat(header.getAll("fOO")).containsExactly("bar", "baz", "meep");

    soft.assertThat(header.keySet()).containsExactlyInAnyOrder("foo");
  }

  @Test
  public void multipleValues() {
    CommitHeaders header =
        newCommitHeaders()
            .add("Foo", "foo-foo")
            .add("Bar", "bar-bar")
            .add("Baz", "baz-baz")
            .build();

    soft.assertThat(header.getFirst("foo")).isEqualTo("foo-foo");
    soft.assertThat(header.getAll("foo")).containsExactly("foo-foo");
    soft.assertThat(header.getFirst("bar")).isEqualTo("bar-bar");
    soft.assertThat(header.getAll("bar")).containsExactly("bar-bar");
    soft.assertThat(header.getFirst("baz")).isEqualTo("baz-baz");
    soft.assertThat(header.getAll("baz")).containsExactly("baz-baz");

    soft.assertThat(header.keySet()).containsExactlyInAnyOrder("foo", "bar", "baz");
  }

  @Test
  public void fromToEmpty() {
    CommitHeaders from =
        newCommitHeaders()
            .add("foo", "foo-foo")
            .add("bar", "bar-bar")
            .add("baz", "baz-baz")
            .build();

    CommitHeaders header = newCommitHeaders().from(from).build();

    soft.assertThat(header.getFirst("foo")).isEqualTo("foo-foo");
    soft.assertThat(header.getAll("foo")).containsExactly("foo-foo");
    soft.assertThat(header.getFirst("bar")).isEqualTo("bar-bar");
    soft.assertThat(header.getAll("bar")).containsExactly("bar-bar");
    soft.assertThat(header.getFirst("baz")).isEqualTo("baz-baz");
    soft.assertThat(header.getAll("baz")).containsExactly("baz-baz");

    soft.assertThat(header.keySet()).containsExactlyInAnyOrder("foo", "bar", "baz");
  }

  @Test
  public void fromToPreFilled() {
    CommitHeaders from =
        newCommitHeaders()
            .add("foo", "foo-foo")
            .add("bar", "bar-bar")
            .add("baz", "baz-baz")
            .build();

    CommitHeaders header =
        newCommitHeaders().add("abc", "def").add("bar", "bar").from(from).build();

    soft.assertThat(header.getFirst("Abc")).isEqualTo("def");
    soft.assertThat(header.getAll("Abc")).containsExactly("def");
    soft.assertThat(header.getFirst("foo")).isEqualTo("foo-foo");
    soft.assertThat(header.getAll("foo")).containsExactly("foo-foo");
    soft.assertThat(header.getFirst("bar")).isEqualTo("bar");
    soft.assertThat(header.getAll("bar")).containsExactly("bar", "bar-bar");
    soft.assertThat(header.getFirst("baz")).isEqualTo("baz-baz");
    soft.assertThat(header.getAll("baz")).containsExactly("baz-baz");

    soft.assertThat(header.keySet()).containsExactlyInAnyOrder("abc", "foo", "bar", "baz");
  }

  @Test
  public void fromToPostFilled() {
    CommitHeaders from =
        newCommitHeaders()
            .add("foo", "foo-foo")
            .add("bar", "bar-bar")
            .add("baz", "baz-baz")
            .build();

    CommitHeaders header =
        newCommitHeaders().from(from).add("abc", "def").add("bar", "bar").build();

    soft.assertThat(header.getFirst("Abc")).isEqualTo("def");
    soft.assertThat(header.getAll("Abc")).containsExactly("def");
    soft.assertThat(header.getFirst("foo")).isEqualTo("foo-foo");
    soft.assertThat(header.getAll("foo")).containsExactly("foo-foo");
    soft.assertThat(header.getFirst("bar")).isEqualTo("bar-bar");
    soft.assertThat(header.getAll("bar")).containsExactly("bar-bar", "bar");
    soft.assertThat(header.getFirst("baz")).isEqualTo("baz-baz");
    soft.assertThat(header.getAll("baz")).containsExactly("baz-baz");

    soft.assertThat(header.keySet()).containsExactlyInAnyOrder("abc", "foo", "bar", "baz");
  }

  @Test
  public void toBuilder() {
    CommitHeaders from =
        newCommitHeaders()
            .add("foo", "foo-foo")
            .add("bar", "bar-bar")
            .add("baz", "baz-baz")
            .build();

    CommitHeaders header = from.toBuilder().add("abc", "def").add("bar", "bar").build();

    soft.assertThat(header.getFirst("Abc")).isEqualTo("def");
    soft.assertThat(header.getAll("Abc")).containsExactly("def");
    soft.assertThat(header.getFirst("foo")).isEqualTo("foo-foo");
    soft.assertThat(header.getAll("foo")).containsExactly("foo-foo");
    soft.assertThat(header.getFirst("bar")).isEqualTo("bar-bar");
    soft.assertThat(header.getAll("bar")).containsExactly("bar-bar", "bar");
    soft.assertThat(header.getFirst("baz")).isEqualTo("baz-baz");
    soft.assertThat(header.getAll("baz")).containsExactly("baz-baz");

    soft.assertThat(header.keySet()).containsExactlyInAnyOrder("abc", "foo", "bar", "baz");
  }

  @ParameterizedTest
  @CsvSource({
    "Hello,Hello",
    "HELLO,Hello",
    "hElLo,Hello",
    "hello,Hello",
    "modified-since,Modified-Since",
    "MODIFIED-SINCE,Modified-Since"
  })
  public void capitalization(String input, String expected) {
    String header = newCommitHeaders().add(input, "VALUE").build().keySet().iterator().next();
    soft.assertThat(header).isEqualTo(lowerCase(expected));
    soft.assertThat(capitalizeName(input)).isEqualTo(expected);
  }
}
