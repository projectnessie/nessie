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
package org.projectnessie.quarkus.runner;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.junit.jupiter.api.Test;

class TestInputBuffer {

  @Test
  void emptyInput() {
    List<String> lines = new ArrayList<>();
    InputBuffer buf = new InputBuffer(new StringReader(""), lines::add);
    assertThat(buf.io()).isFalse();
    assertThat(lines).isEmpty();
    buf.flush();
    assertThat(lines).isEmpty();
  }

  @Test
  void scattered() {
    ArrayBlockingQueue<Integer> characters = new ArrayBlockingQueue<>(250);

    // Just need some Reader implementation that implements ready() + read()
    Reader reader =
        new StringReader("") {
          @Override
          public int read() {
            return characters.poll();
          }

          @Override
          public boolean ready() {
            return characters.size() > 0;
          }
        };

    List<String> lines = new ArrayList<>();
    InputBuffer buf = new InputBuffer(reader, lines::add);
    assertThat(buf.io()).isFalse();
    assertThat(lines).isEmpty();

    for (char c : "Hello World".toCharArray()) {
      characters.add((int) c);
    }

    // It should have done some I/O ...
    assertThat(buf.io()).isTrue();
    // ... but there was no trailing newline, so nothing to print (yet)
    assertThat(lines).isEmpty();

    for (char c : "\nFoo Bar Baz\nMeep".toCharArray()) {
      characters.add((int) c);
    }

    // It should have done some I/O ...
    assertThat(buf.io()).isTrue();
    // ... and give us the first two lines ("Meep" is on an unterminated line)
    assertThat(lines).containsExactly("Hello World", "Foo Bar Baz");

    // Just a CR does not trigger a "line complete"
    characters.add(13);
    assertThat(buf.io()).isTrue();
    assertThat(lines).containsExactly("Hello World", "Foo Bar Baz");

    // ... but a LF does
    characters.add(10);
    assertThat(buf.io()).isTrue();
    assertThat(lines).containsExactly("Hello World", "Foo Bar Baz", "Meep");

    // Add some more data, with an unterminated line...
    for (char c : "\nMore text\nNo EOL".toCharArray()) {
      characters.add((int) c);
    }
    assertThat(buf.io()).isTrue();
    assertThat(lines).containsExactly("Hello World", "Foo Bar Baz", "Meep", "", "More text");

    // "Final" flush() should yield the remaining data
    buf.flush();
    assertThat(lines)
        .containsExactly("Hello World", "Foo Bar Baz", "Meep", "", "More text", "No EOL");
  }
}
