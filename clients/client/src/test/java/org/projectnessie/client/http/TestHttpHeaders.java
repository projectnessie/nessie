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
package org.projectnessie.client.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.URLConnection;
import java.util.Arrays;
import java.util.LinkedHashSet;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestHttpHeaders {
  @Test
  public void multipleValues() {
    HttpHeaders headers = new HttpHeaders();
    headers.put("Abc", "def");
    headers.put("abc", "ghi");
    headers.put("aBc", "jkl");
    assertThat(headers.asMap())
        .containsExactly(entry("Abc", new LinkedHashSet<>(Arrays.asList("def", "ghi", "jkl"))));
    assertThat(headers.getValues("abc")).containsExactly("def", "ghi", "jkl");
  }

  @Test
  public void notPresent() {
    HttpHeaders headers = new HttpHeaders();
    assertThat(headers.getValues("notThere")).isEmpty();
  }

  @Test
  public void applyToUrlConnection() {

    HttpHeaders headers = new HttpHeaders();
    headers.put("Abc", "def");
    headers.put("abc", "ghi");
    headers.put("aBc", "jkl");
    headers.put("Foo-Bar", "Baz");

    URLConnection urlConnectionMock = mock(URLConnection.class);
    ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);

    headers.applyTo(urlConnectionMock);
    verify(urlConnectionMock, times(4))
        .addRequestProperty(nameCaptor.capture(), valueCaptor.capture());

    assertThat(nameCaptor.getAllValues()).containsExactly("Abc", "Abc", "Abc", "Foo-Bar");
    assertThat(valueCaptor.getAllValues()).containsExactly("def", "ghi", "jkl", "Baz");
  }
}
