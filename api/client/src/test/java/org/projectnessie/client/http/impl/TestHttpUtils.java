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
package org.projectnessie.client.http.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestHttpUtils {

  public static Stream<Arguments> testParseQueryString() {
    return Stream.of(
        Arguments.of(
            "key1=value1" + "&" + "key2=value2",
            ImmutableMap.of("key1", "value1", "key2", "value2")),
        Arguments.of(
            "key1=%E4%BD%A0%E5%A5%BD"
                + "&"
                + "key2=%CE%9A%CE%B1%CE%BB%CE%B9%CE%BC%CE%AD%CF%81%CE%B1",
            ImmutableMap.of("key1", "你好", "key2", "Καλιμέρα")));
  }

  @ParameterizedTest
  @MethodSource
  void testParseQueryString(String queryString, Map<String, String> expected) {
    Map<String, String> result = HttpUtils.parseQueryString(queryString);
    assertThat(result).isEqualTo(expected);
  }
}
