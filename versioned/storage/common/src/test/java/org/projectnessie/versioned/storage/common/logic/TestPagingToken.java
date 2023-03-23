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
package org.projectnessie.versioned.storage.common.logic;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations.unsafeWrap;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.emptyPagingToken;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.pagingToken;

import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

@ExtendWith(SoftAssertionsExtension.class)
public class TestPagingToken {
  @InjectSoftAssertions SoftAssertions soft;

  static Stream<Arguments> asString() {
    return Stream.of(
        arguments(new byte[0], ""),
        arguments("foo".getBytes(UTF_8), "666f6f"),
        arguments(new byte[5], "0000000000"));
  }

  @ParameterizedTest
  @MethodSource("asString")
  public void asString(byte[] bytes, String expected) {
    PagingToken token = pagingToken(unsafeWrap(bytes));
    String str = token.asString();
    soft.assertThat(str).isEqualTo(expected);
    PagingToken tokenFromWStr = PagingToken.fromString(str);
    soft.assertThat(tokenFromWStr).isEqualTo(token);
  }

  @Test
  public void empty() {
    soft.assertThat(emptyPagingToken()).extracting(PagingToken::token).isEqualTo(ByteString.EMPTY);
  }
}
