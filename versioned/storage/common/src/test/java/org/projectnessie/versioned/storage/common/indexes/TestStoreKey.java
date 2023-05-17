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
package org.projectnessie.versioned.storage.common.indexes;

import static java.lang.Character.isSurrogate;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.INTEGER;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.deserializeKey;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.util.Util.STRING_100;
import static org.projectnessie.versioned.storage.common.util.Util.asHex;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestStoreKey {
  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @MethodSource("keyLengthGood")
  void keyLengthGood(List<String> elements) {
    key(elements).check();
  }

  static Stream<Arguments> keyLengthGood() {
    return Stream.of(
        arguments(singletonList("1")),
        arguments(singletonList(STRING_100)),
        arguments(singletonList(STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100)),
        arguments(asList(STRING_100, STRING_100)),
        arguments(asList(STRING_100, STRING_100, STRING_100)),
        arguments(asList(STRING_100, STRING_100, STRING_100, STRING_100)),
        arguments(
            asList(STRING_100, STRING_100, STRING_100, STRING_100, STRING_100.substring(0, 96))));
  }

  @ParameterizedTest
  @MethodSource("keyTooLong")
  void keyTooLong(List<String> elements) {
    assertThatThrownBy(() -> key(elements).check())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Key too long, max allowed length: " + StoreKey.MAX_LENGTH);
  }

  static Stream<Arguments> keyTooLong() {
    return Stream.of(
        arguments(
            singletonList(STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100 + "x")),
        arguments(
            singletonList(
                STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100 + STRING_100)),
        arguments(asList(STRING_100, STRING_100, STRING_100, STRING_100, STRING_100, "x")),
        arguments(asList("x", STRING_100, STRING_100, STRING_100, STRING_100, STRING_100)));
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 5, 15, 19, 20})
  void keyElementsGood(int elements) {
    key(IntStream.range(1, elements).mapToObj(i -> "foo").collect(Collectors.toList()));
  }

  @ParameterizedTest
  @MethodSource("compare")
  void compare(StoreKey a, StoreKey b, int expectedCompare) {
    soft.assertThat(a)
        .describedAs("Compare of %s to %s expect %d", a, b, expectedCompare)
        .extracting(k -> Integer.signum(k.compareTo(b)))
        .asInstanceOf(INTEGER)
        .isEqualTo(expectedCompare);
    soft.assertThat(a)
        .describedAs("Reverse compare of %s to %s expect %d", a, b, expectedCompare)
        .extracting(k -> Integer.signum(b.compareTo(k)))
        .asInstanceOf(INTEGER)
        .isEqualTo(-expectedCompare);
  }

  static Stream<Arguments> compare() {
    return Stream.of(
        arguments(key("M", "k2\u0001k3", "C"), key("M", "k2\u0001πa", "C"), -1), // UNICODE CHAR
        arguments(key("a"), key("a"), 0),
        arguments(key("a"), key("aa"), -1),
        arguments(key("a", "a"), key("a"), 1),
        arguments(key("a", "a"), key("a", "aa"), -1),
        arguments(key("a", "a"), key("a", "a", "a"), -1),
        arguments(key("a", "a", "a"), key("a", "aa", "a"), -1),
        arguments(key("a\u0001a"), key("a\u0001a"), 0),
        arguments(key("a\u0001a"), key("aa\u0001a"), -1),
        arguments(key("a\u0001a"), key("a\u0001a"), 0),
        arguments(key("a\u0001a"), key("aa\u0001aa"), -1),
        arguments(key("aπ\u0001a"), key("aπ\u0001a"), 0), // UNICODE CHAR
        arguments(key("aπ\u0001a"), key("aπa\u0001a"), -1), // UNICODE CHAR
        arguments(key("aπ\u0001a"), key("aπ\u0001a"), 0), // UNICODE CHAR
        arguments(key("aπ\u0001a"), key("aπa\u0001aa"), -1), // UNICODE CHAR
        arguments(key("aa\u0001a"), key("aπ\u0001a"), -1), // UNICODE CHAR
        arguments(key("aa\u0001a"), key("aπa\u0001a"), -1), // UNICODE CHAR
        arguments(key("aa\u0001a"), key("aπ\u0001a"), -1), // UNICODE CHAR
        arguments(key("aa\u0001a"), key("aπa\u0001aa"), -1), // UNICODE CHAR
        arguments(key("a", "a"), key("a"), 1),
        arguments(key("a", "a"), key("a", "aa"), -1),
        arguments(key("a", "a"), key("a", "a", "a"), -1),
        arguments(key("a", "a", "a"), key("a", "aa", "a"), -1),
        arguments(key("a"), key("aa"), -1),
        arguments(key("a", "πa"), key("a"), 1), // UNICODE CHAR
        arguments(key("a", "πa"), key("a", "πaa"), -1), // UNICODE CHAR
        arguments(key("a", "aπ"), key("a", "aπ", "a"), -1), // UNICODE CHAR
        arguments(key("a", "aπ", "π"), key("a", "aπ", "πa"), -1), // UNICODE CHAR
        arguments(key("a", "aπ"), key("a", "aπ", "πa"), -1), // UNICODE CHAR
        arguments(key("a", "aπ", "a"), key("a", "aπa", "a"), -1), // UNICODE CHAR
        arguments(key("a"), key("a", "b"), -1),
        arguments(key("a"), key("a", "a"), -1),
        arguments(key("a"), key("aπ", "a"), -1), // UNICODE CHAR
        arguments(key("aa"), key("aπ"), -1), // UNICODE CHAR
        arguments(key("a", "a"), key("a"), 1),
        arguments(key("a"), key("abcdef"), -1),
        arguments(key("abcdef"), key("a"), 1),
        arguments(key("abcdef"), key("0123", "123", "123"), 1),
        arguments(key("abcdef", "abc", "abc"), key("0123"), 1),
        arguments(key("0"), key("0123", "123", "123"), -1),
        arguments(key("abcdef", "abc", "abc"), key("a"), 1),
        arguments(key("key.0"), key("key.1"), -1),
        arguments(key("key.42"), key("key.42"), 0),
        arguments(key("key", "0"), key("key", "1"), -1),
        arguments(key("key", "42"), key("key", "42"), 0));
  }

  @Test
  void keyEndsWithElement() {
    soft.assertThat(key("a").endsWithElement("a")).isTrue();
    soft.assertThat(key("a", "a").endsWithElement("a")).isTrue();
    soft.assertThat(key("a").endsWithElement("A")).isFalse();
    soft.assertThat(key("a", "b").endsWithElement("a")).isFalse();
    soft.assertThat(key("a", "b").endsWithElement("b")).isTrue();
  }

  @Test
  void keyStartsWithElementsOrParts() {
    soft.assertThat(key("a").startsWithElementsOrParts(key("a"))).isTrue();
    soft.assertThat(key("b").startsWithElementsOrParts(key("a"))).isFalse();
    soft.assertThat(key("b", "a").startsWithElementsOrParts(key("a"))).isFalse();
    soft.assertThat(key("a", "b").startsWithElementsOrParts(key("a"))).isTrue();
    soft.assertThat(key("a", "b").startsWithElementsOrParts(key("a", "b"))).isTrue();
    soft.assertThat(key("a", "b", "c").startsWithElementsOrParts(key("a", "b"))).isTrue();
    soft.assertThat(key("a", "b\u0001b", "c").startsWithElementsOrParts(key("a", "b"))).isTrue();
    soft.assertThat(key("a", "b\u0001b", "c").startsWithElementsOrParts(key("a", "b\u0001b")))
        .isTrue();
    soft.assertThat(
            key("a", "b\u0001b\u0001b", "c").startsWithElementsOrParts(key("a", "b\u0001b")))
        .isTrue();
    soft.assertThat(key("a", "bb\u0001b", "c").startsWithElementsOrParts(key("a", "b"))).isFalse();
    soft.assertThat(key("a", "bb", "c").startsWithElementsOrParts(key("a", "b"))).isFalse();
    soft.assertThat(key("a", "a\u0001b", "c").startsWithElementsOrParts(key("a", "b"))).isFalse();
    soft.assertThat(key("a", "bπ\u0001b", "c").startsWithElementsOrParts(key("a", "b")))
        .isFalse(); // UNICODE CHAR
    soft.assertThat(key("a", "bπ", "c").startsWithElementsOrParts(key("a", "b")))
        .isFalse(); // UNICODE CHAR
    soft.assertThat(key("a", "bπ\u0001b", "c").startsWithElementsOrParts(key("a", "bπ")))
        .isTrue(); // UNICODE CHAR
    soft.assertThat(key("a", "bπ", "c").startsWithElementsOrParts(key("a", "bπ")))
        .isTrue(); // UNICODE CHAR
    soft.assertThat(key("a", "πb\u0001b", "c").startsWithElementsOrParts(key("a", "π")))
        .isFalse(); // UNICODE CHAR
    soft.assertThat(key("a", "πb", "c").startsWithElementsOrParts(key("a", "π")))
        .isFalse(); // UNICODE CHAR
    soft.assertThat(key("a", "πb\u0001b", "c").startsWithElementsOrParts(key("a", "πb")))
        .isTrue(); // UNICODE CHAR
    soft.assertThat(key("a", "πb", "c").startsWithElementsOrParts(key("a", "πb")))
        .isTrue(); // UNICODE CHAR
  }

  @Test
  void invalidKeys() {
    soft.assertThatIllegalArgumentException().isThrownBy(() -> key(""));
    soft.assertThatIllegalArgumentException().isThrownBy(() -> key("abc", "", "foo"));
    soft.assertThatIllegalArgumentException().isThrownBy(() -> key("abc", "foo", ""));
    soft.assertThatIllegalArgumentException().isThrownBy(() -> key(Collections.singletonList("")));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> key(Arrays.asList("abc", "", "foo")));
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> key(Arrays.asList("abc", "foo", "")));
  }

  static Stream<Arguments> keySerializationRoundTrip() {
    return Stream.of(
        arguments(false, key("A"), 3, "410000"),
        arguments(true, key("A"), 3, "410000"),
        arguments(false, key("abc"), 5, "6162630000"),
        arguments(true, key("abc"), 5, "6162630000"),
        arguments(false, key("abc", "def", "ghi"), 13, "61626300" + "64656600" + "67686900" + "00"),
        arguments(true, key("abc", "def", "ghi"), 13, "61626300" + "64656600" + "67686900" + "00"),
        arguments(false, key(STRING_100 + STRING_100), 202, null),
        arguments(true, key(STRING_100 + STRING_100), 202, null),
        arguments(
            false, key(STRING_100, STRING_100, STRING_100, STRING_100, STRING_100), 506, null),
        arguments(
            true, key(STRING_100, STRING_100, STRING_100, STRING_100, STRING_100), 506, null));
  }

  @ParameterizedTest
  @MethodSource("keySerializationRoundTrip")
  public void keySerializationRoundTrip(
      boolean directBuffer, StoreKey key, int expectedSerializedSize, String checkedHex) {
    IntFunction<ByteBuffer> alloc =
        len -> directBuffer ? ByteBuffer.allocateDirect(len) : ByteBuffer.allocate(len);

    ByteBuffer serialized = key.serialize(alloc.apply(506));
    soft.assertThat(serialized.remaining()).isEqualTo(expectedSerializedSize);
    soft.assertThat(StoreIndexImpl.serializedSize(key)).isEqualTo(expectedSerializedSize);
    if (checkedHex != null) {
      soft.assertThat(asHex(serialized)).isEqualTo(checkedHex);
    }
    StoreKey deserialized = deserializeKey(serialized.duplicate());
    soft.assertThat(deserialized).isEqualTo(key);

    ByteBuffer big = alloc.apply(8192);
    big.position(1234);
    big.put(serialized.duplicate());
    big.position(8000);
    ByteBuffer ser = big.duplicate().flip();
    ser.position(1234);
    deserialized = deserializeKey(ser.duplicate());
    soft.assertThat(deserialized).isEqualTo(key);
  }

  @Test
  public void utf8surrogates() {
    char[] arr = new char[] {0xd800, 0xdc00, 0xd8ff, 0xdcff};

    utf8verify(arr);
  }

  @Test
  public void utf8invalidSurrogates() {
    char[] arr = new char[] {0xd800, 'a'};

    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> utf8verify(arr))
        .withMessageStartingWith("Unmappable surrogate character");
  }

  @ParameterizedTest
  @ValueSource(strings = {"süße sahne", "là-bas"})
  public void utf8string(String s) {
    char[] arr = s.toCharArray();

    utf8verify(arr);
  }

  @Test
  public void utf8allChars() {
    int charFirst = 1;
    int charLast = 65535;
    int charCount = charLast - charFirst + 1;

    char[] arr = new char[charCount];
    int idx = 0;
    for (int i = charFirst; i <= charLast; i++) {
      char c = (char) i;
      // Ignore surrogates
      if (!isSurrogate(c)) {
        arr[idx++] = c;
      }
    }
    arr = Arrays.copyOf(arr, idx);

    utf8verify(arr);
  }

  private void utf8verify(char[] arr) {
    ByteBuffer serToBufferFromString = ByteBuffer.allocate(arr.length * 3 + 2);
    StoreKey.putString(serToBufferFromString, new String(arr));
    serToBufferFromString.put((byte) 0);
    serToBufferFromString.put((byte) 0);
    serToBufferFromString.flip();

    ByteBuffer bufferFromString = ByteBuffer.allocate(arr.length * 3 + 2);
    bufferFromString.put(new String(arr).getBytes(UTF_8));
    bufferFromString.put((byte) 0);
    bufferFromString.put((byte) 0);
    bufferFromString.flip();

    int mismatch = bufferFromString.mismatch(serToBufferFromString);
    if (mismatch != -1) {
      soft.assertThat(mismatch).describedAs("Mismatch at %d", mismatch).isEqualTo(-1);
    }

    StoreKey deser = deserializeKey(serToBufferFromString.duplicate());
    ByteBuffer b2 = ByteBuffer.allocate(serToBufferFromString.capacity());
    deser.serialize(b2);

    mismatch = serToBufferFromString.mismatch(b2);
    if (mismatch != -1) {
      soft.assertThat(mismatch).describedAs("Mismatch at %d", mismatch).isEqualTo(-1);
    }

    soft.assertThat(deser.rawString()).isEqualTo(new String(arr));
  }

  static Stream<Arguments> shortestMismatch() {
    return Stream.of(
        arguments("aaaa", "bbbb", "b"),
        arguments("aaaa", "b", "b"),
        arguments("a", "bbbb", "b"),
        arguments("a", "b", "b"),
        arguments("", "b", "b"),
        arguments("aaaa", "aaab", "aaab"),
        arguments("aaaa", "aaaa", "aaaa"),
        arguments("aaaa", "aaaaaa", "aaaaa"),
        arguments("aaaa", "aaaaa", "aaaaa"),
        arguments("aaaa", "aaaab", "aaaab"),
        arguments("aaaa", "aaaab", "aaaab"),
        arguments("", "", ""),
        arguments("abandonments.abandonwares", "abandonments.abbreviated", "abandonments.abb"));
  }
}
