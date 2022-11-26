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
package org.projectnessie.tools.contentgenerator;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.tools.contentgenerator.keygen.KeyGenerator;

@ExtendWith(SoftAssertionsExtension.class)
public class TestKeyGenerator {

  private static final int MAX_RETRY = 10;
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void constantStrings() {
    KeyGenerator gen = KeyGenerator.newKeyGenerator("hello world");
    List<String> values = Stream.generate(gen::generate).limit(20).collect(Collectors.toList());
    soft.assertThat(values).hasSize(20).allMatch("hello world"::equals);
  }

  @Test
  public void intFunc() {
    KeyGenerator gen = KeyGenerator.newKeyGenerator("hello world ${int,100} foo");
    List<String> values = Stream.generate(gen::generate).limit(20).collect(Collectors.toList());
    soft.assertThat(values)
        .hasSize(20)
        .allSatisfy(s -> assertThat(s).matches("hello world [0-9]+ foo"));
  }

  @Test
  public void seqFunc() {
    KeyGenerator gen = KeyGenerator.newKeyGenerator("hello world ${seq,20} foo");
    List<String> values = Stream.generate(gen::generate).limit(20).collect(Collectors.toList());
    soft.assertThat(values)
        .hasSize(20)
        .allSatisfy(s -> assertThat(s).matches("hello world [0-9]+ foo"))
        .containsExactlyElementsOf(
            IntStream.range(20, 40)
                .mapToObj(i -> "hello world " + i + " foo")
                .collect(Collectors.toList()));
  }

  @Test
  public void everySeqFunc() {
    KeyGenerator gen = KeyGenerator.newKeyGenerator("hello world ${every,5,seq,20} foo");
    List<String> values = Stream.generate(gen::generate).limit(20).collect(Collectors.toList());
    soft.assertThat(values)
        .hasSize(20)
        .allSatisfy(s -> assertThat(s).matches("hello world [0-9]+ foo"))
        .containsExactlyElementsOf(
            IntStream.range(20, 24)
                .mapToObj(i -> "hello world " + i + " foo")
                .flatMap(s -> Stream.of(s, s, s, s, s))
                .collect(Collectors.toList()));
  }

  @Test
  public void everyIntFunc() {
    for (int retry = 0; ; retry++) {
      KeyGenerator gen = KeyGenerator.newKeyGenerator("hello world ${every,5,int,100} foo");
      List<String> values = Stream.generate(gen::generate).limit(20).collect(Collectors.toList());
      soft.assertThat(values)
          .hasSize(20)
          .allSatisfy(s -> assertThat(s).matches("hello world [0-9]+ foo"));

      soft.assertThat(values.subList(0, 5)).allMatch(values.get(0)::equals);
      soft.assertThat(values.subList(5, 20)).noneMatch(values.get(0)::equals);

      soft.assertThat(values.subList(5, 10)).allMatch(values.get(5)::equals);
      soft.assertThat(values.subList(0, 5)).noneMatch(values.get(5)::equals);
      soft.assertThat(values.subList(10, 20)).noneMatch(values.get(5)::equals);

      soft.assertThat(values.subList(10, 15)).allMatch(values.get(10)::equals);
      soft.assertThat(values.subList(0, 10)).noneMatch(values.get(10)::equals);
      soft.assertThat(values.subList(15, 20)).noneMatch(values.get(10)::equals);

      soft.assertThat(values.subList(15, 20)).allMatch(values.get(15)::equals);
      soft.assertThat(values.subList(0, 15)).noneMatch(values.get(15)::equals);

      // The generator pattern uses random values, it's unlikely, but possible to have the same
      // value in the generated values.
      try {
        soft.assertAll();
        break;
      } catch (AssertionError e) {
        if (retry == MAX_RETRY) {
          throw e;
        }
      }
    }
  }

  @Test
  public void stringFunc() {
    for (int retry = 0; ; retry++) {
      KeyGenerator gen = KeyGenerator.newKeyGenerator("hello world ${string,5} foo");
      List<String> values = Stream.generate(gen::generate).limit(20).collect(Collectors.toList());
      soft.assertThat(values)
          .hasSize(20)
          .allSatisfy(s -> assertThat(s).matches("hello world [A-Za-z0-9 _.-]{5} foo"));

      soft.assertThat(new HashSet<>(values)).hasSizeGreaterThan(15);

      // The generator pattern uses random values, it's unlikely, but possible to have the same
      // value in the generated values.
      try {
        soft.assertAll();
        break;
      } catch (AssertionError e) {
        if (retry == MAX_RETRY) {
          throw e;
        }
      }
    }
  }

  @Test
  public void probUuidFunc() {
    for (int retry = 0; ; retry++) {
      KeyGenerator gen = KeyGenerator.newKeyGenerator("hello world ${prob,.5,uuid} foo");
      List<String> values = Stream.generate(gen::generate).limit(20).collect(Collectors.toList());
      soft.assertThat(values)
          .hasSize(20)
          .allSatisfy(s -> assertThat(s).matches("hello world [0-9a-f-]+ foo"));

      soft.assertThat(new HashSet<>(values)).hasSizeGreaterThan(1).hasSizeLessThan(20);

      // The generator pattern uses random values, it's unlikely, but possible to have the same
      // value in the generated values.
      try {
        soft.assertAll();
        break;
      } catch (AssertionError e) {
        if (retry == MAX_RETRY) {
          throw e;
        }
      }
    }
  }

  @Test
  public void longKeysScenario() {
    for (int retry = 0; ; retry++) {
      KeyGenerator gen =
          KeyGenerator.newKeyGenerator(
              "stuff-folders./stuff-${every,10,uuid}/foolish-key/${every,5,uuid}/${uuid}_0");
      List<String> values = Stream.generate(gen::generate).limit(20).collect(Collectors.toList());
      soft.assertThat(values).hasSize(20);
      soft.assertThat(new HashSet<>(values)).hasSize(20);

      // The generator pattern uses random values, it's unlikely, but possible to have the same
      // value in the generated values.
      try {
        soft.assertAll();
        break;
      } catch (AssertionError e) {
        if (retry == MAX_RETRY) {
          throw e;
        }
      }
    }
  }
}
